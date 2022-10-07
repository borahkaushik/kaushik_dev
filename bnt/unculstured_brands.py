import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row, DataFrame
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql.functions import *
import boto3
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','domain','s3_bucket','brand_dict_database','brand_dict_table_name', 'bnt_file_key'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# -----------------------------------------------------------data base function---------------------------------------------------------------
def get_data_from_athena(database, table_name, query):
    print("Fetching data from athena = {}.{}".format(database, table_name))
    try : 
        if query is None : 
            glue_df = glueContext.create_dynamic_frame_from_catalog(
                database = database,
                table_name = table_name,
                transformation_ctx = "glue_df" )
            
            spark_df = glue_df.toDF()
            if len(spark_df.head(1)) == 0 :
                return None
            print("Data fetched sucessfully")
            return spark_df
        else :
            glue_df = glueContext.create_dynamic_frame_from_catalog(
                database = database,
                table_name = table_name,
                transformation_ctx = "glue_df",
                push_down_predicate = query) 
            
            spark_df = glue_df.toDF()
            if len(spark_df.head(1)) == 0 :
                return None
            print("Data fetched sucessfully")
            return spark_df
    except Exception as e:
        print(e)
        raise Exception(e)

def get_data_from_s3(s3_bucket, file_type):
    s3_path = 's3://{bucket}/{s3_object_key}'.format(bucket = s3_bucket, s3_object_key=file_type)
    print("Fetching data from s3 path = {}".format(s3_path))
    glue_df = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        format="parquet",
        connection_options={
            "paths": [s3_path]
        },
        format_options={
            "withHeader": True,
        })
        
    spark_df = glue_df.toDF()
    spark_df = spark_df.drop("__index_level_0__")
    # print(s3_path ," = ",spark_df.count())
    return spark_df
    
def save_dataframe_to_s3(df, s3_bucket, file_key):
    if df == None or len(df.head(1)) == 0 :
        print("Empty dataframe.\n Skipping saving dataframe")
        return
    path = 's3a://{}/{}/'.format(s3_bucket, file_key)
    print("Saving df to loc = {}".format(path))
    df.write.mode("overwrite").option("header",True).parquet(path)
    print("dfs saved successfully")

def get_data_from_aurora_db():
    # need to push this to aws secrets manager
    try :
        print("fetching data fropm aurora db")
        database = 'BungeeMatchPool'
        db_url = "jdbc:postgresql://sandbox-db-cluster.cluster-cf6pgrepnkpy.us-east-1.rds.amazonaws.com:5432/BungeeMatchPool"
        table_name = "brand_standardization"
        db_username = "postgres"
        db_password = "Bungee_sand_rds_2021!"
        print("fetching data fropm aurora db: {}.{}".format(database,table_name))
        df = glueContext.read.format("jdbc").option("url", db_url).option("dbtable", table_name).option("user", db_username).option("password", db_password).load()
        if len(df.head(1)) == 0 :
            return None
        return df
    except Exception as e:
        raise Exception(e)

def delete_old_s3_object(s3_client, s3_bucket, key):
    s3_client.delete_object(Bucket= s3_bucket, Key= key)

def replace_old_s3_object(df, s3_client, s3_bucket, key):
    delete_old_s3_object(s3_client, s3_bucket, key)
    save_dataframe_to_s3(df, s3_bucket, key)

# --------------------------------------------------------------utility function----------------------------------------------------------------
def combine_dfs(df_list):
    print("Combning dfs")
    combined_df = reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), df_list)
    print("Combined df size = {}".format(combined_df.count()))
    return combined_df

def get_max_id(df):
    if df == None or len(df.head(1)) == 0:
        return 0

    result = df.select([max('id')])
    max_id = result.collect()[0]['max(id)']
    return max_id

def insert_id(df, start_idx):
    print("Inserting ids")
    updated_schema = StructType(df.schema.fields[:] + [StructField("id", LongType(), False)])
    zipped_rdd = df.rdd.zipWithIndex().map(lambda x: (x[0], x[1] + start_idx))
    id_df = (zipped_rdd.map(lambda ri: Row(*list(ri[0]) + [ri[1]])).toDF(updated_schema))
    # print("New id_uuid = {}".format(new_id_uuid_df.count()))
    return id_df

# -----------------------------------------------------------------preprocessing---------------------------------------------------------

def generate_existing_graph(aurora_db_data):
    if aurora_db_data == None:
        return None
    print("Generating old graph")
    old_cluster = aurora_db_data.select(aurora_db_data.crawled_name.alias('brand'), aurora_db_data.cluster_id.cast(IntegerType()).alias('id'))
    old_cluster = old_cluster.filter(old_cluster.source == 'SC')
    print("Generated old graph")
    return old_cluster

def get_brands_in_db(aurora_db_df):
    if aurora_db_df == None:
        return [] 
    
    db_brands = aurora_db_df.rdd.map(lambda df: df.crawled_name).collect()
    return db_brands

# ----------------------------------post processinh-----------------------------------
def convert_to_database_schema(cluster_df: DataFrame, brand_dict: DataFrame, db_brand_list:DataFrame, cluster_type: str)-> DataFrame:

    if ( cluster_df == None or len(cluster_df.head(1)) == 0 ) or ( brand_dict == None or len(brand_dict.head(1)) == 0 ):
        print("Empty dataframe")
        return None, None
    print('Converting df to database schema')
    print('Input df:', cluster_df.printSchema())
    cluster_brand_info = cluster_df.join(brand_dict, cluster_df.brand == brand_dict.crawled_name, 'inner').drop('brand')
    cluster_brand_info = cluster_brand_info.withColumn('source', lit(cluster_type))
    cluster_brand_info = cluster_brand_info.withColumnRenamed('id', 'cluster_id')
    cluster_brand_info = cluster_brand_info.withColumn('status', lit('IN_PROGRESS'))
    cluster_brand_info = cluster_brand_info.withColumn('updated_date', current_timestamp().cast("string"))
    cluster_brand_info = cluster_brand_info.select('cluster_id', 'crawled_name', 'source', 'status', 'updated_date', 'product_url_1', 'image_url_1', 'product_url_2')
    updation_brands = cluster_brand_info.filter(cluster_brand_info.crawled_name.isin(db_brand_list))
    insertion_brands = cluster_brand_info.filter(~cluster_brand_info.crawled_name.isin(db_brand_list))
    updation_brnads = updation_brands.withColumn( 'query_type', lit('U'))
    insertion_brnads = insertion_brands.withColumn('query_type', lit('I'))
    print('updation_brnads df:', updation_brnads.printSchema())
    print('insertion_brnads df:', insertion_brnads.printSchema())
    return insertion_brnads, updation_brnads



def get_unclustered_brands(id_string_cluster, id_brand_cluster, brand_dict):
    string_cluster_brands = id_string_cluster.select('brands')
    brand_cluster_brands = id_brand_cluster.select('brand')
    clutered_brands = combine_dfs([string_cluster_brands, brand_cluster_brands])
    crawled_brands = brand_dict.select('crawled_brands')
    unclustered_brands = crawled_brands.subtract(clutered_brands)
    unclustered_brands = unclustered_brands.withColumn('cluster_id', lit(-1))
    return unclustered_brands


# -------------------------------------------------------------------------------
def get_brand_cluster(s3_bucket, bnt_file_key):
    new_brand_cluster = get_data_from_s3(s3_bucket, bnt_file_key+'/new_brand_cluster')
    update_brand_cluster = get_data_from_s3(s3_bucket, bnt_file_key+'/update_brand_cluster')
    brand_cluster = combine_dfs([new_brand_cluster, update_brand_cluster])
    return brand_cluster

def main(args):

    s3_bucket = args['s3_bucket']
    brand_dict_database = args['brand_dict_database']
    brand_dict_table_name = args['brand_dict_table_name']
    domain = args['domain']
    bnt_file_key = args['bnt_file_key']
    s3_client = boto3.client('s3')


    brand_dict = get_data_from_athena(brand_dict_database, brand_dict_table_name, None)
    print("brand_dict : ", brand_dict.printSchema())

    #  get brand_cluster from s3
    id_brand_cluster = get_brand_cluster(s3_bucket, bnt_file_key)

    # get new string cluster from s3
    string_cluster = get_data_from_s3(s3_bucket, bnt_file_key+'/raw_string_cluster')
    all_string_cluste_set= string_cluster.groupBy('id').agg(array_sort(collect_set('brand')).alias('brand')).drop('id')

    # get old string cluster from aurora db
    print("getting data from aurora db")
    aurora_db_data = get_data_from_aurora_db()
    old_string_cluster = generate_existing_graph(aurora_db_data)
    db_brand_list = get_brands_in_db(aurora_db_data)

    print("Getting new string cluster")
    #  getting new string clusters string clusters generated 
    old_string_cluster_set = old_string_cluster.old_brand_cluster.groupBy('old_id').agg(array_sort(collect_set('old_brand')).alias('old_brand_set'))
    new_string_cluster = all_string_cluste_set.subtract(old_string_cluster_set).drop('cluster_id')

    # assigne cluster_ids to the new string clusters
    print("Inserting Ids")
    idx = get_max_id(id_brand_cluster)
    id_string_cluster = insert_id(new_string_cluster, idx+1)
    id_string_cluster = id_string_cluster.withColumn('brand', explode('brand')).withColumnRenamed('id', 'cluster_id')

    #  fetching unclustered brands
    print("Getting unclusterwed brands")
    unclustered_brands = get_unclustered_brands(id_string_cluster, id_brand_cluster, brand_dict)

    # saving string cluster to s3
    print("Saving string cluster") 
    inserertion_data, updation_data = convert_to_database_schema(string_cluster, brand_dict, db_brand_list, 'SC')
    replace_old_s3_object( inserertion_data, s3_client, s3_bucket, bnt_file_key+'/new_string_cluster/insert')
    replace_old_s3_object( updation_data, s3_client, s3_bucket, bnt_file_key+'/new_string_cluster/update')
    
    # saving unclustered brands to s3
    print("Saving unclustered brands") 
    inserertion_data, updation_data = convert_to_database_schema(unclustered_brands, brand_dict, db_brand_list, 'UC')
    replace_old_s3_object( inserertion_data, s3_client, s3_bucket, bnt_file_key+'/unclustered_brands/insert', brand_dict)
    replace_old_s3_object( updation_data, s3_client, s3_bucket, bnt_file_key+'/unclustered_brands/update', brand_dict)
    

    
main(args)

