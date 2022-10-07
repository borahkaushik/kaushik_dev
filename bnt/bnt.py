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
from graphframes import *
from difflib import get_close_matches
import boto3
from datetime import datetime as dt

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','domain','s3_bucket','brand_dict_database','brand_dict_table_name','product_matches_database','product_matches_table', 'bnt_file_key'])

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
        table_name = "ml_brand_standardization"
        db_username = "postgres"
        db_password = "Bungee_sand_rds_2021!"
        print("fetching data fropm aurora db: {}.{}".format(database,table_name))
        df = glueContext.read.format("jdbc").option("url", db_url).option("dbtable", table_name).option("user", db_username).option("password", db_password).load()
        if len(df.head(1)) == 0 :
            return None
        return df
    except Exception as e:
        raise Exception(e)

def delete_old_s3_object(s3_resource, s3_bucket, key):
    print("Deleting s3 object: ", key)
    bucket = s3_resource.Bucket(s3_bucket)
    bucket.objects.filter(Prefix=key).delete()

def replace_old_s3_object(df, s3_resource, s3_bucket, key):
    delete_old_s3_object(s3_resource, s3_bucket, key)
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

# ----------------------------------------------preprocessing-------------------------------------------------------------------
def product_matches_preprocessing(product_matches):
    product_matches = product_matches.na.drop(subset=["brand_a", "brand_b"])
    product_matches = filter_brand_names(product_matches, 'brand_a')
    product_matches = filter_brand_names(product_matches, 'brand_b')
    return product_matches

def brand_dict_preprocessing(brand_dict):
    brand_dict = filter_brand_names(brand_dict, 'crawled_name')
    brand_dict = brand_dict.dropDuplicates(['crawled_name'])
    return brand_dict
    
def filter_brand_names(df, column_name):
    df = df.withColumn("processed_column", translate( col(column_name), "\"'", "")).drop(column_name).withColumnRenamed('processed_column',column_name)
    return df

def generate_brand_matches_from_product_matches(product_matches):
    print("Fetching brand matches from product matches")
    directed_a_b_brand_matches = product_matches.select(product_matches.brand_a, product_matches.brand_b)
    directed_b_a_brand_matches = product_matches.select(product_matches.brand_a.alias('brand_b'), product_matches.brand_b.alias('brand_a'))
    undirected_brand_matches = combine_dfs([directed_a_b_brand_matches, directed_b_a_brand_matches]).dropDuplicates(['brand_a','brand_b'])
    undirected_brand_matches = undirected_brand_matches.groupBy(['brand_a','brand_b']).count()
    print("Fetched brand matches")
    return undirected_brand_matches

def generate_existing_graph(aurora_db_data):
    if aurora_db_data == None:
        return None
    print("Generating old graph")
    old_cluster = aurora_db_data.select(aurora_db_data.crawled_name.alias('brand'), aurora_db_data.cluster_id.cast(IntegerType()).alias('id'), aurora_db_data.source)
    old_cluster = old_cluster.filter(old_cluster.source == 'BC')
    print("Generated old graph")
    return old_cluster


def get_brands_in_db(aurora_db_df):
    print("Fetching db brands")
    if aurora_db_df == None:
        print("No brands found in DB")
        return None 
    db_brands = aurora_db_df.select(aurora_db_df.crawled_name.alias('brand'))
    print(db_brands)
    return db_brands

# ----------------------------------------------postprocessing-------------------------------------------------------------------

def get_unclustered_brands(brand_dict, brand_cluster):
    crawled_brands = brand_dict.select(brand_dict.crawled_name)
    clustered_brands = brand_cluster.select(brand_cluster.brand)
    unclustered_brands = crawled_brands.subtract(clustered_brands.select('brand'))
    return unclustered_brands

def convert_to_database_schema(cluster_df, brand_dict, db_brand_df, cluster_type)-> DataFrame:

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
    
    if db_brand_df is None:
        return cluster_brand_info, None
    
    print("db_brand_df : ",db_brand_df.count())
    updation_brands = cluster_brand_info.join(db_brand_df, cluster_brand_info.crawled_name == db_brand_df.brand, 'inner')
    print("updation_brands : ",updation_brands.count())
    insertion_brands = cluster_brand_info.join(db_brand_df, cluster_brand_info.crawled_name == db_brand_df.brand, 'leftanti')
    print("insertion_brands : ",insertion_brands.count())
    
    return insertion_brands, updation_brands

# -----------------------------------------brand graph attribute generation---------------------------------------------------------------

def generate_brand_graph_edges(undirected_brand_matches):
    print("Fetching brand graph edges")
    edge_df = undirected_brand_matches.withColumnRenamed('brand_a','src').withColumnRenamed('brand_b','dst')
    return edge_df

def generate_brand_graph_vertices(brand_dict):
    print("Fetching brand graph vertices")
    vertex_df = brand_dict.select(brand_dict.crawled_name.alias('id'))
    return vertex_df

# -----------------------------------------Graph Generation---------------------------------------------------------------

class Graph:
    def __init__(self, vertices, edges):
        self.vertices = vertices
        self.edges = edges
        self.construct_graph()
    
    def construct_graph(self):
        print("Constructing graph")
        self.graph = GraphFrame(self.vertices, self.edges)
        print("Graph created")
        return self.graph
        
    def get_connected_componenets(self):
        print("Generating connected components from graph")
        sc.setCheckpointDir("s3://aws-glue-assets-209656230012-us-east-1/scripts/graph_checkpoints/")
        self.connected_components = self.graph.connectedComponents()
        print("Connected components schema", self.connected_components.printSchema())
        print("Connected components generated")
        return self.connected_components
        #  connected_components["id","componenets"]

    def generate_filtered_clusters(self):
        self.generate_clusters()
        print("Filtering clusters")
        self.filtered_cluster = self.connected_component_cluster.filter(size(col("id_set"))>1)
        print("Filtered clusters generated")
        print("Filtered clusters schema", self.filtered_cluster.printSchema())
        return self.filtered_cluster

    def generate_clusters(self):
        self.get_connected_componenets()
        print("Generating clusters")
        self.connected_component_cluster = self.connected_components.groupBy('component').agg(collect_set('id').alias("id_set")).drop('component')
        #  one sinle brand cannot form a clusters so removing single size array
        print("clusters Generated")
        return self.connected_component_cluster

# -----------------------------------------Brnad Cluster Formation---------------------------------------------------------------

class Brand_normalization:
    def __init__(self, new_brand_cluster, old_brand_cluster ):
        self.new_cluster_start_index = 1
        self.old_brand_cluster = self.get_old_brand_sets(old_brand_cluster)
        self.new_brand_cluster = self.get_new_brand_sets(new_brand_cluster) 

    def generate_brand_clusters(self):
        print("Generating brand clusters")
        if self.old_brand_cluster != None:
            print("old brand cluster schema", self.old_brand_cluster.printSchema())
            print("new brand cluster schema", self.new_brand_cluster.printSchema())
            self.new_brand_cluster, self.updated_brand_cluster = self.get_updated_brand_sets()
            return self.new_brand_cluster, self.updated_brand_cluster
        else :
            print("new brand cluster schema", self.new_brand_cluster.printSchema())
            self.new_brand_cluster = self.new_brand_cluster.withColumnRenamed('new_id','id').withColumnRenamed('new_brand','brand')
            self.updated_brand_cluster = None
            return self.new_brand_cluster, None

    def get_old_brand_sets(self, old_brand_cluster):
        if old_brand_cluster == None:
            return None
        self.new_cluster_start_index = get_max_id(old_brand_cluster) + 1
        return old_brand_cluster.select(old_brand_cluster.id.alias('old_id'), old_brand_cluster.brand.alias('old_brand'))

    def get_new_brand_sets(self, new_brand_cluster):
        new_brand_cluster =  insert_id(new_brand_cluster, self.new_cluster_start_index)
        return new_brand_cluster.select(new_brand_cluster.id.alias('new_id'), explode(new_brand_cluster.id_set).alias('new_brand'))

    def get_vertex(self, set_id_matches):
        print("Generating merged graph vertex")
        set_id_a = set_id_matches.select(col("new_id").alias("id"))
        set_id_b = set_id_matches.select(col("old_id").alias("id"))
        vertex = combine_dfs([set_id_a, set_id_b]).dropDuplicates(['id'])
        print("merged graph vertex generated") 
        return vertex
    
    def generate_graph_edges(self, set_id_matches):
        print("Generating merged graph edges")
        directed_edge_a_b = set_id_matches.select('new_id','old_id')
        directed_edge_b_a = set_id_matches.select(col('new_id').alias('old_id') ,col('old_id').alias('new_id'))
        undirected_edges = combine_dfs([directed_edge_a_b, directed_edge_b_a])
        undirected_edges = undirected_edges.dropDuplicates(["new_id","old_id"]).withColumnRenamed('new_id', 'src').withColumnRenamed('old_id', 'dst')
        print("merged graph edges generated") 
        return undirected_edges

    def get_brand_cluster(self, connected_clusters):
        print("Generating old and new merged brand clusters")
        old_cluster_brands = connected_clusters.join(self.old_brand_cluster, connected_clusters.id == self.old_brand_cluster.old_id, 'inner').drop('old_id')
        old_cluster_brands = old_cluster_brands.withColumnRenamed('old_brand','brand')
        new_cluster_brands = connected_clusters.join(self.new_brand_cluster, connected_clusters.id == self.new_brand_cluster.new_id, 'inner').drop('new_id')
        new_cluster_brands = new_cluster_brands.withColumnRenamed('new_brand','brand')
        connected_brand_clusters = combine_dfs([old_cluster_brands, new_cluster_brands])
        print("Generated merged brand clusters")
        return connected_brand_clusters
        #  connected_brand_clusters("id", "")

    def get_updated_brand_sets(self):
        set_id_matches = self.new_brand_cluster.join(self.old_brand_cluster, self.new_brand_cluster.new_brand == self.old_brand_cluster.old_brand, 'inner')
        print("set_id_matches: ", set_id_matches.printSchema())
        vertices = self.get_vertex(set_id_matches)
        edges = self.generate_graph_edges(set_id_matches)
        print("Merging old and new graph")
        graph = Graph(vertices, edges)
        connected_clusters = graph.get_connected_componenets()

        connected_brand_clusters = self.get_brand_cluster(connected_clusters)
        print("connected_brand_clusters: ", connected_brand_clusters.printSchema())
        set_id_list_brand_cluster = connected_brand_clusters.groupBy('component').agg(array_sort(collect_set('id')).alias('id_sets'), collect_set('brand').alias('brand_set')).drop('component')
        self.updated_id_brand_cluster = set_id_list_brand_cluster.withColumn('id',array_min(set_id_list_brand_cluster.id_sets)).drop('id_sets')

        # updated_id_brand_cluster['id','brand_set']
        old_brand_set = self.old_brand_cluster.groupBy('old_id').agg(collect_set('old_brand').alias('old_brand_set'))
        set_id_old_and_updated_brand_cluster = self.updated_id_brand_cluster.join(old_brand_set, self.updated_id_brand_cluster.id == old_brand_set.old_id, 'inner' ).drop('old_id')
        updated_brand_cluster = set_id_old_and_updated_brand_cluster.select("id",array_except("brand_set","old_brand_set").alias('brand_set')).filter(size(col("brand_set"))>0)
        updated_brand_cluster = updated_brand_cluster.withColumn('brand', explode('brand_set')).drop('brand_set')

        # updation_brand_cluster['id','brand_set']
        old_and_updated_set_id = connected_brand_clusters.select(col('id').alias('old_set_id')).dropDuplicates()
        new_set_id_brand_cluster = self.new_brand_cluster.groupBy(col("new_id").alias('id')).agg(collect_set('new_brand').alias('brand_set'))
        new_brand_cluster = new_set_id_brand_cluster.join(old_and_updated_set_id, new_set_id_brand_cluster.id == old_and_updated_set_id.old_set_id, 'leftanti')
        new_brand_cluster = new_brand_cluster.withColumn('brand', explode('brand_set')).drop('brand_set')
        # insertion_brand_cluster['id','brand_set']
        print("Merged old and new graph")
        return new_brand_cluster, updated_brand_cluster
    
    def get_updated_brand_cluster(self):
        print("Generating updated brand cluster")
        if self.new_brand_cluster is None or len(self.new_brand_cluster.head(1)) == 0:
            self.merged_brand_cluster = self.updated_brand_cluster
        elif self.updated_brand_cluster is None or len(self.updated_brand_cluster.head(1)) == 0:
            self.merged_brand_cluster = self.new_brand_cluster
        else:
            old_cluster = self.old_brand_cluster.join(self.updated_brand_cluster, self.old_brand_cluster.old_brand == self.updated_brand_cluster.brand, 'leftanti')
            old_cluster = old_cluster.withColumnRenamed('old_id', 'id').withColumnRenamed('old_brand', 'brand')
            self.merged_brand_cluster = combine_dfs([old_cluster, self.updated_brand_cluster, self.new_brand_cluster])
        print("Updated brand cluster generated")
        return self.merged_brand_cluster
        # latest_brand_cluster['id','brand']

# ---------------------------------------------------------main---------------------------------------------------------------------
    
def main(args):
    s3_bucket = args['s3_bucket']
    brand_dict_database = args['brand_dict_database']
    brand_dict_table_name = args['brand_dict_table_name']
    product_matches_database = args['product_matches_database']
    product_matches_table = args['product_matches_table']
    domain = args['domain']
    bnt_file_key = args['bnt_file_key']
    

    #  input
    print('Inside main')
    product_matches = get_data_from_athena(product_matches_database, product_matches_table, None)
    print("product_matches : ", product_matches.printSchema())
    brand_dict = get_data_from_athena(brand_dict_database, brand_dict_table_name, None)
    print("brand_dict : ", brand_dict.printSchema())

    s3_resource = boto3.resource('s3')

    print("Fetching aurora db_data")
    aurora_db_data = get_data_from_aurora_db()
    old_brand_cluster = generate_existing_graph(aurora_db_data)
    db_brand_list = get_brands_in_db(aurora_db_data)

    #  preprocessing
    brand_dict = brand_dict_preprocessing(brand_dict)
    filtered_product_matches = product_matches_preprocessing(product_matches)
    brand_matches = generate_brand_matches_from_product_matches(filtered_product_matches)

    bradn_graph_vertices_df = generate_brand_graph_vertices(brand_dict)
    brand_graph_edge_df = generate_brand_graph_edges(brand_matches)

    # brand graph generation 
    print("Generating brand graph")
    brand_graph = Graph(bradn_graph_vertices_df, brand_graph_edge_df)
    brand_cluster = brand_graph.generate_filtered_clusters()
    brand_cluster.printSchema()

    # brand cluster formation 
    print("Cluster formation")
    bnt = Brand_normalization(brand_cluster, old_brand_cluster)
    new_brand_cluster, updated_brand_cluster = bnt.generate_brand_clusters()
    merged_brand_cluster = bnt.get_updated_brand_cluster()

    # saving brand clusters to s3

    print("Saving new brand cluster")  
    inserertion_data, updation_data = convert_to_database_schema(new_brand_cluster, brand_dict, db_brand_list, 'BC')
    replace_old_s3_object( inserertion_data, s3_resource, s3_bucket, bnt_file_key+'/new_brand_cluster/insert')
    replace_old_s3_object( updation_data, s3_resource, s3_bucket, bnt_file_key+'/new_brand_cluster/update')
    
    print("Saving updated brand cluster") 
    inserertion_data, updation_data = convert_to_database_schema(updated_brand_cluster, brand_dict, db_brand_list, 'BC')
    replace_old_s3_object( inserertion_data, s3_resource, s3_bucket, bnt_file_key+'/update_brand_cluster/insert')
    replace_old_s3_object( updation_data, s3_resource, s3_bucket, bnt_file_key+'/update_brand_cluster/update')

    # getting unclustered brands 
    unclustered_brands = get_unclustered_brands(brand_dict, merged_brand_cluster)
    replace_old_s3_object( unclustered_brands, s3_resource, s3_bucket, bnt_file_key+'/unmatched_brands')

    
main(args)

