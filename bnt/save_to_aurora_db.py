import pymysql as sq
import boto3
import psycopg2
import pandas as pd
import awswrangler as wr


pd.options.mode.chained_assignment = None
def connect_to_database():
    print('Connecting to database')
    try:
        
        ENDPOINT = "sandbox-db-cluster.cluster-cf6pgrepnkpy.us-east-1.rds.amazonaws.com"
        PORT = 5432
        USR = "postgres"
        DBNAME = "BungeeMatchPool"
        PASS= "Bungee_sand_rds_2021!"
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USR, password=PASS)
        print('Database connection succcessful')
        return conn
    except Exception as error:
        print(f'in Creating connection ! {error}')

def open_file(s3_bucket, s3_key):    
    print("Importing file from s3")
    path = 's3://{}/{}'.format(s3_bucket, s3_key)
    print('IMporting file from {}'.format(path))
    df = wr.s3.read_parquet(path=path)
    df = df[["cluster_id", "crawled_name", "source", "status", "updated_date", "product_url_1", "image_url_1", "product_url_2"]]
    df = df.fillna("")
    df.insert(2, "standard_name", "")
    df.rename(columns = {'image_url_1':'image_url'}, inplace = True)
    print("File loaded sucessfully :", len(df.index))
    return df

def get_db_query(source_df,  target_db_table, action):

    string_dtypes = source_df.convert_dtypes().select_dtypes("string")
    source_df[string_dtypes.columns] = string_dtypes.apply(lambda x: x.str.replace('"',"")).apply(lambda x: x.str.replace("'",""))
    print(len(source_df.index))
    source_df.drop_duplicates(subset="crawled_name", keep='first', inplace=True)
    print(len(source_df.index))
    if action == 'INSERT':
        print("Generating insert query")
        query  = 'INSERT INTO '+target_db_table+' ('+ str(', '.join(source_df.columns))+ ') VALUES '
        for index, row in source_df.iterrows():
            query = query + str(tuple(row.values)) + ','
        query = query[:-1]

    elif action == 'UPDATE':
        print("Generating update query")
        source_df.insert(2, "user_id", "")
        source_df.insert(2, "current_step", "")
        query = "UPDATE "+ target_db_table+ " SET "
        for column_name in source_df.columns:
            if column_name in ['crawled_name']:
                continue
            if column_name == 'updated_date':
                sub_query =  " " + column_name + " = CASE crawled_name "
                for index, row in source_df.iterrows():
                    sub_query = sub_query + "  WHEN '" + str(row.crawled_name) + "' THEN TO_TIMESTAMP('" + str(row[column_name]) + "', 'YYYY-MM-DD HH24:MI:SS.MS') "

                sub_query = sub_query + " END,"
                query = query + sub_query
            else:
                sub_query =  " " + column_name + " = CASE crawled_name "
                for index, row in source_df.iterrows():
                    sub_query = sub_query + "  WHEN '" + str(row.crawled_name) + "' THEN '" + str(row[column_name]) + "' "

                sub_query = sub_query + " END,"
                query = query + sub_query


        query = query.rstrip(query[-1])

        id_list = str("', '".join(source_df.crawled_name))
        query  = query + " WHERE crawled_name in ('" + id_list + "') "


    query = query.replace("\\","")
    print('Query generated')
    return query 


def show_db( table_name):
    db_connection = connect_to_database()
    cur = db_connection.cursor()
    cur.execute("""SELECT * from {}""",format(table_name))
    query_results = cur.fetchall()
    print(f'Query results: {query_results}')
    print(len(query_results))
    db_connection.close()
    
    
def execute_db_query( query):
    print('Starting execution')
    db_connection = connect_to_database()
    cur = db_connection.cursor()
    try : 
        cur.execute(query)
        db_connection.commit()
        print('executing finished successfully')
        db_connection.close()
    except Exception as e:
        print("Failure :", e)
        raise Exception(e)
        db_connection.close ()
    
s3_client = boto3.client('s3')

def main(args):
    try:
        table_name = args['table_name']
        s3_bucket = args['s3_bucket']
        
        # Create connection to Wasabi / S3
        s3 = boto3.resource('s3')
        print("Start")
        # Get bucket object
        # my_bucket = s3.Bucket('ml-stack.prod')
        # insertion_s3_object_keys = []
        # updation_s3_object_keys = []
        # for obj in my_bucket.objects.filter(Prefix="brand_normalization"):
        #     if 'leftover_brands' in obj.key
        #         if 'insert' in obj.key:
        #             insertion_s3_object_keys.append(obj.key)
        #         elif 'update' in obj.key:
        #             updation_s3_object_keys.append(obj.key)
        
        insertion_s3_object_keys = ['brand_normalization/leftover_brands/insert/part-00000-2ea9511c-dcaf-4e76-b158-975a662fd038-c000.snappy.parquet', 'brand_normalization/leftover_brands/insert/part-00001-2ea9511c-dcaf-4e76-b158-975a662fd038-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00000-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00001-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00002-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00003-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00004-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00005-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00006-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00007-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00008-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00009-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00010-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_brand_cluster/insert/part-00011-6b36c831-3780-4fed-af7a-ed59867271d9-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00014-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00015-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00016-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00017-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00018-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00019-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00020-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00021-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00022-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet', 'brand_normalization/new_string_cluster/insert/part-00023-e69b9c4f-55a0-4cb2-9c22-0e7f9e12aea8-c000.snappy.parquet']
        updation_s3_object_keys = []

        print(insertion_s3_object_keys)
        print(updation_s3_object_keys)
        print("End")
        df_list = []
        print("Fetching dataframes")
        for s3_object in insertion_s3_object_keys:
            df = open_file(s3_bucket, s3_object)
            if len(df.index) == 0:
                print("0 size df")
                continue
            print("Appending df")
            df_list.append(df)
        print("Combining dataframes")   
        insertion_df = pd.concat(df_list, axis=0)    
        print("Executing: insertion ", s3_object)
        query = get_db_query(insertion_df, table_name, 'INSERT')
        execute_db_query(query)
        
        df_list = []
        print("Fetching dataframes")
        for s3_object in updation_s3_object_keys:
            df = open_file(s3_bucket, s3_object)
            if len(df.index) == 0:
                continue
            df_list.append(df)
        print("Combining dataframes")    
        updation_df = pd.concat(df_list, axis=0)    
        print("Executing: insertion ", s3_object)
        query = get_db_query(updation_df, table_name, 'UPDATE')
        execute_db_query(query)

    except Exception as err:
        return

args = {}
args['table_name'] = 'brand_standardization'
args['s3_bucket'] = 'ml-stack.prod'
main(args)