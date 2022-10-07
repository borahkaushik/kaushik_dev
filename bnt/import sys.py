import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from functools import reduce

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

def get_data_from_s3(s3_bucket, domain, date, file_type):
    s3_path = 's3://{}/type={}/domain={}/year={}/month={}/day={}'.format(s3_bucket, file_type, domain, date['year'], date['month'], date['day'])
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
    # print("S3 Data fetched = {}".format(spark_df.count()))
    return spark_df

def combine_dfs(df_list):
    print("Combning dfs")
    combined_df = reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), df_list)
    print("Combined df size = {}".format(combined_df.count()))
    return combined_df
    
def save_df_to_s3(df, s3_bucket, domain, date, file_type):
    s3_path = 's3://{}/type={}/domain={}/year={}/month={}/day={}'.format(s3_bucket, file_type, domain, date['year'], date['month'], date['day'])
    print("Saving df to loc = {}".format(s3_path))
    df.write.mode("overwrite").option("header",True).parquet(s3_path)
    print("dfs saved successfully")

def get_current_week_exact_matches(exact_matches, id_uuid):
    uuid_a_exact_matches = exact_matches.join(id_uuid, exact_matches.uuid_a ==  id_uuid.uuid,"inner").drop("id", "uuid")
    uuid_a_b_exact_matches = uuid_a_exact_matches.join(id_uuid, exact_matches.uuid_b ==  id_uuid.uuid,"inner").drop("id", "uuid")
    print("Counting items")
    print(uuid_a_b_exact_matches.count())
    print(uuid_a_b_exact_matches.show())
    return uuid_a_b_exact_matches
    
def get_date(day, month, year):
    date = {}
    date['year'] = year
    date['month'] = month
    date['day'] = day
    return date

def main():
    s3_bucket = args["BUCKET"]
    domain = args["SEGMENT"]
    curr_day = args["DAY"] 
    curr_month = args["MONTH"] 
    curr_year = args["YEAR"]
    prev_day = args["PREV_DAY"] 
    prev_month = args["PREV_MONTH"] 
    prev_year = args["PREV_YEAR"] 
    current_date = get_date(curr_day, curr_month, curr_year)
    prev_date = get_date(prev_day, prev_month, prev_year)
    src_file_type = "exact_matches"
    dest_file_type = "delta_to_full_run_exact_matches"
    
    delta_run_exact_matches = get_data_from_s3(s3_bucket, domain, current_date, src_file_type)
    prev_week_full_run_exact_matches = get_data_from_s3(s3_bucket, domain, prev_date, dest_file_type)
    complete_exact_matches = combine_dfs([delta_run_exact_matches, prev_week_full_run_exact_matches])
    id_uuid = get_data_from_s3(s3_bucket, domain, current_date, "id_uuid")
    curr_week_full_run_exact_matches = get_current_week_exact_matches(complete_exact_matches, id_uuid)
    # save_df_to_s3(curr_week_full_run_exact_matches, s3_bucket, domain, current_date, dest_file_type )
    
    