import pandas as pd
from awsglue.utils import getResolvedOptions
import boto3
import io
from difflib import get_close_matches
import sys
import awswrangler as wr

args = getResolvedOptions(sys.argv, ['s3_bucket','bnt_file_key'])


def import_file_from_s3(s3_bucket, s3_key):
    print("Importing file from s3")
    path = 's3://{}/{}/'.format(s3_bucket, s3_key)
    print('IMporting file from {}'.format(path))
    df = pd.read_parquet('')
    # df = wr.s3.read_parquet(path=path)
    print("File loaded sucessfully :", len(df.index))
    return df


def geenrate_string_clusters(brand_list):
    clusters = []
    leftover_brands = []
    cluster_id = 1
    for idx, brand in enumerate(brand_list):
        print("Processing: {} out of {}".format(idx, len(brand_list)))
        print("Forming string cluster for :", brand)
        cluster = get_close_matches( brand, brand_list)
        print("Cluster : ", cluster)
        print("Before removing :", len(brand_list))

        if len(cluster) > 1:
            for item in cluster:
                brand_list.remove(item)
                temp_dict = {'cluster_id': cluster_id, 'crawled_name': item, 'source': 'SC'}
                clusters.append(temp_dict)
        
            cluster_id = cluster_id + 1      
        else :
            brand_list.remove(brand)
            temp_dict = {'cluster_id': -1, 'crawled_name': brand, 'source': 'None'}
            leftover_brands.append(temp_dict)
        print("After removing :", len(brand_list))

    return clusters, leftover_brands


def main(args):
    print('inside main')
    s3_bucket = args['s3_bucket']
    bnt_file_key = args['bnt_file_key']
    s3_unmatched_brand_object_key = bnt_file_key+'/unmatches_brands'

    unmatched_brands = import_file_from_s3(s3_bucket, s3_unmatched_brand_object_key)
    
    brand_list = unmatched_brands['crawled_name'].tolist()
    string_clusters, leftover_brands = geenrate_string_clusters(brand_list)
    
    string_clusters_df = pd.DataFrame.from_dict(string_clusters)
    leftover_brands_df = pd.DataFrame.from_dict(leftover_brands)
    
    string_clusters_s3_path = 's3a://{}/{}/'.format(s3_bucket, bnt_file_key+'/raw_string_cluster/data.csv')
    string_clusters_df.to_csv(string_clusters_s3_path, index=False)
    leftover_brands_s3_path = 's3a://{}/{}/'.format(s3_bucket, bnt_file_key+'/raw_unclustered_brands/data.csv')
    leftover_brands_df.to_csv(leftover_brands_s3_path, index=False)

main(args)