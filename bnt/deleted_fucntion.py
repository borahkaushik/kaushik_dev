
# class String_Clustering:
#     def  __init__(self, brand_cluster, brand_dict):
#         self.normalized_brands = brand_cluster # latest_brand_cluster['id','brand_id_set']
#         self.brand_dict = brand_dict
#         self.crawled_brands = self.get_crawled_brands_list()

#     def get_crawled_brands_list(self):
#         return self.brand_dict.select( self.brand_dict.crawled_name.alias('brand'))
#         # crawled_brands['id','brand']

#     def get_unnormalized_brands(self):
#         return self.crawled_brands.join(self.normalized_brands, self.crawled_brands.brand == self.normalized_brands.brand, 'leftanti')
#         # crawled_brands['id','brand']

#     def get_string_clusters(self, brand_list):
#         print("Generating string clusters")
#         clusters = []
#         for brand in brand_list:
#             cluster = get_close_matches(brand, brand_list)
#             clusters.append(cluster)

#         cluster_string = [ [','.join(cluster)] for cluster in clusters]
#         column = ['string_clusters']
#         brand_string_df = spark.createDataFrame(cluster_string, column)
#         brand_string_cluster_df = brand_string_df.select(array_sort(split(col("string_clusters"),",")).alias("brand_cluster")).drop("string_clusters")
#         brand_string_cluster_df = brand_string_cluster_df.filter(size(brand_string_cluster_df.brand_cluster)>1).drop_duplicates()
#         print("Generated string clusters")
#         return brand_string_cluster_df

#     def cluster_unnormalized_brands(self):
#         print("clustering barnds not grouped by brand cluster")
#         self.unclustered_brands = self.get_unnormalized_brands()
#         unclustered_brands_list = self.unclustered_brands.rdd.map(lambda x:x[0]).collect()
#         string_cluster = self.get_string_clusters(unclustered_brands_list)
#         idx = get_max_id(self.normalized_brands)
#         id_string_cluster = insert_id(string_cluster, idx+1)
#         self.id_string_cluster = id_string_cluster.withColumn('brand', explode('brand_cluster')).drop('brand_cluster')
#         print("clusterd non grouped brands")
#         return self.id_string_cluster

#     def get_unclustered_brands(self):
#         print("Fetching unclustered brands")
#         if self.id_string_cluster == None:
#             print("Unclustered brands fetched")
#             return self.unclustered_brands
#         else :
#             self.unclustered_brands = self.unclustered_brands.subtract(self.id_string_cluster.select('brand'))
#             print("Unclustered brands fetched")
#             return self.unclustered_brands 



    # # string cluster formation 
    # str_cluster = String_Clustering(merged_brand_cluster, brand_dict)
    # string_cluster = str_cluster.cluster_unnormalized_brands()
    # unclustered_brands = str_cluster.get_unclustered_brands()
    
    # # saving string cluster to s3
    # print("Saving string cluster") 
    # inserertion_data, updation_data = convert_to_database_schema(string_cluster, brand_dict, db_brand_list, 'SC')
    # replace_old_s3_object( inserertion_data, s3_client, s3_bucket, bnt_file_key+'/new_string_cluster/insert')
    # replace_old_s3_object( updation_data, s3_client, s3_bucket, bnt_file_key+'/new_string_cluster/update')
    
    # # saving unclustered brands to s3
    # print("Saving unclustered brands") 
    # inserertion_data, updation_data = convert_to_database_schema(unclustered_brands, brand_dict, db_brand_list, 'UC')
    # replace_old_s3_object( inserertion_data, s3_client, s3_bucket, bnt_file_key+'/unclustered_brands/insert', brand_dict)
    # replace_old_s3_object( updation_data, s3_client, s3_bucket, bnt_file_key+'/unclustered_brands/update', brand_dict)