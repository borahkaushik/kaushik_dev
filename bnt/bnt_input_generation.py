import awswrangler as wr
import pandas as pd

def lambda_handler(event, context):
    s3_bucket = event['s3_bucket'] if 's3_bucket' in event else 'ml-stack.prod'
    domain = event['domain'] if 'domain' in event else 'pets'
    exact_matches_db = event['exact_matches_db'] if 'exact_matches_db' in event else 'ml_stack_prod_matches'
    exact_matches_table = event['exact_matches_table'] if 'exact_matches_table' in event else 'match_data_warehouse_v2'
    crawled_data_db = event['crawled_data_database'] if 'crawled_data_database' in event else 'bungeedatawarehouse'
    crawled_data_table = event['crawled_data_table'] if 'crawled_data_table' in event else 'bungee_global_prod_d'
    match_source_list = event['match_source'] if 'match_source' in event else ['fastlane']
    temp_db = event['bnt_input_database'] if 'bnt_input_database' in event else "ml_temp_table"
    temp_product_matches_table = event['bnt_input_table'] if 'bnt_input_table' in event else "bnt_input_delta_exact_matches"
    store_name_list = event['store_names'] if 'store_names' in event else ['petcarerx','petsmart','lovegroomers','ryanspet','tractorsupplyrx','californiapetpharmacy','petsuppliesplus','petedge','petmeds','entirelypetspharmacy','chewy',
                        'vcahospitals','heartlandvetsupply','petco','allivet','smartpakequine','petflow','jefferspet','tractorsupply','entirelypets','groomerschoice','baxterboo']
    brand_dict_db = event['brand_dict_db'] if 'brand_dict_db' in event else 'bungeedatalake'
    brand_dict_table = event['brand_dict_table'] if 'brand_dict_table' in event else 'distinct_new_brands'
    
    bnt_object_key = "brand_normalization"
    bnt_input_object_key = "bnt_input"
    exact_matches_temp_table_creation_query = get_temporary_delta_product_matches_table_query(exact_matches_db, exact_matches_table, crawled_data_db, crawled_data_table, domain, match_source_list, store_name_list)
    exact_matches_s3_output_path = "{folder}/{sub_folder}/{suffix}/".format(folder = bnt_object_key, sub_folder = bnt_input_object_key, suffix = "product_matches")
    create_temp_table(s3_bucket, exact_matches_temp_table_creation_query, exact_matches_db, temp_db, temp_product_matches_table, exact_matches_s3_output_path)
    
    
    brand_dict_temp_table_creation_query = get_temporary_brand_dict_table_query(brand_dict_table)
    brand_dict_s3_output_path = "{folder}/{sub_folder}/{suffix}/".format(folder = bnt_object_key, sub_folder = bnt_input_object_key, suffix = "brand_dict")
    create_temp_table(s3_bucket, brand_dict_temp_table_creation_query, brand_dict_db, temp_db, brand_dict_table, brand_dict_s3_output_path)

    return {
        'statusCode': 200,
        'body': {
            's3_bucket': s3_bucket,
            'domain': domain, 
            'bnt_input_db': temp_db, 
            'bnt_input_table':temp_product_matches_table, 
            'brand_dict_db' : temp_db,
            'brand_dict_table' : brand_dict_table,
            'bnt_file_key' : bnt_object_key
        }
    }

def get_exact_matches_date(database, domain, match_source_list):
    query = get_exact_matches_date_fetching_query(domain, match_source_list)
    
    result_df = run_athena_query(database, query)

    dates = []
    for idx in result_df.index:
        dates.append(int(result_df['year'][idx]) * 10000 + int(result_df['month'][idx]) * 100 + int(result_df['day'][idx]) * 1)
        
    dates = sorted(dates, reverse=True)
        
    return dates

def convert_to_date_map(date_int):
    str_date = str(date_int)
    date = {}
    date['day'] = str_date[6:]
    date['month'] = str_date[4:6]
    date['year'] = str_date[0:4]

    return date

# ---------------------------generate queries-------------------------------------------------------------

def get_exact_matches_date_fetching_query(domain, match_source_list):
    query = """SELECT day, month, year 
    FROM match_data_warehouse_v2 
    WHERE 
        segment = '{domain}' and match_source in ({match_source_list})
        GROUP BY day, month, year
    """.format(domain = domain, match_source_list = match_source_list)
    query = query.replace('\n',' ').replace('[','').replace(']','')
    return query
    
def get_temporary_delta_product_matches_table_query(exact_matches_database, exact_matches_table, crawled_data_database, crawled_data_table, domain, match_source_list, store_name_list):
    date_list = get_exact_matches_date(exact_matches_database, domain, match_source_list)
    curr_date = convert_to_date_map(date_list[0])
    prev_date = convert_to_date_map(date_list[1])
    filter_string= """replace(replace(replace(replace(trim(replace(translate(regexp_replace({brand}, '[一-龠]+|[ァ-ヴー]+|[ぁ-ゔ]', ''),chr(154)||chr(155)||chr(156)||chr(157)||chr(158)||
																chr(159)||chr(160)||chr(161)||chr(162)||chr(163)||chr(164)||chr(165)||
																chr(166)||chr(167)||chr(168)||chr(169)||chr(170)||chr(171)||chr(172)||
																chr(173)||chr(174)||chr(175)||chr(176)||chr(177)||chr(178)||chr(179)||
																chr(180)||chr(181)||chr(182)||chr(183)||chr(184)||chr(185)||chr(186)||
																chr(187)||chr(191)||chr(192)||chr(193)||
																chr(194)||chr(195)||chr(196)||chr(197)||chr(198)||chr(199)||chr(200)||
																chr(201)||chr(202)||chr(203)||chr(204)||chr(205)||chr(206)||chr(207)||',Σ≤た®Â™?^)('||
																chr(208)||chr(209)||chr(210)||chr(211)||chr(212)||chr(213)||chr(214)||
																chr(215)||chr(216)||chr(217)||chr(218)||chr(219)||chr(220)||chr(221)||
																chr(222)||chr(223)||chr(224)||chr(225)||chr(226)||chr(227)||chr(228)||
																chr(229)||chr(230)||chr(231)||chr(232)||chr(233)||chr(234)||chr(235)||
																chr(236)||chr(237)||chr(238)||chr(239)||chr(240)||chr(241)||chr(242)||
																chr(243)||chr(244)||chr(245)||chr(246)||chr(247)||chr(248)||chr(249)||
																chr(250)||chr(251)||chr(252)||chr(253)||chr(254)||chr(255), '^^^^^^'),'^','')),'   ',' '), '  ', ' '), '  ', ' '), '  ', ' ')"""
                
    brand_a_filter = filter_string.format(brand = 'brand_a')
    brand_b_filter = filter_string.format(brand = 'brand_b')
                
    query = """
            SELECT  sku_uuid_a, sku_uuid_b, {brand_a_filter} brand_a , {brand_b_filter} brand_b
            FROM (
                SELECT sku_uuid_a, sku_uuid_b, brand_a, prod_d.brand as brand_b
                FROM (
                    SELECT sku_uuid_a, sku_uuid_b, prod_d.brand as brand_a
                    FROM (
                        SELECT  distinct "sku_uuid_a", "sku_uuid_b" 
                        FROM "{exact_matches_database}"."{exact_matches_table}" 
                        WHERE year = '{curr_year}' and month ='{curr_month}' and day ='{curr_day}' and segment='{domain}' and match_source in ({match_source})
                        EXCEPT
                        SELECT  distinct "sku_uuid_a", "sku_uuid_b" 
                        FROM "{exact_matches_database}"."{exact_matches_table}"
                        WHERE year = '{prev_year}' and month ='{prev_month}' and day ='{prev_day}' and segment='{domain}' and match_source in ({match_source})
                    ) delta_exact_matches
                    JOIN (
                        SELECT uuid_n, brand 
                        FROM "{crawled_data_database}"."{crawled_data_table}"
                        WHERE product_segment='pets' and source in ({store_name})
                    ) prod_d
                    on sku_uuid_a = uuid_n
                    ) delta_exact_matches
                JOIN (
                    SELECT uuid_n, brand 
                    FROM "{crawled_data_database}"."{crawled_data_table}"
                    WHERE product_segment='pets' and source in ({store_name})
                ) prod_d
                on sku_uuid_b = uuid_n
            )
            """
    query = query.format( exact_matches_database = exact_matches_database, exact_matches_table = exact_matches_table, crawled_data_database = crawled_data_database, crawled_data_table = crawled_data_table,
                        brand_a_filter = brand_a_filter , brand_b_filter = brand_b_filter, domain = domain, match_source = match_source_list, store_name = store_name_list, 
                         prev_year = prev_date["year"], curr_year = curr_date["year"], prev_month = prev_date["month"], curr_month = curr_date["month"], prev_day = prev_date["day"], curr_day = curr_date["day"])
    query = query.replace('\n',' ').replace('[','').replace(']','')
    return query

def get_temporary_brand_dict_table_query(brand_dict_table):
    return """SELECT * FROM {table}""".format(table = brand_dict_table)

# ----------------processing temp table---------------------------------------------------------

def run_athena_query(database , query):
    print("Executing Query: ", query, "\n On Database: ", database)
    try : 
        df_iter = wr.athena.read_sql_query(
                sql = query,
                database = database,
                ctas_approach=True,
                chunksize=True
        )
    
        result_df = pd.concat(df_iter)
        return result_df
    except Exception as e:
        raise Exception(e)

def delete_athena_table(database, table, s3_bucket, file_key):
    res = wr.catalog.delete_table_if_exists(database = database, table = table)
    s3_path = "s3://{bucket}/{suffix}".format(bucket = s3_bucket, suffix = file_key)
    wr.s3.delete_objects(s3_path)
    if res :
        print('Exixting table deleted')
    else :
        print('Table not present in athena')
        
def create_ctash_table(s3_bucket, s3_suffix, query, source_db, ctash_db, temp_table):
    s3_output_object = "s3://{bucket}/{suffix}".format(bucket = s3_bucket, suffix = s3_suffix)
    print('Creating temp table : {temp_db}.{temp_table} , \nSource Database : {db}, \nS3 output object key : {s3}'.format(temp_db = ctash_db ,temp_table = temp_table , db = source_db, s3 = s3_output_object))
    wr.athena.create_ctas_table(
        sql=query,
        database = source_db,
        ctas_database=ctash_db,
        ctas_table = temp_table,
        s3_output= s3_output_object,
        wait=True
    )
    print('New table created')
    
def create_temp_table(s3_bucket, table_creation_query, source_db, temp_db, temp_table, s3_suffix):
    print("Table creation query :", table_creation_query)
    delete_athena_table(temp_db, temp_table, s3_bucket, s3_suffix)
    create_ctash_table(s3_bucket, s3_suffix, table_creation_query, source_db, temp_db, temp_table)