# import ast
# import hashlib
# import json
# import boto3
# import sys
# # sys.path.insert(1, '/app/airflow/scripts/')
# from cdl_common_script import *
# import csv
# import codecs

# schema_tracker_table = get_param('GEN', 'schema_tracker_table')
# metadata_database = get_param('GEN', 'mysql_db')
# metadata_input_metadata_table = get_param('GEN', 'mysql_input_metadata')
# metadata_ingestion_table = get_param('GEN', 'mysql_ingestion_metadata')
# bucket_name = get_param('GEN', 'landing_bucket_name')
# region_name = get_param('GEN', 'region')
# athena_db = get_param('GEN', 'athena_landing_db')
# catalog_name = "AwsDataCatalog"
# prefix = ''
# job_table_name = ''
# job_src_type = 'oracle'
# job_source_name = "revitas"
# job_data_file_type = "CSV"

# table_name_list = []
# audit_success_list_str = sys.argv[1]
# audit_success_list = ast.literal_eval(audit_success_list_str)
# print(audit_success_list)


# # ===========================================================================
# # Executes query in mysql
# # ==========================================================================
# def execute_mysql_query(query):
#     print("execute_mysql_query query:{} :::: started".format(query))
#     cursor.execute(query)
#     return cursor.fetchall()


# # ==============================================================================================
# # This function is used to get filter out files as a list for which header needs to be taken
# # ==============================================================================================
# def get_file_list(bucket_prefix, load_ts_mysql):
#     print("get_file_list :: started")
#     bucket_prefix = bucket_prefix + "/load_timestamp=" + load_ts_mysql
#     response = boto3.client('s3').list_objects(Bucket=bucket_name, Prefix=bucket_prefix)
#     filename_list = []
#     for info in response['Contents']:
#         key_s3 = info["Key"]
#         folders = key_s3.split("/")
#         if len(folders[4]) > 0:
#             load_ts_s3 = key_s3.split("/")[3].split("=")[1]
#             if load_ts_s3 == load_ts_mysql:
#                 filename_list.append(key_s3)
#     print("get_file_list :: Ended")
#     return filename_list


# # =============================================================================
# # This function is used to populate schema_tracker_table.
# # =============================================================================
# def insert_schema_tracker(serial_no_mysql, load_ts_mysql, hash_value, schema_check_str):
#     schema_check_str = schema_check_str.replace("'", "")
#     insert_query = "INSERT into {}.{}(Ingestion_id,Source_ingestion,Object_Name,Hash_schema,Actual_schema," \
#                    "Latest_version,Load_timestamp) VALUES({},'{}','{}','{}','{}','Y','{}')" \
#         .format(metadata_database, schema_tracker_table, serial_no_mysql, job_source_name,
#                 job_table_name, hash_value, schema_check_str, load_ts_mysql)
#     cursor.execute(insert_query)
#     connection.commit()


# # ===================================================================================
# # This function will update the schema_tracker table for latest version changes.
# # ===================================================================================
# def update_schema_tracker():
#     print("Updating schema tracker latest version to 'N' for ")
#     query = "update {}.{} set Latest_version ='N' where Latest_version = 'Y' " \
#             "and Source_ingestion = '{}' and Object_Name = '{}'" \
#         .format(metadata_database, schema_tracker_table, job_source_name, job_table_name)
#     cursor.execute(query)
#     connection.commit()


# def update_input_metadata():
#     print("Updating latest hash value in input metadata")
#     update_input_metadata_query = """update {}.{} set columns_hash = '{}' 
#     where table_name = '{}' and src_type = '{}' and source_name = '{}';""" \
#         .format(metadata_database, metadata_input_metadata_table, hash_value_from_s3,
#                 job_table_name, job_src_type,job_source_name)
#     cursor.execute(update_input_metadata_query)
#     connection.commit()


# # =====================================================================================================
# # This function is used to get headers for the list of file for which we need to genrate hash values
# # ======================================================================================================
# def read_first_line_from_s3_file(info, file_type):
#     try:
#         if file_type in ["CSV", "TSV"]:
#             client = boto3.client("s3", "us-east-1")
#             csv_obj = client.get_object(Bucket=bucket_name, Key=info)
#             lines = csv_obj['Body'].read().splitlines(True)
#             reader = csv.reader(codecs.iterdecode(lines, 'utf-8'))
#             return next(reader)[0]
#     except Exception:
#         raise Exception("Error occurred while extracting schema from s3 files ")


# def extract_columns_from_record(str_s, file_type):
#     str_s = str(str_s, 'utf-8').rstrip('\n').replace(" ","_").replace("-","_")
#     if file_type == "JSON":
#         str_j = json.loads(str_s)
#         return list(str_j.keys())
#     elif file_type in ["CSV", "TSV"]:
#         return get_list_from_string(str_s)
#     else:
#         raise Exception("Unknown job_data_file_type encountered!")


# # ======================================================================
# # This function is responsible for fetching tables schema from athena
# # ======================================================================
# def get_athena_table_schema(table):
#     print("get_athena_table_schema db={}, region={}, table={}, catalog={} ::::: Started"
#           .format(athena_db, region_name, table, catalog_name))
#     client = boto3.client("athena", region_name)
#     response = client.get_table_metadata(CatalogName=catalog_name, DatabaseName=athena_db, TableName=table)

#     athena_columns = []
#     if bool(response) and bool(response['TableMetadata']) and len(response['TableMetadata']['Columns']) > 1:
#         print("Schema retrieved from athena is valid")
#         for column in response['TableMetadata']['Columns']:
#             athena_columns.append(column['Name'])
#     print("get_athena_table_schema ::::: Ended")
#     return athena_columns


# # =========================================================================================================
# # This function will give the result after list comparison btw athena and s3 and highlight the differences
# # =========================================================================================================
# def expand_list(columns, s3_compare):
#     list_analysis = []
#     for element in s3_compare:
#         if element in columns:
#             list_analysis.append(element)
#         else:
#             list_analysis.append("NEW_FIELD_ADDED_HERE/FIELD_NAME_CHANGE")
#     return list_analysis


# def get_list_from_string(list_str):
#     list_str = list_str.lower()
#     if '|' in list_str:
#         return list_str.split('|')
#     elif ',' in list_str:
#         return list_str.split(',')
#     else:
#         raise Exception("Delimiter is not valid")


# def is_schema_equal(athena_columns, s3_column):
#     s3_columns = []
#     for x in s3_column:
#         s3_columns.append(x.lower())
#     if job_data_file_type == "CSV" or job_data_file_type == "TSV":
#         if athena_columns == s3_columns:
#             return True
#         else:
#             if len(athena_columns) == len(s3_columns):
#                 raise Exception('Equal Length schema from CSV is different S3 schema already existing on Athena')
#             elif len(athena_columns) < len(s3_columns):
#                 if athena_columns == s3_columns[0:len(athena_columns)]:
#                     print("New Columns found are sequentially correct for CSV,"
#                           " but returning False as crawler needs to rerun")
#                     return False
#                 else:
#                     raise Exception('S3 schema is different from schema already existing on Athena')
#             else:
#                 if s3_columns == athena_columns[0:len(s3_columns)]:
#                     print(
#                         "Few columns are removed in new S3 schema, returning True as no. of columns will "
#                         "remain same due to old data")
#                     return True
#                 else:
#                     raise Exception('S3 schema is different from schema already existing on Athena')
#     else:
#         return sorted(athena_columns) == sorted(s3_columns)


# def handle_first_time_schema():
#     print("handle_first_time_schema ::::: Started")
#     update_input_metadata()
#     result_set = execute_mysql_query(select_ingestion_metadata_query)
#     insert_schema_tracker(result_set[0][0], result_set[0][1], hash_value_from_s3, first_file_schema_str)
#     print("handle_first_time_schema ::::: Ended")
#     return True


# def handle_update_schema():
#     print("handle_update_schema ::::: Started")
#     athena_table_columns = get_athena_table_schema(job_table_name)
#     update_schema_tracker()
#     result_set = execute_mysql_query(select_ingestion_metadata_query)
#     insert_schema_tracker(result_set[0][0], result_set[0][1], hash_value_from_s3, first_file_schema_str)
#     update_input_metadata()
#     print("handle_update_schema ::::: Ended")
#     if is_schema_equal(athena_table_columns, first_file_schema_str.split(",")):
#         print("Crawler needs not to be run as schema is same")
#         return False
#     else:
#         print("Crawler needs to be run as schema has changed")
#         return True


# def should_run_crawler():
#     print("hash_value_from_mysql:::::::: {}".format(hash_value_from_mysql))
#     print("Calling should_run_crawler")
#     if not hash_value_from_mysql:
#         print("Crawler needs to run as schema loaded for first time job_src.job_table::{}.{}"
#               .format(job_src_type, job_table_name))
#         return handle_first_time_schema()
#     elif hash_value_from_mysql == hash_value_from_s3:
#         print("Crawler can be skipped as schema is same for job_src.job_table::{}.{}"
#               .format(job_src_type, job_table_name))
#         return False
#     else:
#         print("Schema updated for job_src.job_table::{}.{}".format(job_src_type, job_table_name))
#         print(".............................=============={}".format(handle_update_schema()))
#         return handle_update_schema()


# # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
# # CODE BEGINS HERE CODE BEGINS HERE CODE BEGINS HERE CODE BEGINS HERE CODE BEGINS HERE
# # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

# elements_equal = True
# connection = get_mysql_conn()
# cursor = connection.cursor()

# status_list = []


# def get_comma_seprated_string(first_file_first_line):
#     if '|' in first_file_first_line:
#         return first_file_first_line.replace('|', ',')
#     elif ',' in first_file_first_line:
#         return first_file_first_line
#     else:
#         raise Exception("Delimiter is not valid")

# for tables in audit_success_list:
#     job_table_name = tables[1]
#     prefix = "{}/{}/frequency=daily".format(job_source_name, job_table_name)
#     print("Prefix going to be used for this ingestion of is : {}".format(prefix))
#     select_schema_hash_query = "select columns_hash from {}.{} where table_name = '{}' and src_type = '{}' and is_active = 'Y'".format(
#         metadata_database, metadata_input_metadata_table, job_table_name, job_src_type)
#     select_ingestion_metadata_query = "select serial_no,load_timestamp,table_name from {}.{} where table_name = '{}' " \
#                                       "and src_type = '{}' and source_name = '{}' order by start_time desc limit 1;" \
#         .format(metadata_database, metadata_ingestion_table, job_table_name, job_src_type,job_source_name)
#     hash_value_from_mysql = execute_mysql_query(select_schema_hash_query)[0][0]
#     hash_value_from_s3 = None

#     # === This chunk will execute the sql statement to get latest load timestamp from mysql ingestion metadata table
#     last_load_ts_from_mysql = execute_mysql_query(select_ingestion_metadata_query)[0][1]
#     print("Recent load timestamp from mysql in str for job : {}".format(last_load_ts_from_mysql))

#     # ======== Calling out function get_file_list for which schema extraction will be done ============
#     print('Prefix is ::',prefix,'and load_ts ::',last_load_ts_from_mysql )
#     s3_file_names = get_file_list(prefix, last_load_ts_from_mysql)
#     print("Files for which schema extraction will proceed : {}".format(s3_file_names))
#     print('S3_dile_name zero is ::::::',s3_file_names[0])
#     first_file_first_line = read_first_line_from_s3_file(s3_file_names[0], job_data_file_type)
#     first_file_schema_str = get_comma_seprated_string(first_file_first_line)


#     # hardcoded value true is used as all files in the folder, schema comparison is not required
#     if elements_equal:
#         md5_hash = hashlib.md5(first_file_schema_str.lower().encode())
#         hash_value_from_s3 = md5_hash.hexdigest()
#         print("Files have same schemas in s3.\nHash values from s3: {}".format(hash_value_from_s3))
#     else:
#         print("Files have different schemas in s3.")
#         raise ValueError('Multiple files with different schema found')

#     run_crawler = should_run_crawler()
#     status_list.append([tables[0], tables[1], run_crawler])
# print('"' + str(status_list) + '"')
