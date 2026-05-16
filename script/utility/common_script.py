from airflow.models import Variable
import json
import mysql.connector
from mysql.connector import pooling
import time
import os
from datetime import datetime
import boto3
import configparser
import sys
import pytz
from functools import lru_cache
from datetime import datetime, timedelta

env = Variable.get("env")
config_parser = configparser.ConfigParser()
config_parser.read('/app/airflow/scripts/common.param'.format(env=env))

datetime_format = '%Y-%m-%d %H-%M-%S'
secret_value_map = {}
params_map = {}


def get_param(first_tag, second_tag):
    if params_map.get(first_tag + second_tag) is not None:
        return params_map.get(first_tag + second_tag)
    cdl_param = config_parser[first_tag][second_tag].strip()
    if cdl_param is None:
        config_parser.read('/app/airflow/scripts/cdl_common_{env}.param'.format(env=env))
        cdl_param = config_parser[first_tag][second_tag].strip()
    params_map[first_tag + second_tag] = cdl_param
    return cdl_param


def get_config_value(key):
    cdl_param = config_parser['GEN'][key].strip()
    if cdl_param is None:
        config_parser.read('/app/airflow/scripts/cdl_common_{env}.param'.format(env=env))
        cdl_param = config_parser['GEN'][key].strip()
    return cdl_param


@lru_cache()
def get_secret_value(key):
    if secret_value_map.get(key) is not None:
        return secret_value_map.get(key)
    try:
        client = boto3.client('secretsmanager', get_param('GEN', 'region'))
        response = client.get_secret_value(
            SecretId=get_param('GEN', 'secret_name'))
        creds = json.loads(response['SecretString'])
        secret_value_map[key] = creds[key]
        return creds[key]
    except Exception as e:
        print("Unexpected Error : ", e)
        raise Exception('Unable to fetch secret')


def get_mysql_conn(pool={}):
    mysql_username = get_secret_value("mysql_username")
    mysql_password = get_secret_value("mysql_password")
    mysql_hostname = get_secret_value("mysql_hostname")
    mysql_db = get_param('GEN', 'mysql_db')
    SSL_CA = get_param('GEN', 'ssl_ca')
    if pool:
        return mysql.connector.pooling.MySQLConnectionPool(host=mysql_hostname, database=mysql_db, user=mysql_username,
                                                           password=mysql_password, ssl_ca=SSL_CA, use_pure=True, pool_size=pool.get("pool_size",5), pool_name=pool.get("pool_name","cdl_default_pool") )
    else:
        return mysql.connector.connect(host=mysql_hostname, database=mysql_db, user=mysql_username,
                                       password=mysql_password, ssl_ca=SSL_CA, use_pure=True)


def check_athena_query_status(response_athena):
    print(" : Query with ID :" + response_athena['QueryExecutionId'] + " in progress...")
    client_athena = boto3.client('athena', region_name=get_param('GEN', 'region'))
    while True:
        response = client_athena.get_query_execution(QueryExecutionId=response_athena['QueryExecutionId'])
        if response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
            print(" : Query - " + response['QueryExecution']['Query'] + "\n\t\t\tCompleted Successfully")
            break
        elif response['QueryExecution']['Status']['State'] == 'FAILED':
            print(" : Query - " + response['QueryExecution']['Query'] + "\n\t\t\tFailed")
            raise Exception("Query: {} has failed".format(response['QueryExecution']['Status']))
        elif response['QueryExecution']['Status']['State'] == 'CANCELLED':
            print(" : Query - " + response['QueryExecution']['Query'] + "\n\t\t\tcancelled by user")
            raise Exception("Query: {} has been canceled by user.".format(response['QueryExecution']['Status']))
        else:
            time.sleep(3)


def msck_repair_table(database_name, tb_name):
    client_athena = boto3.client('athena', region_name=get_param('GEN', 'region'))
    config = {'OutputLocation': get_param('GEN', 's3_temp_location')}
    msck_repair_query = 'msck repair table `{}`.`{}`'.format(database_name, tb_name)
    print("MSCK REPAIR in progress : " + msck_repair_query)
    response_msck_repair_table = client_athena.start_query_execution(
        QueryString=msck_repair_query,
        ResultConfiguration=config,
        WorkGroup=get_param('GEN', 'athena_workgroup')
    )
    check_athena_query_status(response_msck_repair_table)


def athena_drop_partition(database_name, table_name, frequency):
    client_athena = boto3.client('athena', region_name=get_config_value('region'))
    query = "ALTER TABLE {}.{} DROP Partition(frequency='{}') ".format(database_name, table_name, frequency)
    print("Drop partition query is ", query)
    query_response = client_athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': get_config_value('s3_temp_location')},
        WorkGroup=get_config_value('athena_workgroup'))
    check_athena_query_status(query_response)


def execute_query(query):
    mysql_connection_obj = get_mysql_conn()
    cursor = mysql_connection_obj.cursor()
    print("Executing query : \n {}".format(query))
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    mysql_connection_obj.close()
    return result


def execute_insert_query(query):
    conn = get_mysql_conn()
    cursor = conn.cursor(buffered=True)
    cursor.execute(query)
    _id = cursor.lastrowid
    conn.commit()
    cursor.close()
    conn.close()


def get_active_tables(source_name, source_type, freq='daily'):
    mysql_db, mysql_input_metadata = get_config_value('mysql_db'), get_config_value('mysql_input_metadata')
    query = "SELECT table_name from {}.{} where is_active = 'Y' and source_name = '{}' AND src_type = '{}' AND frequency = '{}' " \
        .format(mysql_db, mysql_input_metadata, source_name, source_type, freq)
    result = execute_query(query)
    return [str(r[0]) for r in result]


def get_active_tables_alert_dag(source_name, source_type, freq='daily'):
    mysql_db, mysql_input_metadata = get_config_value('mysql_db'), get_config_value('mysql_input_metadata')
    query = "SELECT table_name from {}.{} where is_active = 'Y' and source_name = '{}' AND src_type in {} AND frequency = '{}' " \
        .format(mysql_db, mysql_input_metadata, source_name, source_type, freq)
    result = execute_query(query)
    return [str(r[0]) for r in result]


def get_table_load_type(table_name, source_name, source_type):
    mysql_db, mysql_input_metadata = get_config_value('mysql_db'), get_config_value('mysql_input_metadata')
    query = "SELECT load_type from {}.{} where is_active = 'Y' and source_name = '{}' AND src_type = '{}' AND table_name = '{}'" \
        .format(mysql_db, mysql_input_metadata, source_name, source_type, table_name)
    result = execute_query(query)
    return result[0][0]


def get_table_pk(table_name, source_name, source_type):
    mysql_db, mysql_input_metadata = get_config_value('mysql_db'), get_config_value('mysql_input_metadata')
    query = "SELECT pk from {}.{} where is_active = 'Y' and source_name = '{}' AND src_type = '{}' AND table_name = '{}'" \
        .format(mysql_db, mysql_input_metadata, source_name, source_type, table_name)
    print("Query to fetch table details from Input Metadata Table : \n {}".format(query))
    result = execute_query(query)
    return result[0][0]


def get_previous_ingestion_date(table_name, source_name, source_type):
    result = datetime.strptime('2015-01-01 00-00-00', datetime_format)
    mysql_db, mysql_ingestion_metadata = get_config_value('mysql_db'), get_config_value('mysql_ingestion_metadata')
    query = "select max(inc_col_state) from {mysql_db}.{mysql_tb} where table_name = '{tb}' and src_type='{src_type}' and source_name='{src_name}' and state_of_run='success'" \
        .format(mysql_db=mysql_db, mysql_tb=mysql_ingestion_metadata, tb=table_name,
                src_type=source_type, src_name=source_name)
    print("previous ingestion details fetch query : \n {}".format(query))
    fetch_result = execute_query(query)
    if fetch_result[0][0] is not None:
        print('Prev {} Ingestion date - {}'.format(table_name, str(fetch_result[0][0])))
        date = fetch_result[0][0] if len(fetch_result[0][0]) > 10 else fetch_result[0][0] + ' 00-00-00'
        result = datetime.strptime(date, datetime_format)
    else:
        query = "select case when max(load_timestamp) is NOT NULL THEN max(load_timestamp) ELSE NULL END as load_timestamp from {mysql_db}.{mysql_tb} " \
                "where table_name = '{tb}' and src_type='{src_type}' and source_name='{src_name}' and state_of_run='success'" \
            .format(mysql_db=mysql_db, mysql_tb=mysql_ingestion_metadata, tb=table_name,
                    src_type=source_type, src_name=source_name)
        fetch_result = execute_query(query)
        if fetch_result[0][0] is not None:
            result = datetime.strptime(fetch_result[0][0], datetime_format) - timedelta(1)
    return result


def ingestion_entry(table_name, start_time, count, inc_state, source_name, source_type,
                    inc_column='load_timestamp', freq='daily'):
    """Insert RDS ingestion metadata table entry
       Args:
           table_name (str): Name of Google API
           start_time (datetime): Ingestion start time
           count (int): Total record count
           inc_state (str): Inc column value
           source_name (str): Data source name
           source_type (str): Data source type - eg: (oracle, ftp, salesforce)
           inc_column (str): Table inc column default 'load_timestamp'
           freq (str): Data ingestion frequency default 'daily'
       Returns:
           None
       """
    end_time = datetime.now()
    mysql_db, mysql_ingestion_metadata = get_config_value('mysql_db'), get_config_value('mysql_ingestion_metadata')
    load_timestamp = inc_state + ' 00-00-00' if len(inc_state) == 10 else inc_state
    try:
        exe_time = str(round((end_time - start_time).total_seconds()))
        query = "INSERT INTO {mysql_db}.{ingestion_tb} (db_name, source_name, table_name, frequency, inc_col, cdc_col, src_type, no_of_record, total_size_bytes, inc_col_state, cdc_col_state, load_type, drop_n_load, load_timestamp, start_time, end_time, total_exe_time_sec, state_of_run) " \
                "VALUES ('cdl_{env}','{source_name}','{table_name}','{freq}','{inc_column}','NA','{source_type}','{count}','1','{inc_state}','NA','{load_type}','N','{load_timestamp}','{start_time}','{end_time}','{exe_time}','Success')" \
            .format(mysql_db=mysql_db, ingestion_tb=mysql_ingestion_metadata, env=env,
                    source_name=source_name, table_name=table_name, freq=freq, inc_column=inc_column,
                    source_type=source_type, count=count, inc_state=inc_state, load_type='INC',
                    load_timestamp=load_timestamp, start_time=start_time,
                    end_time=end_time, exe_time=exe_time)
        print("Ingestion Entry for table {} query : ".format(table_name), query)
        execute_insert_query(query)
    except Exception as e:
        raise Exception("\nIngestion Entry  failed for " + table_name, e)


def update_ingestion_entry(table_name, ingestion_start_time, total_records, inc_state, source_name, source_type):
    load_timestamp = inc_state + ' 00-00-00' if len(inc_state) == 10 else inc_state
    mysql_db, mysql_ingestion_metadata = get_config_value('mysql_db'), get_config_value('mysql_ingestion_metadata')
    query = "select serial_no from {mysql_db}.{mysql_tb} where source_name='{src_name}' and table_name='{tb}' and load_timestamp='{ts}'and state_of_run='success'" \
        .format(mysql_db=mysql_db, mysql_tb=mysql_ingestion_metadata, tb=table_name,
                src_name=source_name, ts=load_timestamp)
    result = execute_query(query)
    if len(result) > 0 and result[0][0]:
        update_query = "update {mysql_db}.{mysql_tb} set no_of_record='{total_records}' where serial_no='{serial_no}'".format(
            mysql_db=mysql_db, mysql_tb=mysql_ingestion_metadata, total_records=total_records, serial_no=result[0][0])
        execute_insert_query(update_query)
    else:
        ingestion_entry(table_name, ingestion_start_time, total_records, inc_state, source_name, source_type)

def convert_tz(tz='UTC', format='%Y-%m-%d %H:%M:%S'):
	UTC = pytz.utc
	IST = pytz.timezone('Asia/Kolkata')
	EST = pytz.timezone('US/Eastern')
	if tz == 'UTC'.lower():
		dt = datetime.now(UTC).strftime(format)
	elif tz == 'IST'.lower():
		dt = datetime.now(IST).strftime(format)
	elif tz == 'EST'.lower():
		dt = datetime.now(EST).strftime(format)
	return dt
