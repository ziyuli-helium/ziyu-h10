import boto3
import pandas as pd
import psycopg2
import mysql.connector
import sshtunnel
import json

import sys
sys.path.append('/Users/ziyuli/Desktop/ziyu-h10/db_connect')

with open('/Users/ziyuli/Desktop/ziyu-h10/db_connect/db_connect/credential.txt') as json_file:
    credential = json.load(json_file)

def db_connect_pgsql(database):
    param_dic_h10 = {
    "host" : credential[database]["host"],
    "user" : credential[database]["user"],
    "database" : credential[database]["database"],
    "password": credential[database]["password"]
    }

    conn = psycopg2.connect(**param_dic_h10)

    return conn

def db_connect_mysql(remote_bind_address,database,query):
    with sshtunnel.SSHTunnelForwarder(
        ('3.91.34.122', 22),
        ssh_username = 'ziyu.li',
        ssh_password = '/Users/ziyuli/.ssh/id_rsa',
        remote_bind_address = (remote_bind_address, 3306)
    ) as tunnel:
        conn = mysql.connector.MySQLConnection(
            host='127.0.0.1',
            user="ziyu.li",
            password=credential["mysql"]["password"],
            port=tunnel.local_bind_port,
            database = database)
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall())

    return df

def db_connect_aws(query,database = 'segment-logs'):
    my_session = boto3.Session(
    aws_access_key_id= credential['aws']['aws_access_key_id'],
    aws_secret_access_key=credential['aws']['aws_secret_access_key'],
    region_name = credential['aws']['region_name']
    )

    import time
    import re
    import io

    params = {
    'region': credential['aws']['region_name'],
    'database': database,
    'bucket': 'h10-segment',
    'path': 'athen-query',
    'query': query

    client = my_session.client('athena', region_name=params["region"])

    def athena_query(client, params):
        response = client.start_query_execution(
            QueryString=params["query"],
            QueryExecutionContext={
            'Database': params['database']
            },
            ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
            }
        )
        return response

    result = athena_query(my_session.client('athena', region_name=params["region"]), params)
    client = my_session.client('athena', region_name=params["region"])
    response = client.get_query_execution(QueryExecutionId = result['QueryExecutionId'])

    def s3_to_pandas(session, params, s3_filename):
        s3client = session.client('s3')
        obj = s3client.get_object(Bucket=params['bucket'],
                                  Key=params['path'] + '/' + s3_filename)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df

    time.sleep(20)

    dataframe = s3_to_pandas(my_session,params,result['QueryExecutionId']+'.csv')

    return dataframe
