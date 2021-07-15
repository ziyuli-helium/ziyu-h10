import boto3
import pandas as pd
import psycopg2
import mysql.connector
import sshtunnel
import json

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
