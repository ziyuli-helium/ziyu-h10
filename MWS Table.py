import boto3
import pandas as pd
import psycopg2

param_dic_h10 = {
    "host" : "helium10-db-read-replica-for-ds.c46yzjdby65l.us-east-1.rds.amazonaws.com",
    "user" : "h10_ziyu_li",
    "database" : "helium10",
    "password": "zDfF0dP*****C5sJ8P"
}
conn_h10 = psycopg2.connect(**param_dic_h10)

cursor_h10 = conn_h10.cursor()

def convert_dataframe(fetch,column_names):
    df = pd.DataFrame(fetch,columns = column_names)
    return df

cursor_h10.execute("""SELECT sum(case when "authToken" is not null then 1 else 0 end) as total_mws_token,
       sum(case when region = 'EU' then 1 else 0 end) as eu_token,
       sum(case when region = 'NA' then 1 else 0 end) as na_token,
       sum(case when region = 'FE' then 1 else 0 end) as fe_token
FROM "MwsAuthTokenOfClient"
WHERE status = true
""")
# MWS Active Token number

MWS_active = convert_dataframe(cursor_h10.fetchall(),["total_active_mws_tokens","active_eu_tokens","active_na_tokens","active_fe_tokens"])

cursor_h10.execute("""SELECT sum(case when "client_id" is not null then 1 else 0 end) as users_with_tokens,
       sum(case when region = 'EU' then 1 else 0 end) as users_with_eu_token,
       sum(case when region = 'NA' then 1 else 0 end) as users_with_na_token,
       sum(case when region = 'FE' then 1 else 0 end) as users_with_fe_token,
       CAST(date_part('year',date(current_date AT TIME ZONE 'America/Los_Angeles')) AS BIGINT) as year,
       CAST(date_part('month',date(current_date AT TIME ZONE 'America/Los_Angeles')) AS BIGINT) as month,
       date(current_date AT TIME ZONE 'America/Los_Angeles') as "date"
FROM (
SELECT DISTINCT client_id,region
FROM "MwsAuthTokenOfClient"
WHERE status = true) tb""")


MWS_active_user = convert_dataframe(cursor_h10.fetchall(),["users_with_active_tokens","users_with_active_eu_tokens","users_with_active_na_tokens","users_with_active_fe_tokens","year","month","date"])

active_mws_tokens = pd.concat([MWS_active,MWS_active_user],axis = 1)

active_mws_tokens = active_mws_tokens[["total_active_mws_tokens","users_with_active_tokens","active_eu_tokens","users_with_active_eu_tokens","active_fe_tokens","users_with_active_fe_tokens","active_na_tokens","users_with_active_na_tokens","year","month","date"]]

cursor_h10.execute("""SELECT sum(case when "authToken" is not null then 1 else 0 end) as total_mws_token,
       sum(case when region = 'EU' then 1 else 0 end) as eu_token,
       sum(case when region = 'NA' then 1 else 0 end) as na_token,
       sum(case when region = 'FE' then 1 else 0 end) as fe_token
FROM "MwsAuthTokenOfClient"
WHERE status = false
""")
# MWS Inactive Token number

MWS_inactive = convert_dataframe(cursor_h10.fetchall(),["total_inactive_mws_tokens","inactive_eu_tokens","inactive_na_tokens","inactive_fe_tokens"])

cursor_h10.execute("""SELECT sum(case when "client_id" is not null then 1 else 0 end) as users_with_tokens,
       sum(case when region = 'EU' then 1 else 0 end) as users_with_eu_token,
       sum(case when region = 'NA' then 1 else 0 end) as users_with_na_token,
       sum(case when region = 'FE' then 1 else 0 end) as users_with_fe_token,
       CAST(date_part('year',date(current_date AT TIME ZONE 'America/Los_Angeles')) AS BIGINT) as year,
       CAST(date_part('month',date(current_date AT TIME ZONE 'America/Los_Angeles')) AS BIGINT) as month,
       date(current_date AT TIME ZONE 'America/Los_Angeles') as "date"
FROM (
SELECT DISTINCT client_id,region
FROM "MwsAuthTokenOfClient"
WHERE status = false) tb""")

MWS_inactive_user = convert_dataframe(cursor_h10.fetchall(),["users_with_inactive_tokens","users_with_inactive_eu_tokens","users_with_inactive_na_tokens","users_with_inactive_fe_tokens","year","month","date"])

inactive_mws_tokens = pd.concat([MWS_inactive,MWS_inactive_user],axis = 1)


inactive_mws_tokens = inactive_mws_tokens[["total_inactive_mws_tokens","users_with_inactive_tokens","inactive_eu_tokens","users_with_inactive_eu_tokens","inactive_fe_tokens","users_with_inactive_fe_tokens","inactive_na_tokens","users_with_inactive_na_tokens","year","month","date"]]

import awswrangler as wr
import boto3
my_session = boto3.Session(
aws_access_key_id= 'AKIA4L5BLC6*****GEAT',
    aws_secret_access_key='TGAd7lizvTT3HIPJDQjAA******SUil',
    region_name = 'us-east-1'
)

column_type_active = {"total_active_mws_tokens":"bigint","users_with_active_tokens":"bigint","active_eu_tokens":"bigint","users_with_active_eu_tokens":"bigint","active_fe_tokens":"bigint","users_with_active_fe_tokens":"bigint","active_na_tokens":"bigint","users_with_active_na_tokens":"bigint","year":"bigint","month":"bigint","date":"date"}

wr.s3.to_parquet(
    df=active_mws_tokens,
    path="s3://h10-tableau/active_mws_tokens/",
    dataset=True,
    index = False,
    boto3_session = my_session,
    mode="overwrite_partitions",        # Could be append, overwrite or overwrite_partitions
    database="segment-logs",  # Optional, only with you want it available on Athena/Glue Catalog
    table="active_mws_tokens",
    dtype= column_type_active,
    compression="snappy",
    partition_cols=["year","month","date"])

column_type_inactive = {"total_inactive_mws_tokens":"bigint","users_with_inactive_tokens":"bigint","inactive_eu_tokens":"bigint","users_with_inactive_eu_tokens":"bigint","inactive_fe_tokens":"bigint","users_with_inactive_fe_tokens":"bigint","inactive_na_tokens":"bigint","users_with_inactive_na_tokens":"bigint","year":"bigint","month":"bigint","date":"date"}

wr.s3.to_parquet(
    df=inactive_mws_tokens,
    path="s3://h10-tableau/inactive_mws_tokens/",
    dataset=True,
    index = False,
    boto3_session = my_session,
    mode="overwrite_partitions",        # Could be append, overwrite or overwrite_partitions
    database="segment-logs",  # Optional, only with you want it available on Athena/Glue Catalog
    table="inactive_mws_tokens",
    dtype= column_type_inactive,
    compression="snappy",
    partition_cols=["year","month","date"])
