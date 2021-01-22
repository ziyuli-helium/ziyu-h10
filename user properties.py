
import boto3
import pandas as pd
import psycopg2
param_dic_h10 = {
    "host" : "helium10-db-read-replica-for-ds.c46yzjdby65l.us-east-1.rds.amazonaws.com",
    "user" : "h10_ziyu_li",
    "database" : "helium10",
    "password": "zDfF0dP4n******J8P"
}
conn_h10 = psycopg2.connect(**param_dic_h10)

cursor_h10 = conn_h10.cursor()

def convert_dataframe(fetch,column_names):
    df = pd.DataFrame(fetch,columns = column_names)
    return df

cursor_h10.execute("""SELECT client_id,"heliumPlanId" FROM "StripeSubscription" WHERE "heliumPlanId" not like '%Free%'""")
# paid user account

paid_user = convert_dataframe(cursor_h10.fetchall(),["client_id","heliumPlanId"])

param_dic_profit = {
    "host" : "helium10-profits-shard0-read-replica-for-ds.c46yzjdby65l.us-east-1.rds.amazonaws.com",
    "user" : "h10_ziyu_li",
    "database" : "helium10_dashboard",
    "password": "KDfF01********C5sJ8sT"
}

conn_profit = psycopg2.connect(**param_dic_profit)

cursor_profit = conn_profit.cursor()

# get user whose sales on recent month >0
cursor_profit.execute("""SELECT DISTINCT "Client_id","purchaseMonth" FROM "MonthlySales"
WHERE "purchaseMonth" = (SELECT MAX("purchaseMonth") FROM "MonthlySales") AND "grossSales" > 0
""")

sale_user = cursor_profit.fetchall()
sale_user_list = convert_dataframe(sale_user,["client_id","purchaseMonth"])
sale_user_list.head()# user who has sales on recent month

cursor_h10.execute("""SELECT user_id,"profitsStarted",s."heliumPlanId"
FROM "Client" as c
LEFT JOIN "StripeSubscription" as s on s.client_id = c.user_id
WHERE "profitsStarted" = true and "heliumPlanId" not like '%Free%'""") # profit enable and paid users

profit_enable_user = convert_dataframe(cursor_h10.fetchall(),["client_id","profitsStarted","heliumPlanId"])
profit_enable_user.head() #paid user who has profitStarted = true

# concat paid heliumPlanid to monthly sales
sale_plan = sale_user_list.merge(paid_user,on = "client_id",how = "left")
paid_user_sales = sale_plan[sale_plan['heliumPlanId'].notnull()]

paid_user_sales_profits = paid_user_sales.merge(profit_enable_user,on = "client_id",how = "left")
paid_user_sales_profits[paid_user_sales_profits["profitsStarted"].isnull()]

len(paid_user_sales_profits[paid_user_sales_profits["profitsStarted"].isnull()].index)

profit_started_rate = len(paid_user_sales_profits[paid_user_sales_profits["profitsStarted"].notnull()].index) / len(paid_user_sales_profits["profitsStarted"].index)

profit_enable_paid_user = paid_user_sales_profits[paid_user_sales_profits["profitsStarted"].notnull()]

cursor_h10.execute("""SELECT "Client_id",CASE WHEN "dailyReportNotification" + "weeklyReportNotification"+"monthlyReportNotification" = 0 THEN false else true end as notification_on
FROM "ProfitsSettingsOfClient" as ps""") #pull information of notification

notification = convert_dataframe(cursor_h10.fetchall(),["client_id","notification_on"])
notification #user with notification information

profit_enable_paid_user = profit_enable_paid_user.merge(notification,on = "client_id",how = "left")
summary_email_subscriptions = len(profit_enable_paid_user[profit_enable_paid_user["notification_on"] == True].index)/len(profit_enable_paid_user["client_id"].index)

param_dic_mws = {
    "host" : "helium10-mws-read-replica-for-ds.c46yzjdby65l.us-east-1.rds.amazonaws.com",
    "user" : "h10_ziyu_li",
    "database" : "dba",
    "password": "0eb35f8******efb74"
}
conn_mws = psycopg2.connect(**param_dic_mws)
cursor_mws = conn_mws.cursor()
mws_active = pd.DataFrame(columns = ["token_id","asin","cogs"])

with conn_mws.cursor() as cursor:
    cursor.itersize = 50
    query = """SELECT token_id,asin,
     (CASE WHEN "productCost" + "shippingCost" IS NULL THEN 0 ELSE "productCost" + "shippingCost" END) as cogs
     FROM "MwsListing"
     WHERE "isActive" = true"""
    cursor.execute(query)

    tb = pd.concat([mws_active,convert_dataframe(cursor.fetchall(),["token_id","asin","cogs"])],ignore_index = True)
tb # asin with token_id and cogs

cursor_h10.execute("""SELECT s.client_id,m.id as token_id
FROM "StripeSubscription" as s
LEFT JOIN "MwsAuthTokenOfClient" as m on s.client_id = m.client_id
LEFT JOIN "Client" as c on c.user_id = s.client_id
WHERE m.id IS NOT NULL and s."heliumPlanId" not like '%Free%'""") #paid account with MWS token

client_token = convert_dataframe(cursor_h10.fetchall(),["client_id","token_id"])

temp1 = client_token.merge(sale_user_list,on = "client_id",how = "left")
client_token_sales = temp1[temp1["purchaseMonth"].notnull()]
client_token_sales #paid user with mws token and has sales last month

mws_tb = tb[tb.token_id.isin(client_token_sales["token_id"])]

cogs_adoption = len(mws_tb[mws_tb["cogs"] > 0.0].index)/len(mws_tb.index)

cursor_h10.execute("""SELECT CAST(avg(keywords) AS float),CAST(date_part('year',date(current_date AT TIME ZONE 'America/Los_Angeles')) AS BIGINT) as year,
       CAST(date_part('month',date(current_date AT TIME ZONE 'America/Los_Angeles')) AS BIGINT) as month,
       date(current_date AT TIME ZONE 'America/Los_Angeles') as "date"
FROM (
SELECT p."Client_id",count(distinct o."KtPhrase_id") as keywords
FROM "KtProduct" as p
LEFT JOIN "KtOrder" as o on o."KtProduct_id" = p.id
GROUP BY 1) tb""")

keyword = convert_dataframe(cursor_h10.fetchall(),["avg_keyword","year","month","date"])

user_property_1 = [profit_started_rate,cogs_adoption,summary_email_subscriptions]

user_property_1 = pd.DataFrame([user_property_1],columns = ["profit_started_rate","cogs_adoption","summary_email_subscriptions"])

user_property = pd.concat([user_property_1,keyword],axis = 1)

import awswrangler as wr
import boto3
my_session = boto3.Session(
aws_access_key_id= 'AKIA4L5B*********',
    aws_secret_access_key='TGAd7lizvTT*******k***',
    region_name = 'us-east-1'
)

column_type = {"profit_started_rate":"double","cogs_adoption":"double","summary_email_subscriptions":"double","avg_keyword":"double","year":"bigint","month":"bigint","date":"date"}

wr.s3.to_parquet(
    df=user_property,
    path="s3://h10-tableau/user_property/",
    dataset=True,
    index = False,
    boto3_session = my_session,
    mode="overwrite_partitions",        # Could be append, overwrite or overwrite_partitions
    database="segment-logs",  # Optional, only with you want it available on Athena/Glue Catalog
    table="h10_user_property",
    dtype= column_type,
    compression="snappy",
    partition_cols=["year","month","date"])
