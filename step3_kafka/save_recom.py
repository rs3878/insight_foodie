from io import StringIO
from sqlalchemy import create_engine
import pandas.io.sql as sqlio
import psycopg2
import pandas as pd
import numpy as np
import boto3


import configparser
config = configparser.ConfigParser()
config.read('../db_properties.ini')

client = boto3.client('s3',
        aws_access_key_id=config['aws']['ACCESS_KEY'],
    aws_secret_access_key=config['aws']['SECRET_KEY'])

csv_obj = client.get_object(Bucket='yelpdatacf',Key='recom_restaurants_919.csv')
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')
df = pd.read_csv(StringIO(csv_string))#,index_col=0)
df = df.reset_index()

engine = create_engine('postgresql://postgres:postgres@localhost:5432/yelp')

params = {
  'database': 'yelp',
  'user': 'postgres',
  'password': '19980126',
  'host': '10.0.0.14',
  'port': '5432'
}

# conn = psycopg2.connect("dbname=yelp user=postgres password=postgres")
conn = psycopg2.connect(**params)
cursor = conn.cursor()
q = "drop table if exists recom;"

cursor.execute(q)
conn.commit()
q = "create table recom( \
        bus_id integer,\
        rating real,\
        user_int_id integer);"
cursor.execute(q)
conn.commit()

for line in df.iterrows():
    x = line[1]['bus_id'],line[1]['rating'],line[1]['user_int_id']
    q = """insert into recom(bus_id,rating,user_int_id) values({},{},{});"""
    cursor.execute(q.format(x[0],x[1],x[2]))
    conn.commit()