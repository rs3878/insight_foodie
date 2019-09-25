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

csv_obj = client.get_object(Bucket='yelpdatacf', Key='clean_user.csv')
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')
df = pd.read_csv(StringIO(csv_string))  # ,index_col=0)
df = df.reset_index().rename(columns={'index': 'user_int_id'})
df = df[['user_int_id', 'user_ids', 'user_name']]

engine = create_engine('postgresql://postgres:postgres@localhost:5432/yelp')

conn = psycopg2.connect("dbname=yelp user=postgres password=postgres")
cursor = conn.cursor()

q = "drop table if exists clean_user;"
cursor.execute(q)
conn.commit()
q = "create table clean_user( \
        user_int_id integer, \
        user_ids text, \
        user_name text);"
cursor.execute(q)
conn.commit()

for line in df.iterrows():
    x = line[1]['user_int_id'],line[1]['user_ids'],str(line[1]['user_name']).replace("'","")
    q = """insert into clean_user(user_int_id,user_ids,user_name) values({},'{}','{}');"""
    cursor.execute(q.format(x[0],x[1],x[2]))
    conn.commit()

