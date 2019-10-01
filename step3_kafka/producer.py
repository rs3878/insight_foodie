import pandas as pd
import numpy as np
from kafka import KafkaProducer
import pandas.io.sql as sqlio
import psycopg2
import datetime

params = {
  'database': 'yelp',
  'user': 'postgres',
  'password': '19980126',
  'host': '10.0.0.14',
  'port': '5432'
}

conn = psycopg2.connect("dbname=yelp user=postgres password=postgres")
#conn = psycopg2.connect(**params)
user = sqlio.read_sql_query("""SELECT * FROM kmeans_users""", conn)

n_users = len(user)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def gen_one(i):
    rdm_customer = np.random.randint(low = 0, high = n_users, size = 1)
    customer = user['user_ids'].iloc[rdm_customer].values[0]
    city = user['user_city'].iloc[rdm_customer].values[0]
    prediction = user['prediction'].iloc[rdm_customer].values[0]
    user_name = user['user_name'].iloc[rdm_customer].values[0]

    timestp = 20190101130000
    rdm_time = datetime.datetime.now()

    output = {}
    output['city'] = city
    output['user'] = customer
    output['timestamp'] = str(rdm_time)
    output['prediction'] = prediction
    output['user_name'] = user_name

    producer.send('yelp3', bytes(str(output), 'utf-8'))

    producer.flush()

for i in range(1000000):
    gen_one(i)
