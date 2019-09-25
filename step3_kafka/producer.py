import pandas as pd
import numpy as np
from random import randrange
import datetime
from kafka import KafkaProducer
import pandas.io.sql as sqlio
import psycopg2


conn = psycopg2.connect("dbname=yelp user=postgres password=postgres")
business = sqlio.read_sql_query("""SELECT * FROM clean_business""", conn)
user = sqlio.read_sql_query("""SELECT * FROM clean_user""", conn)

n_business = len(business)
n_users = len(user)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def gen_one(i):
    rdm_restaurant = np.random.randint(low = 0, high = n_business, size = 1)
    restaurant = business['business_ids'].iloc[rdm_restaurant]

    rdm_customer = np.random.randint(low = 0, high = n_users, size = 1)
    customer = user['user_ids'].iloc[rdm_customer]

    timestp = datetime.datetime(2019, 1, 1, 13, 00)
    rdm_time = timestp+datetime.timedelta(seconds=i)

    output = (restaurant,customer,rdm_time)

    producer.send('yelp', bytes(str(output), 'utf-8'))
    producer.flush()

for i in range(1000000):
    gen_one(i)
