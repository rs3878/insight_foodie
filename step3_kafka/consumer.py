from kafka import KafkaConsumer
import time
import psycopg2
from datetime import datetime
import json
import yaml
import numpy as np

consumer = KafkaConsumer('yelp', bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest')

conn = psycopg2.connect("dbname=yelp user=postgres password=postgres")
cursor = conn.cursor()

postgres_insert_query = """INSERT INTO yelp.public.consumer (business,user_id,time) VALUES (%s,%s,%s)"""


max_waiting_time= 5000 #10000 ms, 10s
for message in consumer:
    try:
        msg = message.value
        msg = msg.decode('utf-8')
        msg = yaml.load(msg)

        record_to_insert = (
            msg['business'],msg['user'], datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S')
        )
        print(record_to_insert)

        cursor.execute(postgres_insert_query, record_to_insert)
        conn.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into consumer table")

    except (Exception, psycopg2.Error) as error:
        if (conn):
            print("Failed to insert record into consumer table", error)