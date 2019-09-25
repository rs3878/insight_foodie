from io import StringIO
from sqlalchemy import create_engine
import pandas.io.sql as sqlio
import psycopg2
import pandas as pd
import numpy as np
import boto3

engine = create_engine('postgresql://postgres:postgres@localhost:5432/yelp')
conn = psycopg2.connect("dbname=yelp user=postgres password=postgres")
cursor = conn.cursor()
q = """drop table if exists kmeans_users;"""
cursor.execute(q)
conn.commit()

q = """create table kmeans_users as ( \
    select i.prediction , u.user_name, u.user_ids, l.user_city \
    from user_type i \
    inner join clean_user u \
    on i.id = u.user_int_id \
    inner join user_location l \
    on u.user_ids=l.user_ids \
    );"""
cursor.execute(q)
conn.commit()

