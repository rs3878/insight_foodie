import os
import boto3
import pandas as pd
import sys
from io import StringIO
from sqlalchemy import create_engine
import configparser
config = configparser.ConfigParser()
config.read('../db_properties.ini')

client = boto3.client('s3',
        aws_access_key_id=config['aws']['ACCESS_KEY'],
    aws_secret_access_key=config['aws']['SECRET_KEY'])


csv_obj = client.get_object(Bucket='yelpdatacf',Key='yelp_public_user_location.csv')
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')
df = pd.read_csv(StringIO(csv_string))#,index_col=0)
#df = df.reset_index().rename(columns={'index':'bus_id'})

engine = create_engine('postgresql://postgres:postgres@localhost:5432/yelp')
df.to_sql('user_location', engine, index=False, if_exists='replace')