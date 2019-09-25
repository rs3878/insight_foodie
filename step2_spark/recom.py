import pyspark.sql.functions as sql_func
from pyspark.sql.types import *
from pyspark.mllib.recommendation import MatrixFactorizationModel, Rating #, ALS
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import Row
import pandas.io.sql as sqlio
import psycopg2

from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.context import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.clustering import KMeans
import pandas as pd
import numpy as np
import boto3
from io import StringIO

def _map_to_pandas(rdds):
    """ Needs to be here due to pickling issues """
    return [pd.DataFrame(list(rdds))]

def to_Pandas(df, n_partitions=None):
    """
    Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
    repartitioned if `n_partitions` is passed.
    :param df:              pyspark.sql.DataFrame
    :param n_partitions:    int or None
    :return:                pandas.DataFrame
    """
    if n_partitions is not None: df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand

sc = SparkContext("spark://ip-10-0-0-13:7077", "yelp")
spark = SparkSession(sc)

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# read data
client = boto3.client('s3')
obj = client.get_object(Bucket='yelpdatacf', Key='yelp_public_cf.csv')
df = pd.read_csv(obj['Body'])

df.review_stars = (df.review_stars - df.review_stars.mean())
ratings = spark.createDataFrame(df)

# use the model that has min RMSE
num_iter,param = 50,0.2
als = ALS(maxIter=num_iter, regParam=param, userCol="user_int_id", #implicitPrefs=True,
              itemCol="bus_id", ratingCol="review_stars", coldStartStrategy="drop")
model = als.fit(ratings)

# generate recommendations
userRecs = model.recommendForAllUsers(10)
userRecs_df = to_Pandas(userRecs)

user_int_id = []
bus_id = []
rating = []
for index, row in userRecs_df.iterrows():
    for x in row['recommendations']:
        user_int_id.append(row['user_int_id'])
        bus_id.append(x[0])
        rating.append(x[1])
df = pd.DataFrame.from_dict({'user_int_id':user_int_id,'bus_id':bus_id,'rating':rating})

csv_buffer = StringIO()
df.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object('yelpdatacf','recom_restaurants.csv').put(Body=csv_buffer.getvalue())

user_feature = model.userFactors
business_feature = model.itemFactors

k = 10
kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(user_feature)
transformed = model.transform(user_feature).select('id', 'prediction')
rows = transformed.collect()
df_pred = spark.createDataFrame(rows)

csv_buffer = StringIO()
df_pred.toPandas().to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object('yelpdatacf','user_type.csv').put(Body=csv_buffer.getvalue())

k = 10
kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(business_feature)
transformed = model.transform(business_feature).select('id', 'prediction')
rows = transformed.collect()
df_pred = spark.createDataFrame(rows)

csv_buffer = StringIO()
df_pred.toPandas().to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object('yelpdatacf','business_type.csv').put(Body=csv_buffer.getvalue())

