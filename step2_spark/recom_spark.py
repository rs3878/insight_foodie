from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.context import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.ml.clustering import KMeans
import pandas as pd
import boto3

url ='postgresql://10.0.0.14:5432/'
properties = {'user':'postgres',
                'password':'19980126',
                'driver':'org.postgresql.Driver'}

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
num_iter,param = 100,0.2
als = ALS(maxIter=num_iter, regParam=param, userCol="user_int_id", #implicitPrefs=True,
              itemCol="bus_id", ratingCol="review_stars", coldStartStrategy="drop")
model = als.fit(ratings)

user_feature = model.userFactors
business_feature = model.itemFactors

k = 10
kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(user_feature)
transformed = model.transform(user_feature).select('id', 'prediction')
rows = transformed.collect()
df = spark.createDataFrame(rows)


df.write.jdbc(url='jdbc:%s' % url+'yelp',
        table='user_type', mode='overwrite',  properties=properties)


k = 10
kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(business_feature)
transformed = model.transform(business_feature).select('id', 'prediction')
rows = transformed.collect()
df = spark.createDataFrame(rows)

df.write.jdbc(url='jdbc:%s' % url+'yelp',
        table='business_type', mode='overwrite',  properties=properties)
