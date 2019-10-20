from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.context import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.ml.clustering import GaussianMixture
import pandas as pd
import boto3

url ='postgresql://10.0.0.14:5432/'
properties = {'user':'postgres',
                'password':'postgres',
                'driver':'org.postgresql.Driver'}

sc = SparkContext("spark://ip-10-0-0-13:7077", "yelp")
spark = SparkSession(sc)

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# read data
client = boto3.client('s3')
obj = client.get_object(Bucket='yelpdatacf', Key='book_cf.csv')
df = pd.read_csv(obj['Body'])

df.rating = (df.rating - df.rating.mean())
ratings = spark.createDataFrame(df)

# use the model that has min RMSE
num_iter,param = 200,0.2
als = ALS(maxIter=num_iter, regParam=param, userCol="user_id", itemCol="book_id", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(ratings)

user_feature = model.userFactors
book_feature = model.itemFactors

k = 20
gmm = GaussianMixture().setK(k).setSeed(1).setFeaturesCol("features")
model = gmm.fit(user_feature)
transformed = model.transform(user_feature).select('id', 'prediction')
rows = transformed.collect()
df = spark.createDataFrame(rows)


df.write.jdbc(url='jdbc:%s' % url+'yelp',
        table='book_gm_user_feature20', mode='overwrite',  properties=properties)


k = 20
gmm = GaussianMixture().setK(k).setSeed(1).setFeaturesCol("features")
model = gmm.fit(book_feature)
transformed = model.transform(book_feature).select('id', 'prediction')
rows = transformed.collect()
df = spark.createDataFrame(rows)

df.write.jdbc(url='jdbc:%s' % url+'yelp',
        table='book_gm_feature20', mode='overwrite',  properties=properties)