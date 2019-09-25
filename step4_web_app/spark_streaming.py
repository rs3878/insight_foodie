from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils as kfk

import decouple as dc
import os
import sys



## FUNCTION DEFINITIONS
def stdin(sys_argv):
    """
    Imports Kafka & Cassandra parameters.
    """
    # Sets sensitive variables from ENV file
    try:
        path_home = os.getcwd()
        os.chdir(r"../../util/settings")
        settings = dc.Config(dc.RepositoryEnv(".env"))
        os.chdir(path_home)
    except:
        raise OSError("Cannot import ENV settings. Check path for ENV.")

    # Imports terminal input for simulation & Kafka settings
    try:
        p = {}
        p["spark_name"]= settings.get("SPARK_NAME")
        p["cassandra"] = settings.get("CASSANDRA_MASTER", cast=dc.Csv())
        p["cassandra_key"] = settings.get("CASSANDRA_KEYSPACE")
        p["kafka_brokers"] = settings.get("KAFKA_BROKERS")
        p["kafka_topic"] = settings.get("KAFKA_TOPIC", cast=dc.Csv())
    except:
        raise ValueError("Cannot interpret external settings. Check ENV file.")

    return p


from pyspark.ml.feature import BucketedRandomProjectionLSH

brp = BucketedRandomProjectionLSH(
    inputCol="features", outputCol="hashes", seed=12345, bucketLength=1.0
)
model = brp.fit(df)
model.approxSimilarityJoin(df, df, 3.0, distCol="EuclideanDistance")

one_row = df.where(df.id == 1).first().features
model.approxNearestNeighbors(df2, one_row, df.count()).collect()

from pyspark.sql import functions as F

to_dense_vector = F.udf(Vectors.dense, VectorUDF())
df = df.withColumn('features', to_dense_vector('features'))