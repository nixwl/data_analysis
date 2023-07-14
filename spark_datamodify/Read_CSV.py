from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.\
    appName("read_csv").\
    getOrCreate()

df = spark.read.format('csv')\
    .option("sep", ",")\
    .option("header", "True")\
    .option("encoding", "utf-8")\
    .schema("userId STRING, movieId STRING, rating DOUBLE, tstamp TIMESTAMP" )\
    .load('hdfs://node-1:9000/user/root/testfile/ratings.csv')


df.printSchema()
df.show()

