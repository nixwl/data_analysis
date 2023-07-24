from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.\
    appName("read_csv").\
    master("local").\
    getOrCreate()
    # master("spark://node-1:7077").\


df = spark.read.format('csv')\
    .option("sep", ",")\
    .option("header", "True")\
    .option("encoding", "utf-8")\
    .schema("userId STRING, movieId STRING, rating DOUBLE, tstamp TIMESTAMP" )\
    .load('hdfs://node-1:9000/user/root/testfile/ratings.csv')
df.show()

# 筛选 without groups (shorthand for df.groupBy().agg()).
# df.agg({"rating": "max"}).show()

# DataFrame 别名
# df_alias = df.alias("df_alias")
# df_alias.show()

# DataFrame‘s Column's approxQuantile 近似分位数
# list_ = df.approxQuantile('rating', (0.25, ), 0.01)
# print(list_)

# 持久化设定：pyspark.sql.DataFrame.Cache     pyspark.sql.DataFrame.checkpoint
# 重新划定分区：pyspark.sql.DataFrame.coalesce

# pyspark.sql.DataFrame.colRegex
# Selects column based on the column name specified as a regex and returns it as Column.

# Returns all the records as a list of Row
# df.collect()

# 输出表架构
# df.printSchema()
# 打印表
# df.show()
# df.count()
# df2 = df.limit(5)
# #d f2.count()
# df2.printSchema()
# df2.show()
# ### write to mysql
# df2.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","movie_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
#
#
# df3 = spark.read.format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","movie_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      load()
#
# df3.show()
