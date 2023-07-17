from pyspark.sql import SparkSession
import time

'''
:@ function: 配件启动函数
'''
def conf_init():
     spark = SparkSession.builder.appName("data_analysis").master("local").getOrCreate()
     sc = spark.sparkContext
     return spark, sc


def load_base_data(spark, dbname):
    df2 = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
        option("dbtable", "base_data"). \
        option("user", "spark"). \
        option("password", "12345678"). \
        load()
    return df2

def __main__():
    spark, sc = conf_init()
    df2 = load_base_data(spark,)
