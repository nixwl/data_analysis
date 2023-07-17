from pyspark.sql import SparkSession
import time
import pyspark.sql.functions as func
import pymysql

'''
:@ function: 配件启动函数
'''
def conf_init():
     spark = SparkSession.builder.appName("data_analysis").master("local").getOrCreate()
     sc = spark.sparkContext
     return spark, sc


def load_base_data(spark, dbname):
    df2 = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true&characterEncoding=UTF-8"). \
        option("dbtable", dbname). \
        option("user", "spark"). \
        option("password", "12345678"). \
        load()
    return df2

def __main__():
    spark, sc = conf_init()
    df1 = load_base_data(spark, 'CaP_data')
    # df1.show()
    df1 = df1.filter("concern_rate > 0").orderBy(df1["concern_rate"].desc(), df1["room_price"].desc())
    # df1.show(1000)
    quantiles = [
         quantiles_1_10,
         quantiles_2_10,
         quantiles_3_10,
         quantiles_4_10,
         quantiles_5_10,
         quantiles_6_10,
         quantiles_7_10,
         quantiles_8_10,
         quantiles_9_10,
    ]= df1.approxQuantile("concern_rate", [0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875], 0.01)
    print(quantiles)


# ----------------------------------------------------------------------------------- #
__main__()
