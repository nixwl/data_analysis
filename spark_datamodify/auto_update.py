from pyspark.sql import SparkSession
import time


'''
:@ function: 配件启动函数
'''
def conf_init():
     spark = SparkSession.builder.appName("data_analysis").master("local").getOrCreate()
     sc = spark.sparkContext
     return spark, sc

def load_base_data(spark):
     df2 = spark.read.format("jdbc"). \
          option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
          option("dbtable", "base_data"). \
          option("user", "spark"). \
          option("password", "12345678"). \
          load()
     return df2

def update_other_table_schema(df2):
     ### 其他表：from base_data
     AaP = df2.select("地址", "平方价格", '总价格')  # table_name：Aap_data
     TaP = df2.select("房型", "平方价格", '总价格')  # table_name：TaP_data
     RaP = df2.select("面积", "平方价格", '总价格')  # table_name：RaP_data
     MaP = df2.select("装修", "平方价格", '总价格')  # table_name：MaP_data
     LaP = df2.select("楼层", "平方价格", '总价格')  # table_name：LaP_data
     BTaP = df2.select("建成时间", "平方价格", '总价格')  # table_name：BTaP_data
     CaPB = df2.select("关注度", "朝向")  # table_name：CaPB_data
     CaT = df2.select("关注度", '房型')  # table_name：CaT_data
     CaP = df2.select("关注度", "平方价格", '总价格')  # table_name：PaC_data
     return [AaP, TaP, RaP, MaP, LaP, BTaP, CaPB, CaT, CaP]


def write_tableTosql(df):
     df.write.mode("overwrite"). \
          format("jdbc"). \
          option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
          option("dbtable", df + "_data"). \
          option("user", "spark"). \
          option("password", "12345678"). \
          save()
     
def __main__():
     spark, sc = conf_init()
     df2 = load_base_data(spark)
     # 查看 base_data 的表结构
     df2.printSchema()
     df2.show()
     table_list = update_other_table_schema(df2)
     # 查看其他表数据
     for i in table_list:
          print(i.show())
     # 表存入数据库：
     for i in table_list:
          write_tableTosql(i)
     spark.stop()


# ### write to mysql
# AaP.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","AaP_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
#
# TaP.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","TaP_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
#
# RaP.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","RaP_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
#
# MaP.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","MaP_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
#
# LaP.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","LaP_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
#
# BTaP.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","BTaP_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
#
# CaPB.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","CaPB_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
#
#
# CaT.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","CaT_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
#
# CaP.write.mode("overwrite").\
#      format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","CaP_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      save()
# spark.stop()