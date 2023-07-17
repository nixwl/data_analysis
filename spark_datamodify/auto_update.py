from pyspark.sql import SparkSession
import time
import pymysql

from pyspark.sql.functions import monotonically_increasing_id

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
     AaP = df2.select("id", "address", "room_price", 'total_price')  # table_name：Aap_data
     TaP = df2.select("id", "type", "room_price", 'total_price')  # table_name：TaP_data
     RaP = df2.select("id","room", "room_price", 'total_price')  # table_name：RaP_data
     MaP = df2.select("id","what_fix", "room_price", 'total_price')  # table_name：MaP_data
     LaP = df2.select("id","level", "room_price", 'total_price')  # table_name：LaP_data
     BTaP = df2.select("id","built_time", "room_price", 'total_price')  # table_name：BTaP_data
     CaPB = df2.select("id","concern_rate", "orient")  # table_name：CaPB_data
     CaT = df2.select("id","concern_rate", 'type')  # table_name：CaT_data
     CaP = df2.select("id", "concern_rate", "room_price", 'total_price')  # table_name：PaC_data

     return [AaP, TaP, RaP, MaP, LaP, BTaP, CaPB, CaT, CaP]


def write_tableTosql(df, name):
     df.write.mode("overwrite"). \
          format("jdbc"). \
          option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
          option("dbtable",  name + "_data"). \
          option("user", "spark"). \
          option("password", "12345678"). \
          save()

def dropDuplicates(df):
     df.dropDuplicates()

def execsql():
     db = pymysql.connect(host="192.168.101.20", user="spark",
                          password="12345678", database="spark",
                          charset="utf8")
     # 2.利用db方法创建游标对象
     cur = db.cursor()
     # sql 语句定义：
     PK_AaP = "ALTER TABLE AaP_data ADD PRIMARY KEY(id);"
     PK_TaP = "ALTER TABLE TaP_data ADD PRIMARY KEY(id);"
     PK_RaP = "ALTER TABLE RaP_data ADD PRIMARY KEY(id);"
     PK_MaP = "ALTER TABLE MaP_data ADD PRIMARY KEY(id);"
     PK_LaP = "ALTER TABLE LaP_data ADD PRIMARY KEY(id);"
     PK_BTaP = "ALTER TABLE BTaP_data ADD PRIMARY KEY(id);"
     PK_CaPB = "ALTER TABLE CaPB_data ADD PRIMARY KEY(id);"
     PK_CaT = "ALTER TABLE CaT_data ADD PRIMARY KEY(id);"
     PK_CaP = "ALTER TABLE CaP_data ADD PRIMARY KEY(id);"

     FK_AaP_data_base_data = "alter table spark.AaP_data add constraint AaP_data_base_data_id_fk foreign key (id) references spark.base_data (id) on update cascade;"

     FK_TaP_data_base_data = "alter table spark.TaP_data add constraint TaP_data_base_data_id_fk foreign key (id) references spark.base_data (id) on update cascade;"

     FK_RaP_data_base_data = "alter table spark.RaP_data add constraint RaP_data_base_data_id_fk foreign key (id) references spark.base_data (id) on update cascade;"

     FK_Map_data_base_data = "alter table spark.MaP_data add constraint MaP_data_base_data_id_fk foreign key (id) references spark.base_data (id) on update cascade;"

     FK_Lap_data_base_data = "alter table spark.LaP_data add constraint LaP_data_base_data_id_fk foreign key (id) references spark.base_data (id) on update cascade;"

     FK_BTap_data_base_data = "alter table spark.BTaP_data add constraint BTaP_data_base_data_id_fk foreign key (id) references spark.base_data (id) on update cascade;"

     FK_CaPB_data_base_data = "alter table spark.CaPB_data add constraint CaPB_data_base_data_id_fk foreign key (id) references spark.base_data (id) on update cascade;"

     FK_CaT_data_base_data = "alter table spark.CaT_data add constraint CaT_data_base_data_id_fk foreign key (id) references spark.base_data (id) on update cascade;"

     FK_CaP_data_base_data = "alter table spark.CaP_data add constraint CaP_data_base_data_id_fk foreign key (id) references spark.base_data (id) on update cascade;"

     PK_ExecList = [PK_AaP, PK_TaP, PK_RaP, PK_MaP, PK_LaP, PK_BTaP, PK_CaPB, PK_CaT, PK_CaP]
     FK_ExecList = [FK_AaP_data_base_data, FK_TaP_data_base_data, FK_RaP_data_base_data, FK_Map_data_base_data, FK_Lap_data_base_data,
                    FK_BTap_data_base_data, FK_CaPB_data_base_data, FK_CaT_data_base_data, FK_CaP_data_base_data]
     # 3. 执行游标
     for PK in PK_ExecList:
          cur.execute(PK)
     db.commit()

     for FK in FK_ExecList:
          cur.execute(FK)
     db.commit()

     # 查看游标执行结果
     # for line in cur.fetchall():
     #      print(line)
     cur.close()
     db.close()


def __main__():
     spark, sc = conf_init()
     df2 = load_base_data(spark)
     # 查看 base_data 的表结构
     df2.printSchema()
     df2.show()
     table_list = update_other_table_schema(df2)
     # 查看其他表数据
     for i in table_list:
          dropDuplicates(i)
          print(i.show())
     name_list = ['AaP', 'TaP', 'RaP', 'MaP', 'LaP', 'BTaP', 'CaPB', 'CaT', 'CaP']
     # 表存入数据库：
     for i in range(len(table_list)):
          write_tableTosql(table_list[i], name_list[i])
     spark.stop()
     # 数据库表结构更新
     execsql()

__main__()
# execsql()


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