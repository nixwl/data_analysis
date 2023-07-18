from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, LongType

'''
@: format:
['名称', '地址', '房型', '面积', '朝向', '装修', '楼层', '建成时间', '楼层结构', '总价格', '平方价格', '关注度', '发布时间']
['优客联邦一期', '丽都', '2室1厅', '79.17', '西南', '精装', '低楼层(共16层)', '2010', '板楼', '1600000', '20210', '1', '4个月以前发布']
'''

'''
@:function: format_func
@:param: 
    modified_list 信息列表
@:author:

'''
def format_func(modified_list):
    for i in range(1, len(modified_list), 1):
        for j in range(len(modified_list)):
            # format room from string to float
            if j == 3:
                modified_list[i][j] = float(modified_list[i][j])
            # format built_date from string to int
            if j == 7:
                modified_list[i][j] = int(modified_list[i][j])
            # format total_price from string to float
            if j == 9:
                modified_list[i][j] = float(modified_list[i][j])
            # format room_price from string to float (元/平)
            if j == 10:
                modified_list[i][j] = float(modified_list[i][j])
            # format Concern_rate from string to int
            if j == 11:
                modified_list[i][j] = int(modified_list[i][j])

spark = SparkSession.builder.appName("data_analysis").master("local").getOrCreate()
sc = spark.sparkContext

file_rdd = sc.textFile('hdfs://node-1:9000/user/root/testfile/lianjia.csv')


base_list = file_rdd.map(lambda x: x).collect()
modified_list = list()
label_list = ['名称','地址','房型','面积','朝向','装修','楼层','建成时间','楼层结构','总价格','平方价格','关注度','发布时间']
modified_list.append(label_list)

for count_1 in range(1, len(base_list), 1):
    each_item_List = str(base_list[count_1]).split(',')
    # print(each_item_List)
    # 处理每行的每列
    write_info = True
    for count_2 in range(len(each_item_List)):
        # print(each_item_List[count_2], end=' ')
        if count_2 == 0:
            temp = str(each_item_List[count_2]).split(' ')[0]
            each_item_List[count_2] = temp
            # print(each_item_List[count_2])

        if count_2 == 3:
            temp = str(each_item_List[count_2]).split('平')[0]
            each_item_List[count_2] = temp
            # print(each_item_List[count_2])

        if count_2 == 4:
            temp = str(each_item_List[count_2]).split(' ')[0]
            each_item_List[count_2] = temp
            # print(each_item_List[count_2])

        if count_2 == 6:
            if '楼层' not in each_item_List[count_2]:
                write_info = False
            # print(each_item_List[count_2])

        if count_2 == 7:
            temp = str(each_item_List[count_2]).split('年')[0]
            each_item_List[count_2] = int(temp)     # tue: 15.05 最后一次修改
            # print(each_item_List[count_2])

        if count_2 == 9:
            temp = float(str(each_item_List[count_2]).split('万')[0]) * 10000
            each_item_List[count_2] = str(int(temp))
            # print(each_item_List[count_2])

        if count_2 == 10:
            temp = each_item_List[count_2].split('"')[1] + \
                   each_item_List[count_2 + 1].split("元")[0]
            each_item_List[count_2] = temp
            each_item_List.pop(11)
            # print(each_item_List[count_2])
        if count_2 == 11:
            temp = each_item_List[count_2].split('人')[0]
            each_item_List[count_2] = temp
            # print(each_item_List[count_2])
        if count_2 == 12:
            temp = each_item_List[count_2]
            has_digit = any(char.isdigit() for char in temp)
            if not has_digit:
                write_info = False
    if write_info:
        modified_list.append(each_item_List)

# for i in modified_list:
#     print(i)

format_func(modified_list)

# print("after format:")
# for i in modified_list:
#     print(i)
modified_list = modified_list[1 : len(modified_list)]

# import numpy as np
# dtemp =np.array(modified_list).shape
# print(dtemp)

rdd = sc.parallelize(modified_list)
# print(rdd)
# print(rdd.map(lambda x:x).collect())

# rdd.saveAsTextFile("hdfs://node-1:9000/user/root/spark/base_data.csv")


schema = StructType([
        StructField("name", StringType(), False),#名称
        StructField("address", StringType(), False),#地址
        StructField("type", StringType(), False),#房型
        StructField("room", DoubleType(), False),#面积
        StructField("orient", StringType(), False),#朝向
        StructField("what_fix", StringType(), False),#装修
        StructField("level", StringType(), False),#楼层
        StructField("built_time", IntegerType(), False),#建成时间
        StructField("level_structure", StringType(), False),#楼层结构
        StructField("total_price", DoubleType(), False),#总价格
        StructField("room_price", DoubleType(), False),#平方价格
        StructField("concern_rate", IntegerType(), False),#关注度
        StructField("publish_time", StringType(), False)#发布时间
])
# StructField("name", StringType(), False),
# StructField("address", StringType(), False),
# StructField("type", StringType(), False),
# StructField("room", DoubleType(), False),
# StructField("toward", StringType(), False),
# StructField("renovation", StringType(), False),
# StructField("level", StringType(), False),
# StructField("built_time", StringType(), False),
# StructField("floor_structure", StringType(), False),
# StructField("total_price", DoubleType(), False),
# StructField("room_price", DoubleType(), False),
# StructField("concern_rate", IntegerType(), False),
# StructField("publish_time", StringType(), False)

# convert RDD -> DataFrame
# df = spark.createDataFrame(rdd, schema)
df = rdd.toDF(schema)
df = df.dropDuplicates()
# 设置虚拟主键自增策略
df = df.withColumn("id", monotonically_increasing_id())
# df.schema.add(StructField("编号", LongType(),nullable=False))
# df.rdd.zipWithUniqueId()
# df.printSchema()
# df.show()
# print(df.dropDuplicates().select("关注度", "平方价格", '总价格').orderBy(df["关注度"].desc()).show())


# ### write to mysql
df.write.mode("overwrite").\
     format("jdbc").\
     option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
     option("dbtable","base_data").\
     option("user","spark").\
     option("password","12345678").\
     save()

# ### read from mysql
# df2 = spark.read.format("jdbc").\
#      option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
#      option("dbtable","base_data").\
#      option("user","spark").\
#      option("password","12345678").\
#      load()
#
# df2.show()