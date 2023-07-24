from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, LongType
def format_func(modified_list):
    for i in range(1, len(modified_list), 1):
        for j in range(13):
            # format room from string to float
            if j == 3:
                modified_list[i][j] = float(str(modified_list[i][j]))
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
    return modified_list
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

csv_list = ['lianjia-双流.csv', 'lianjia-大邑.csv', 'lianjia-崇州.csv', 'lianjia-彭州.csv', 'lianjia-成华.csv',
            'lianjia-新津.csv', 'lianjia-新都.csv', 'lianjia-武侯.csv', 'lianjia-浦江.csv', 'lianjia-温江.csv',
            'lianjia-简阳.csv', 'lianjia-郫都.csv', 'lianjia-都江堰.csv','lianjia-金堂.csv', 'lianjia-金牛.csv',
            'lianjia-锦江.csv', 'lianjia-青白江.csv', 'lianjia-青羊.csv', 'lianjia-龙泉驿.csv']
df_list = list()
spark = SparkSession.builder.appName("data_analysis").master("local").getOrCreate()
sc = spark.sparkContext

for each_csv in csv_list:
    file_rdd = sc.textFile('hdfs://node-1:9000/user/root/spark/' + each_csv )

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
                if not any(char.isdigit() for char in each_item_List[count_2]):
                    write_info = False
                    continue
                temp = str(each_item_List[count_2]).split('年')[0]
                each_item_List[count_2] = int(temp)
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

    #print(type(modified_list[1][3]))
    modified_list = format_func(modified_list)
    #print(type(modified_list[1][3]))
    modified_list = modified_list[1 : len(modified_list)]
    rdd = sc.parallelize(modified_list)

    df = rdd.toDF(schema)

    print(each_csv + '去重前', df.count())
    df = df.dropDuplicates()
    print('去重后', df.count())
    df_list.append(df)

# for i in df_list:
#     print(i.show())

andf_count_list = list()
i = 0
for each_df in df_list:
    andf = each_df.describe('room_price')
    andf_count_list.append([csv_list[i].split('-')[1].split('.')[0],
                            andf.select('room_price').rdd.map(lambda x:x).collect()[1]['room_price']])
    i += 1
print(andf_count_list)

for i in range(len(andf_count_list)):
    andf_count_list[i][1] = float(andf_count_list[i][1])

rdd = sc.parallelize(andf_count_list)
df = rdd.toDF(StructType([StructField("address", StringType(), False),
                          StructField("mean_room_price", DoubleType(), False)]
                         )
              )
print(df.show())

df.write.mode("overwrite").\
     format("jdbc").\
     option("url","jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true").\
     option("dbtable","address_mean").\
     option("user","spark").\
     option("password","12345678").\
     save()