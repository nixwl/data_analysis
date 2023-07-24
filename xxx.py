from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.config("spark.sql.session.charset", "UTF-8").appName("data_analysis").master("local").getOrCreate()
sc = spark.sparkContext


df = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true&characterEncoding=UTF-8"). \
        option("dbtable", 'base_data'). \
        option("user", "spark"). \
        option("password", "12345678"). \
        load()

csv_list = ['lianjia-双流.csv', 'lianjia-大邑.csv', 'lianjia-崇州.csv', 'lianjia-彭州.csv', 'lianjia-成华.csv',
            'lianjia-新津.csv', 'lianjia-新都.csv', 'lianjia-武侯.csv', 'lianjia-浦江.csv', 'lianjia-温江.csv',
            'lianjia-简阳.csv', 'lianjia-郫都.csv', 'lianjia-都江堰.csv', 'lianjia-金堂.csv', 'lianjia-金牛.csv',
            'lianjia-锦江.csv', 'lianjia-青白江.csv', 'lianjia-青羊.csv', 'lianjia-龙泉驿.csv']
schema = StructType([
    StructField("name", StringType(), False),  # 名称
    StructField("address", StringType(), False),  # 地址
    StructField("type", StringType(), False),  # 房型
    StructField("room", DoubleType(), False),  # 面积
    StructField("orient", StringType(), False),  # 朝向
    StructField("what_fix", StringType(), False),  # 装修
    StructField("level", StringType(), False),  # 楼层
    StructField("built_time", IntegerType(), False),  # 建成时间
    StructField("level_structure", StringType(), False),  # 楼层结构
    StructField("total_price", DoubleType(), False),  # 总价格
    StructField("room_price", DoubleType(), False),  # 平方价格
    StructField("concern_rate", IntegerType(), False),  # 关注度
    StructField("publish_time", StringType(), False)  # 发布时间
])



def function1():
    begin = 1979
    end = 2024
    bili = list()
    df.createOrReplaceTempView("df_view")
    for year in range(begin, end, 5):
        sql_log = "select * from df_view where `built_time` < "+ str(year+5)  + " and " + " `built_time` " + " >= " + str(year)
        #print('总：', df.count())
        #print('year 分', spark.sql(sql_log).count())
        r = (float(spark.sql(sql_log).count()) / float(df.count()))*100
        bili.append([str(year)+'-'+str(year+5),spark.sql(sql_log).count(),r])

    print(bili)
    rdd = sc.parallelize(bili)
    df_to_mysql = rdd.toDF( StructType([StructField("built_time_partition", StringType(), False),
                            StructField("partition_count", IntegerType(), False),
                            StructField("rate", DoubleType(), False)])
                          )

    print(df_to_mysql.show())
    df_to_mysql.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
        option("dbtable", "built_time_analysis"). \
        option("user", "spark"). \
        option("password", "12345678"). \
        save()



def function2():
    from pyspark.sql import functions as F

    df.createOrReplaceTempView("df_view")
    sql_log = "SELECT what_fix, count(*) as count, count(*) as rate FROM `df_view` GROUP BY what_fix"
    df_ans = spark.sql(sql_log)
    df_ans = df_ans.withColumn('rate', (F.col('rate').cast('float') / F.lit(df.count())) * 100)
    print(df_ans.show())
    df_ans.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", "mod_analysis"). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

def function3():
    from pyspark.sql import functions as F
    df_new = df.withColumn('level', F.col('level').cast('string').substr(0, 3))
    df_new.createOrReplaceTempView('df_new_view')
    sql_log = "SELECT level, count(*) as count, count(*) as rate FROM `df_new_view` GROUP BY level"
    df_new = spark.sql(sql_log)
    df_new = df_new.withColumn('rate',  (F.col('rate').cast('float') / F.lit(df.count())) * 100 )
    print(df_new.show())
    df_new.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
        option("dbtable", "level_analysis"). \
        option("user", "spark"). \
        option("password", "12345678"). \
        save()


def function4():
    df.createOrReplaceTempView('df_view')
    sql_log = "SELECT type ,count(*) as count, SUM(concern_rate) as concern_rate_sum, SUM(concern_rate)/ count(*)  as factor " \
              "from df_view GROUP BY type"
    df_new = spark.sql(sql_log)
    print(df_new.show())
    df_new.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
        option("dbtable", "type_and_concern_analysis"). \
        option("user", "spark"). \
        option("password", "12345678"). \
        save()


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

def function5():
    from pyspark.sql import functions as F
    df_list = list()
    for each_csv in csv_list:
        file_rdd = sc.textFile('hdfs://node-1:9000/user/root/spark/' + each_csv)

        base_list = file_rdd.map(lambda x: x).collect()
        modified_list = list()
        label_list = ['名称', '地址', '房型', '面积', '朝向', '装修', '楼层', '建成时间', '楼层结构', '总价格',
                      '平方价格', '关注度', '发布时间']
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

        # print(type(modified_list[1][3]))
        modified_list = format_func(modified_list)
        # print(type(modified_list[1][3]))
        modified_list = modified_list[1: len(modified_list)]
        rdd = sc.parallelize(modified_list)

        df_new = rdd.toDF(schema)

        print(each_csv + '去重前', df_new.count())
        df_new = df_new.dropDuplicates()
        print('去重后', df_new.count())
        df_list.append(df_new)

    index = 0
    for each_df in df_list:
        each_df_select = each_df.select('what_fix', 'room_price')
        each_df_select_mean = each_df_select.groupBy('what_fix').avg().withColumnRenamed('avg(room_price)', 'mean_room_price')
        each_df_select_max = each_df_select.groupBy('what_fix').max().withColumnRenamed('max(room_price)', 'max_room_price')
        each_df_select_min = each_df_select.groupBy('what_fix').min().withColumnRenamed('min(room_price)', 'min_room_price')

        each_df_select_mean = each_df_select_mean.withColumn('mean_room_price', F.round(F.col('mean_room_price'), 2))
        each_df_select_max = each_df_select_max.withColumn('max_room_price', F.round(F.col('max_room_price'), 2))
        each_df_select_min = each_df_select_min.withColumn('min_room_price', F.round(F.col('min_room_price'), 2))

        each_df_select = each_df_select_max.join(each_df_select_mean, ['what_fix'] ,'inner')
        each_df_select = each_df_select.join(each_df_select_min, ['what_fix'], 'inner')

        df_list[index] = each_df_select
        index += 1
        print(each_df_select.show())

    index = 0
    for df_ in df_list:
        # print(df_.show())
        df_.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", csv_list[index].split('-')[1].split('.')[0] + 'mod_A_room_pirce_analysis_db'). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()
        index += 1

function5()