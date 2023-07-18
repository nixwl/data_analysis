from pyspark.sql import SparkSession
import time
import pyspark.sql.functions as func
import pymysql
from pyspark.sql.types import StructType, StructField
import pandas as pd
from pyspark.sql.functions import split, col, substring_index

'''
:@ function: 配件启动函数
'''
def conf_init():
    spark = SparkSession.builder.appName("data_analysis").master("local").getOrCreate()
    sc = spark.sparkContext
    return spark, sc


'''
:@ function: 数据库加载函数
'''
def load_data(spark, dbname):
    df2 = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true&characterEncoding=UTF-8"). \
        option("dbtable", dbname). \
        option("user", "spark"). \
        option("password", "12345678"). \
        load()
    return df2


def process_CaP_data_quantiles(spark, df_CaP_data, quantiles):
    len_ = len(quantiles)
    df_CaP_data.createOrReplaceTempView("df_CaP_data_temp_view")
    return_df = list()
    each_df_analysis = list()

    # 构造查询语句
    for i in range(len_):
        if i < len_ - 1:
            num_begin = quantiles[i]
            num_end = quantiles[i + 1]
            range_str = " `concern_rate` " + " >= " + str(num_begin) + " and " + " `concern_rate` " + " < " + str(
                num_end)
            sql_log = "select * from df_CaP_data_temp_view where" + range_str + \
                      " ORDER BY `concern_rate` asc, room_price desc, total_price desc"
            return_df.append(spark.sql(sql_log))
            # return_df.show()
        elif i == len_:
            range_str = " `concern_rate` " + " >= " + str(quantiles[i])
            sql_log = "select * from df_CaP_data_temp_view where" + range_str + \
                      " ORDER BY `concern_rate` asc, room_price desc, total_price desc"
            return_df.append(spark.sql(sql_log))

    # 依次处理 df
    for df in return_df:
        df.printSchema()
        # df.createOrReplaceTempView("df_temp_view")
        ''' df_room_price_info \ df_total_price_info 表结构如下：
        +-------+------------------+
        |summary|        room_price|
        +-------+------------------+
        |  count|               428|
        |   mean|15026.345794392524|
        | stddev| 5804.989038314126|
        |    min|            3393.0|
        |    max|           44791.0|
        '''
        df_room_price_info = df.describe("room_price")
        df_total_price_info = df.describe("total_price")
        joined_df = df_room_price_info.join(df_total_price_info, on='summary', how="inner")
        # print(joined_df)
        each_df_analysis.append(joined_df)

    process_CaP_data_quantiles_savetomysql("CaP_data", return_df, each_df_analysis, quantiles)


def process_CaP_data_quantiles_savetomysql(db_name, return_df, each_df_analysis, quantiles):
    # 依次将分组数据存入数据库：
    grouped_db = list()
    grouped_analysis_db = list()
    for name in quantiles:
        grouped_db.append(db_name + "grouped_db" + str(int(name)))
        grouped_analysis_db.append(db_name + "grouped_analysis_" + str(int(name)) + "db")
    # print(grouped_db,' ' ,grouped_analysis_db, ' ', len(quantiles))
    # 存储分组数据
    for index in range(len(return_df)):
        return_df[index].write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_db[index]). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()
    # 存储组分析数据
    for index in range(len(each_df_analysis)):
        each_df_analysis[index].write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_analysis_db[index]). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()
    print("savetomysql down")


def process_CaP_data(spark):
    df_CaP_data = load_data(spark, 'CaP_data')
    # df1.show(1000)
    quantiles = df_CaP_data.approxQuantile("concern_rate", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], 0.01)
    quantiles = list(set(quantiles))
    # print(quantiles) ## quantiles 是分位点列表
    # -----------[0.0, 1.0, 2.0, 3.0, 5.0, 7.0, 15.0] ----------------------------------------------#
    process_CaP_data_quantiles(spark, df_CaP_data, quantiles)


def process_RaP_data_quantiles(spark, df_RaP_data, quantiles):
    len_ = len(quantiles)
    df_RaP_data.createOrReplaceTempView("df_RaP_data_temp_view")
    return_df = list()
    each_df_analysis = list()

    for i in range(len_):
        if i == 0:
            range_str = " `room` " + " < " + str(quantiles[i])
            sql_log = "select * from df_RaP_data_temp_view where" + range_str + \
                      " ORDER BY `room` desc, `room_price` desc, `total_price` desc"
            return_df.append(spark.sql(sql_log))

        elif i == len_:
            range_str = " `concern_rate` " + " >= " + str(quantiles[i])
            sql_log = "select * from df_RaP_data_temp_view where" + range_str + \
                      " ORDER BY `room` desc, `room_price` desc, `total_price` desc"
            return_df.append(spark.sql(sql_log))

        elif i < len_ - 1:
            num_begin = str(quantiles[i])
            num_end = str(quantiles[i + 1])
            range_str = " `room` " + " >= " + num_begin + " and " + " `room` " + " < " + num_end
            sql_log = "select * from df_RaP_data_temp_view where" + range_str + \
                      " ORDER BY `room` desc, `room_price` desc, `total_price` desc"
            return_df.append(spark.sql(sql_log))
    # checkpoint: print(return_df)

    # 依次处理 df
    for df in return_df:
        df.printSchema()
        df_room_info = df.describe("room")
        df_room_price_info = df.describe("room_price")
        df_total_price_info = df.describe("total_price")
        joined_df = df_room_info.join(df_room_price_info, on='summary', how="inner")
        joined_df = joined_df.join(df_total_price_info, on='summary', how='inner')
        # checkpoint：print(joined_df.show())
        each_df_analysis.append(joined_df)

    process_CaP_data_quantiles_savetomysql("RaP_data", return_df, each_df_analysis, quantiles)


def process_RaP_data_quantiles_savetomysql(db_name, return_df, each_df_analysis, quantiles):
    # 依次将分组数据存入数据库：
    grouped_db = list()
    grouped_analysis_db = list()
    for name in quantiles:
        grouped_db.append(db_name + "grouped_db" + str(int(name)))
        grouped_analysis_db.append(db_name + "grouped_analysis_" + str(int(name)) + "db")
    # print(grouped_db,' ' ,grouped_analysis_db, ' ', len(quantiles))
    # 存储分组数据
    for index in range(len(return_df)):
        return_df[index].write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_db[index]). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()
    # 存储组分析数据
    for index in range(len(each_df_analysis)):
        each_df_analysis[index].write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_analysis_db[index]). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()
    print("savetomysql down")


def process_RaP_data(spark):
    df_RaP_data = load_data(spark, 'RaP_data')
    quantiles = df_RaP_data.approxQuantile("room", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], 0.01)
    quantiles = list(set(quantiles))
    quantiles.sort()
    # print(quantiles) # [63.41, 76.14, 81.76, 87.36, 89.67, 95.4, 106.58, 119.21, 132.47]
    # ---------------------------------------------------------------------------------------------- #
    process_RaP_data_quantiles(spark, df_RaP_data, quantiles)


def process_TaP_data_quantiles_savetomysql(db_name, return_df, each_df_analysis):
    for name, df in return_df:
        grouped_db_name = db_name + "grouped_db" + str(name)
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    for name, df in each_df_analysis:
        grouped_analysis_db_name = db_name + "grouped_analysis_" + str(name) + "db"
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_analysis_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    print("savetomysql down")


def process_Tap_data_quantiles(spark, df_Tap_data_pandas_groupby_dataframe):
    return_df = list()
    each_df_analysis = list()

    for each_name, each_frame in df_Tap_data_pandas_groupby_dataframe:
        each_frame.createOrReplaceTempView("each_frame_view")
        each_frame = spark.sql("select * from each_frame_view ORDER BY `room_price` desc, `total_price` desc ")
        return_df.append((each_name, each_frame))

    for each_name, each_frame in df_Tap_data_pandas_groupby_dataframe:
        each_frame_room_price_info = each_frame.describe("room_price")
        each_frame_total_price_info = each_frame.describe("total_price")
        joined_df = each_frame_room_price_info.join(each_frame_total_price_info, on='summary', how="inner")
        # print(joined_df.show())
        each_df_analysis.append((each_name, joined_df))
    # print(return_df)
    # print(each_df_analysis)

    # 存储数据
    process_TaP_data_quantiles_savetomysql("TaP_data", return_df, each_df_analysis)


def process_Tap_data(spark):
    df_Tap_data = load_data(spark, 'TaP_data')
    df_Tap_data_pandas = df_Tap_data.toPandas()
    df_Tap_data_pandas_groupbytype = df_Tap_data_pandas.groupby("type")
    df_Tap_data_pandas_groupbytype_dataframe = list()

    for group_name, group_data in df_Tap_data_pandas_groupbytype:
        group_dataframe = spark.createDataFrame(group_data)
        df_Tap_data_pandas_groupbytype_dataframe.append((group_name, group_dataframe))
    # check_point:
    # for group_name, group_dataframe in df_Tap_data_pandas_groupbytype_dataframe:
    #     print(f"Group: {group_name}")
    #     group_dataframe.show()

    process_Tap_data_quantiles(spark, df_Tap_data_pandas_groupbytype_dataframe)
    # ---------------------------------------------------------------------------------------------- #
    # process_RaP_data_quantiles(spark, df_RaP_data, quantiles)


def process_Map_data_quantiles_savetomysql(db_name, return_df, each_df_analysis):
    for name, df in return_df:
        grouped_db_name = db_name + "grouped_db" + str(name)
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    for name, df in each_df_analysis:
        grouped_analysis_db_name = db_name + "grouped_analysis_" + str(name) + "db"
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_analysis_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    print("savetomysql down")


def process_Map_data_quantiles(spark, df_MaP_data_pandas_groupby_what_fix_dataframe):
    return_df = list()
    each_df_analysis = list()

    for each_name, each_frame in df_MaP_data_pandas_groupby_what_fix_dataframe:
        each_frame.createOrReplaceTempView("each_frame_view")
        each_frame = spark.sql("select * from each_frame_view ORDER BY `room_price` desc, `total_price` desc ")
        return_df.append((each_name, each_frame))

    for each_name, each_frame in df_MaP_data_pandas_groupby_what_fix_dataframe:
        each_room_price_info = each_frame.describe("room_price")
        each_total_price_info = each_frame.describe("total_price")
        joined_df = each_room_price_info.join(each_total_price_info, on='summary', how="inner")
        # print(joined_df.show())
        each_df_analysis.append((each_name, joined_df))
    # print(return_df)
    # print(each_df_analysis)

    # 存储数据
    process_Map_data_quantiles_savetomysql("MaP_data", return_df, each_df_analysis)


def process_MaP_data(spark):
    df_MaP_data = load_data(spark, 'MaP_data')
    df_MaP_data_pandas = df_MaP_data.toPandas()
    df_MaP_data_pandas_groupby_what_fix = df_MaP_data_pandas.groupby("what_fix")
    df_MaP_data_pandas_groupby_what_fix_dataframe = list()

    for group_name, group_data in df_MaP_data_pandas_groupby_what_fix:
        group_dataframe = spark.createDataFrame(group_data)
        df_MaP_data_pandas_groupby_what_fix_dataframe.append((group_name, group_dataframe))

    # check_point:
    # for group_name, group_dataframe in df_MaP_data_pandas_groupbytype_dataframe:
    #     print(f"Group: {group_name}")
    #     group_dataframe.show()
    process_Map_data_quantiles(spark, df_MaP_data_pandas_groupby_what_fix_dataframe)


def process_LaP_data_quantiles_savetomysql(db_name, return_df, each_df_analysis):
    for name, df in return_df:
        grouped_db_name = db_name + "grouped_db" + str(name)
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    for name, df in each_df_analysis:
        grouped_analysis_db_name = db_name + "grouped_analysis_" + str(name) + "db"
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_analysis_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    print("savetomysql down")


def process_LaP_data_quantiles(spark, df_LaP_data_pandas_groupby_current_level_dataframe):
    return_df = list()
    each_df_analysis = list()

    for each_name, each_frame in df_LaP_data_pandas_groupby_current_level_dataframe:
        each_frame.createOrReplaceTempView("each_frame_view")
        each_frame = spark.sql("select * from each_frame_view ORDER BY `room_price` desc, `total_price` desc ")
        return_df.append((each_name, each_frame))

    for each_current_level_name, each_current_level_frame in df_LaP_data_pandas_groupby_current_level_dataframe:
        each_current_level_frame_room_price_info = each_current_level_frame.describe("room_price")
        each_current_level_frame_total_price_info = each_current_level_frame.describe("total_price")

        joined_df = each_current_level_frame_room_price_info.join(each_current_level_frame_total_price_info,
                                                                  on='summary', how="inner")
        # print(joined_df.show())
        each_df_analysis.append((each_current_level_name, joined_df))
    # print(return_df)
    # print(each_df_analysis)

    # 存储数据
    process_LaP_data_quantiles_savetomysql("LaP_data", return_df, each_df_analysis)


def process_LaP_data(spark):
    df_LaP_data = load_data(spark, 'LaP_data')
    split_df_LaP_data = df_LaP_data.select(col("id"), col("room_price"), col("total_price"),
                                           substring_index(col("level"), "(共", 1).alias("current_level"),
                                           substring_index(col("level"), "(共", -1).alias("total_level"))
    split_df_LaP_data = split_df_LaP_data.select(col("id"), col("room_price"), col("total_price"),
                                                 col("current_level"),
                                                 substring_index(col("total_level"), "层)", 1).alias("total_level")
                                                 )
    df_LaP_data = split_df_LaP_data
    # print(df_LaP_data.show())

    df_LaP_data_pandas = df_LaP_data.toPandas()
    df_LaP_data_pandas_groupby_current_level = df_LaP_data_pandas.groupby("current_level")
    df_LaP_data_pandas_groupby_current_level_dataframe = list()

    for group_name, group_data in df_LaP_data_pandas_groupby_current_level:
        group_dataframe = spark.createDataFrame(group_data)
        df_LaP_data_pandas_groupby_current_level_dataframe.append((group_name, group_dataframe))

    # check_point:
    # for group_name, group_dataframe in df_LaP_data_pandas_groupbytype_dataframe:
    #     print(f"Group: {group_name}")
    #     group_dataframe.show()
    process_LaP_data_quantiles(spark, df_LaP_data_pandas_groupby_current_level_dataframe)


def process_AaP_data_quantiles_savetomysql(db_name, return_df, each_df_analysis, quantiles):
    # 依次将分组数据存入数据库：
    grouped_db = list()
    grouped_analysis_db = list()
    for name in quantiles:
        grouped_db.append(db_name + "grouped_db" + str(int(name)))
        grouped_analysis_db.append(db_name + "grouped_analysis_" + str(int(name)) + "db")
    # print(grouped_db,' ' ,grouped_analysis_db, ' ', len(quantiles))
    # 存储分组数据
    for index in range(len(return_df)):
        return_df[index].write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_db[index]). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()
    # 存储组分析数据
    for index in range(len(each_df_analysis)):
        each_df_analysis[index].write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_analysis_db[index]). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()
    print("savetomysql down")


def process_AaP_data_quantiles(spark, df_AaP_data, quantiles):
    len_ = len(quantiles)
    df_AaP_data.createOrReplaceTempView("df_RaP_data_temp_view")
    return_df = list()
    each_df_analysis = list()

    # [9594.0, 11231.0, 12494.0, 13792.0, 14983.0, 16465.0, 18353.0, 20747.0, 24400.0]
    for i in range(len_):
        if i == 0:
            range_str = " `room_price` " + " < " + str(quantiles[i])
            sql_log = "select * from df_RaP_data_temp_view where" + range_str + " ORDER BY room_price desc, total_price desc"
            return_df.append(spark.sql(sql_log))
        elif i == len_ - 1:
            range_str = " `room_price` " + " >= " + str(quantiles[i])
            sql_log = "select * from df_RaP_data_temp_view where" + range_str + " ORDER BY room_price desc, total_price desc"
            return_df.append(spark.sql(sql_log))
        elif i < len_ - 1:
            range_str = " `room_price` " + " >= " + str(quantiles[i]) + " and " + " `room_price` " + " < " + str(
                quantiles[i + 1])
            sql_log = "select * from df_RaP_data_temp_view where" + range_str + " ORDER BY room_price desc, total_price desc"
            return_df.append(spark.sql(sql_log))
    # print(return_df)

    # 依次处理 df
    for df in return_df:
        df.printSchema()
        # df.createOrReplaceTempView("df_temp_view")
        df_room_price_info = df.describe("room_price")
        df_total_price_info = df.describe("total_price")
        joined_df = df_room_price_info.join(df_total_price_info, on='summary', how="inner")
        # print(joined_df)
        each_df_analysis.append(joined_df)

    process_AaP_data_quantiles_savetomysql("AaP_data", return_df, each_df_analysis, quantiles)


def process_AaP_data(spark):
    df_AaP_data = load_data(spark, 'AaP_data')
    # df_AaP_data.createOrReplaceTempView("AaP_data_view")
    quantiles = df_AaP_data.approxQuantile("room_price", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], 0.01)
    quantiles = list(set(quantiles))
    quantiles.sort()

    process_AaP_data_quantiles(spark, df_AaP_data, quantiles)


def process_BTaP_data_quantiles_savetomysql(db_name, return_df, each_df_analysis, quantiles):
    # 依次将分组数据存入数据库：
    grouped_db = list()
    grouped_analysis_db = list()
    for name in quantiles:
        grouped_db.append(db_name + "grouped_db" + str(int(name)))
        grouped_analysis_db.append(db_name + "grouped_analysis_" + str(int(name)) + "db")
    # print(grouped_db,' ' ,grouped_analysis_db, ' ', len(quantiles))
    # 存储分组数据
    for index in range(len(return_df)):
        return_df[index].write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_db[index]). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()
    # 存储组分析数据
    for index in range(len(each_df_analysis)):
        each_df_analysis[index].write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_analysis_db[index]). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()
    print("savetomysql down")


def process_BTaP_data_quantiles(spark, df_BTaP_data, quantiles):
    len_ = len(quantiles)
    df_BTaP_data.createOrReplaceTempView("df_BTaP_data_temp_view")
    return_df = list()
    each_df_analysis = list()
    # [2001.0, 2005.0, 2008.0, 2010.0, 2012.0, 2013.0, 2015.0, 2016.0, 2018.0]

    for i in range(len_):
        if i == 0:
            range_str = " `built_time` " + " < " + str(int(quantiles[i]))
            sql_log = "select * from df_BTaP_data_temp_view where " + range_str + " ORDER BY room_price desc, total_price desc"
            return_df.append(spark.sql(sql_log))
        elif i == len_ - 1:
            range_str = " `built_time` " + " >= " + str(int(quantiles[i]))
            sql_log = "select * from df_BTaP_data_temp_view where " + range_str + " ORDER BY room_price desc, total_price desc"
            return_df.append(spark.sql(sql_log))
        elif i < len_ - 1:
            range_str = " `built_time` " + " >= " + str(int(quantiles[i])) + " and " + " `built_time` " + " < " + str(
                int(quantiles[i + 1]))
            sql_log = "select * from df_BTaP_data_temp_view where " + range_str + " ORDER BY room_price desc, total_price desc"
            return_df.append(spark.sql(sql_log))
    # print(return_df)

    # 依次处理 df
    for df in return_df:
        df.printSchema()
        # df.createOrReplaceTempView("df_temp_view")
        df_built_time_info = df.describe("built_time")
        df_room_price_info = df.describe("room_price")
        df_total_price_info = df.describe("total_price")
        joined_df = df_room_price_info.join(df_total_price_info, on='summary', how="inner")
        joined_df = joined_df.join(df_built_time_info, on='summary', how="inner")
        each_df_analysis.append(joined_df)

    # print(each_df_analysis)

    process_BTaP_data_quantiles_savetomysql("BTaP_data", return_df, each_df_analysis, quantiles)


def process_BTaP_data(spark):
    df_BTaP_data = load_data(spark, 'BTaP_data')
    # df_AaP_data.createOrReplaceTempView("AaP_data_view")
    quantiles = df_BTaP_data.approxQuantile("built_time", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], 0.01)
    quantiles = list(set(quantiles))
    quantiles.sort()
    print(quantiles)

    process_BTaP_data_quantiles(spark, df_BTaP_data, quantiles)


def process_CaPB_data_quantiles_savetomysql(db_name, return_df, each_df_analysis):
    for name, df in return_df:
        grouped_db_name = db_name + "grouped_db" + str(name)
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    for name, df in each_df_analysis:
        grouped_analysis_db_name = db_name + "grouped_analysis_" + str(name) + "db"
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_analysis_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    print("savetomysql down")


def process_CaPB_data_quantiles(spark, df_CaPB_data_pandas_groupby_orient_dataframe):
    return_df = list()
    each_df_analysis = list()

    for each_name, each_frame in df_CaPB_data_pandas_groupby_orient_dataframe:
        each_frame.createOrReplaceTempView("each_frame_view")
        each_frame = spark.sql("select * from each_frame_view ORDER BY `concern_rate` desc")
        return_df.append((each_name, each_frame))

    for each_name, each_frame in df_CaPB_data_pandas_groupby_orient_dataframe:
        each_concern_rate_info = each_frame.describe("concern_rate")
        joined_df = each_concern_rate_info
        each_df_analysis.append((each_name, joined_df))
    # print(return_df)
    # print(each_df_analysis)

    # 存储数据
    process_CaPB_data_quantiles_savetomysql("CaPB_data", return_df, each_df_analysis)


def process_CaPB_data(spark):
    df_CaPB_data = load_data(spark, 'CaPB_data')
    df_CaPB_data_pandas = df_CaPB_data.toPandas()
    ddf_CaPB_data_pandas_groupby_orient = df_CaPB_data_pandas.groupby("orient")
    df_CaPB_data_pandas_groupby_orient_dataframe = list()

    for group_name, group_data in ddf_CaPB_data_pandas_groupby_orient:
        group_dataframe = spark.createDataFrame(group_data)
        df_CaPB_data_pandas_groupby_orient_dataframe.append((group_name, group_dataframe))

    ## check_point:
    # for group_name, group_dataframe in df_BTaP_data_pandas_groupby_orient_dataframe:
    #     print(f"Group: {group_name}")
    #     group_dataframe.show()

    process_CaPB_data_quantiles(spark, df_CaPB_data_pandas_groupby_orient_dataframe)


def process_CaT_data_quantiles_savetomysql(db_name, return_df, each_df_analysis):
    for name, df in return_df:
        grouped_db_name = db_name + "grouped_db" + str(name)
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    for name, df in each_df_analysis:
        grouped_analysis_db_name = db_name + "grouped_analysis_" + str(name) + "db"
        df.write.mode("overwrite"). \
            format("jdbc"). \
            option("url", "jdbc:mysql://192.168.101.20:3306/spark?useSSL=false&Unicode=true"). \
            option("dbtable", grouped_analysis_db_name). \
            option("user", "spark"). \
            option("password", "12345678"). \
            save()

    print("savetomysql down")


def process_CaT_data_quantiles(spark, df_CaPB_data_pandas_groupby_type_dataframe):
    return_df = list()
    each_df_analysis = list()

    for each_name, each_frame in df_CaPB_data_pandas_groupby_type_dataframe:
        each_frame.createOrReplaceTempView("each_frame_view")
        each_frame = spark.sql("select * from each_frame_view ORDER BY `concern_rate` desc")
        return_df.append((each_name, each_frame))

    for each_name, each_frame in df_CaPB_data_pandas_groupby_type_dataframe:
        each_concern_rate_info = each_frame.describe("concern_rate")
        joined_df = each_concern_rate_info
        each_df_analysis.append((each_name, joined_df))

    # 存储数据
    process_CaT_data_quantiles_savetomysql("CaT_data", return_df, each_df_analysis)


def process_CaT_data(spark):
    df_CaT_data = load_data(spark, 'CaT_data')
    df_CaT_data_pandas = df_CaT_data.toPandas()
    ddf_CaPB_data_pandas_groupby_type = df_CaT_data_pandas.groupby("type")
    df_CaPB_data_pandas_groupby_type_dataframe = list()

    for group_name, group_data in ddf_CaPB_data_pandas_groupby_type:
        group_dataframe = spark.createDataFrame(group_data)
        df_CaPB_data_pandas_groupby_type_dataframe.append((group_name, group_dataframe))

    process_CaT_data_quantiles(spark, df_CaPB_data_pandas_groupby_type_dataframe)


def __main__():
    spark, sc = conf_init()

    # 处理 CaP_data
    # process_CaP_data(spark)

    # 处理 RaP_data
    # process_RaP_data(spark)

    # 处理 TaP_data
    # process_Tap_data(spark)

    # 处理 MaP_data
    # process_MaP_data(spark)

    # 处理 LaP_data,将 level 字段的 【中楼层(共6层)】 切分为 两个字段 current_level ， total_level
    # 按照 上面的示例：current_level = 中楼层， total_level = 6
    # process_LaP_data(spark)

    # 处理 AaP_data
    # process_AaP_data(spark)

    # 处理 BTaP_data
    # process_BTaP_data(spark)

    # 处理 CaPB_data
    # process_CaPB_data(spark)
    process_CaT_data(spark)
    # 处理 CaT_data

    spark.stop()


__main__()
