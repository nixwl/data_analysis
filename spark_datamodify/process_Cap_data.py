
def process_quantiles(spark, df_CaP_data, quantiles):
    len_ = len(quantiles)
    df_CaP_data.createOrReplaceTempView("df_CaP_data_temp_view")
    return_df = list()
    each_df_analysis = list()

    # 构造查询语句
    for i in range(len_):
        if i < len_ - 1:
            num_begin = quantiles[i]
            num_end = quantiles[i + 1]
            range_str = " `concern_rate` " + " >= " + str(num_begin) + " and " + " `concern_rate` " + " < " + str(num_end)
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

    savetomysql("CaP_data", return_df, each_df_analysis, quantiles)

def savetomysql(db_name, return_df, each_df_analysis,quantiles):
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

def CaP_data_Process_init(spark, df_CaP_data):

    # df1.show(1000)
    quantiles = df_CaP_data.approxQuantile("concern_rate", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], 0.01)
    quantiles = list(set(quantiles))
    # print(quantiles) ## quantiles 是分位点列表
    # -----------[0.0, 1.0, 2.0, 3.0, 5.0, 7.0, 15.0] ----------------------------------------------#
    process_quantiles(spark, df_CaP_data, quantiles)