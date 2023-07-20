###  spark_datamodify项目文件夹概述
1. 文件结构 <br>
    auto_update.py -> 自动更新脚本 <br>
    data_modify.py -> 数据清洗、聚合脚本 <br>
    data_analysis.py -> 数据分析脚本 (这个脚本写的有点繁杂，稍后会简单重构一下)<br>
    File_Creation.py -> spark 示例文件，展示读取文件的基本方法<br>
    Parallelization_creation.py -> spark 示例文件，展示 RDD 的基本方法<br>
    Read_CSV.py -> spark 示例文件，展示读取CSV并写入mysql的方法<br>
    lianjia.txt -> 原始数据的文本文件，原始数据以csv格式存储在 HDFS<br>
2. 运行流程<br>
   1. 先执行 data_modify.py 读取基数据并对数据进行处理 <br>
   2. 再执行 auto_update.py 对基数据二次处理，更新其他表、视图 , FK 键的创建有点问题，表建好后，在 navicat 上执行一次建立外键，代码即可运行 <br>
   3. 执行 data_analysis.py 生成 n 个表，表名设置与分区有关，看表名和代码注释就知道了（这边也需要重构，表数量有点大，要聚合一下）
      1. 该处生成的表分为两种：分区后的数据表（以分区名来命名的）、对分区数据进行统计的数据表（带有 analysis 字段）
      2. 统计表包含五种基本统计数据：数量统计、最大、最小、平均、标准差
      3. 分区后的数据表：是按照某些字段有序写入的，用 navicat 一看就知道了

3. 分区说明：
   1. 含有数值的分区：用分位点拆分
      1. 关注度与价格：CaP_data 的关注度分位点：[0.0, 1.0, 2.0, 3.0, 4.0, 6.0, 10.0] 
         1. 区间 1：[0 <= x < 1]: CaP_datagrouped_analysis_0db 
         2. 区间 3：[ 1.0 <= x < 2.0]: CaP_datagrouped_analysis_1db 
         3. 区间 4：[ 2.0 <= x < 3.0]: CaP_datagrouped_analysis_2db 
         4. 区间 5：[ 3.0 <= x < 4.0]: CaP_datagrouped_analysis_3db 
         5. 区间 6：[ 4.0 <= x < 6.0]: CaP_datagrouped_analysis_4db 
         6. 区间 7：[ 6.0 <= x < 10.0]: CaP_datagrouped_analysis_5db 
         7. 区间 7：[ x >= 10.0]: CaP_datagrouped_analysis_6db 
   
      2. 面积与价格：RaP_data 的面积分位点：[62.54, 75.26, 81.73, 86.5, 89.42, 94.72, 105.61, 117.98, 131.96]
         1. 区间 1：[ x < 62.54]: RaP_datagrouped_db0
         2. 区间 2：[62.54 <= x < 75.26]: RaP_datagrouped_db1
         3. 区间 3：[75.26 <= x < 81.73]: RaP_datagrouped_db2
         4. 区间 4：[81.73 <= x < 86.5]:  RaP_datagrouped_db3
         5. 区间 5：[86.5 <= x < 89.42]: RaP_datagrouped_db4
         6. 区间 6：[89.42 <= x < 94.72]: RaP_datagrouped_db5
         7. 区间 7：[94.72 <= x < 105.61]: RaP_datagrouped_db6
         8. 区间 8：[105.61 <= x < 117.98]: RaP_datagrouped_db7
         9. 区间 9：[117.98 <= x < 131.96]: RaP_datagrouped_db8
         10. 区间 10：[x >= 131.96]: RaP_datagrouped_db9

      3. 地址与价格：AaP_data 的价格分位点：[9622.0, 11243.0, 12676.0, 13756.0, 14996.0, 16398.0, 18400.0, 20714.0, 24430.0]
         1. 区间 1： [ x < 9622.0]：AaP_datagrouped_db0
         2. 区间 2:  [9622.0 <= x < 11243.0]：AaP_datagrouped_db1
         3. 区间 3： [11243.0 <= x < 12676.0]：AaP_datagrouped_db2
         4. 区间 4： [12676.0 <= x < 13756.0]：AaP_datagrouped_db3
         5. 区间 4： [13756.0 <= x < 14996.0]：AaP_datagrouped_db4
         6. 区间 5： [14996.0 <= x < 16398.0]：AaP_datagrouped_db5
         7. 区间 6： [16398.0 <= x < 18400.0]：AaP_datagrouped_db6
         8. 区间 7： [18400.0 <= x < 20714.0]：AaP_datagrouped_db7
         9. 区间 8： [20714.0 <= x < 24430.0]：AaP_datagrouped_db8
         10. 区间 9： [ x >= 24430.0]：AaP_datagrouped_db9
      
      4. 建成时间与价格：BTaP_data 的建成时间分位点：[2001.0, 2005.0, 2008.0, 2010.0, 2012.0, 2013.0, 2015.0, 2016.0, 2018.0]
         1. 区间 1：[ x < 2001.0]：BTaP_datagrouped_db0
         2. 区间 2：[2001 <= x < 2005.0]：BTaP_datagrouped_db1
         3. 区间 3：[2005 <= x < 2008.0]：BTaP_datagrouped_db2
         4. 区间 4：[2008 <= x < 2010.0]：BTaP_datagrouped_db3
         5. 区间 5：[2010 <= x < 2012.0]：BTaP_datagrouped_db4
         6. 区间 6：[2012 <= x < 2013.0]：BTaP_datagrouped_db5
         7. 区间 7：[2013 <= x < 2015.0]：BTaP_datagrouped_db6
         8. 区间 8：[2015 <= x < 2016.0]：BTaP_datagrouped_db7 
         9. 区间 9：[2016 <= x < 2018.0]：BTaP_datagrouped_db8
         10. 区间 10：[x >= 2018.0]：BTaP_datagrouped_db9
   2. 不含有数值的分区：用 pandas 的分区方法，按值拆分了
      1. 房屋类型与价格：Tap_data，将房屋类型分为: x室y厅的格式
      2. 装修与价格：MaP_data，将装装修类型分为:简装、精装、其他、毛胚
      3. 楼层与价格：LaP_data，将楼层划分为 高、低、中楼层
      4. 关注度与朝向：CaPB_data，将朝向分为 8 个方向
      5. 关注度与类型：CaT_data，将类型分为 x室y厅的格式，与关注度关联