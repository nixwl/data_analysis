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