###  spark_datamodify项目文件夹概述
1. 文件结构 <br>
    auto_update.py -> 自动更新脚本 <br>
    data_modify.py -> 数据清洗、聚合脚本 <br>
    File_Creation.py -> spark 示例文件，展示读取文件的基本方法<br>
    Parallelization_creation.py -> spark 示例文件，展示 RDD 的基本方法<br>
    Read_CSV.py -> spark 示例文件，展示读取CSV并写入mysql的方法<br>
    lianjia.txt -> 原始数据的文本文件，原始数据以csv格式存储在 HDFS<br>
2. 运行流程<br>
   1. 先执行 data_modify.py 读取基数据并对数据进行处理 <br>
   2. 再执行 auto_update.py 对基数据二次处理，更新其他表、视图 <br>