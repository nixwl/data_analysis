from pyspark import SparkConf, SparkContext

spark = SparkConf().setAppName("PySpark App").setMaster("local")
sc = SparkContext(conf=spark)
print("hello world")
data=[1,2,3,4,5]
disdata = sc.parallelize(data)
result = disdata.map(lambda x : x + 1).collect()
print(result)