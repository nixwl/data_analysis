from pyspark import SparkConf,SparkContext

conf = SparkConf().setAppName("file-create App").setMaster("local")
sc = SparkContext(conf=conf)

file = sc.textFile('hdfs://node-1:9000/user/root/testfile/test.txt')
print(file.foreach(lambda x: print(x)))

list_ = file.flatMap(lambda x: x.split(' ')).collect()
print(list_)

