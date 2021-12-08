import collections

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql import Column
from pyspark.sql import GroupedData
from pyspark.sql import DataFrame
from pyspark.conf import SparkConf
# from pyspark import compd
from pyspark.sql.functions import split
from pyspark.sql.functions import array_contains, col, array_intersect
import time
print(f'start time = {time.time()}')
spark = SparkSession.builder.appName("SparkByExamples.com").config("spark.driver.memory", "100g").getOrCreate()
# spark = SparkSession.builder.config("spark.driver.memory", "100g").getOrCreate()
business = spark.read.csv('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/business.csv', sep='::')
users = spark.read.csv('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/user.csv', sep='::')
reviews = spark.read.csv('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/review.csv', sep='::')
print('data loaded')
business.createOrReplaceTempView('table1')
reviews.createOrReplaceTempView('table2')
users.createOrReplaceTempView('table3')
#spark.sql("SELECT t2._c2, t3._c1, t2._c3 FROM table1 t1 INNER JOIN table2 t2 INNER JOIN table3 t3 on t1._c0 = t2._c2 WHERE t1._c1 like '%Stanford%'").show(10000)
spark.sql("SELECT t2._c2, t3._c1, t2._c3 FROM table1 t1 INNER JOIN table2 t2 INNER JOIN table3 t3 on t1._c0 = t2._c2 WHERE t1._c1 like '%Stanford%'").tail(1000)
#output = spark.sql("SELECT t2._c2, t3._c1, t2._c3 FROM table1 t1 INNER JOIN table2 t2 INNER JOIN table3 t3 on t1._c0 = t2._c2 WHERE t1._c1 like '%Stanford%'") #.toDF().collect()) #.show()
#print(output.collect())
#spark.sql("SELECT t0.c1,t0.c2 FROM table1 t0 INNER JOIN table1 t1 ON t0.c1 = t1.c2 AND t0.c2 = t1.c1")
#spark.sql("SELECT table1._c0,table1._c1 FROM table1").show()
#spark.sql("SELECT table1._c0,table1._c1 FROM table1 WHERE table1._c1.contains('Stanford')").show()
#spark.sql("SELECT table1._c0,table1._c1 FROM table1 WHERE table1._c1 like '%Stanford%'").show()
#spark.sql("SELECT t1._c0,t1._c1 FROM table2 t2 INNER JOIN table3 t3 on t3._c0==t2._c0 WHERE t1._c1 like '%Stanford%'").show()

#spark.sql("SELECT t1._c0,t1._c1 FROM table1 t1 INNER JOIN table2 t2 INNER JOIN table3 t3 on t1._c0 = t2._c1 and t3._c0==t2._c0 WHERE t1._c1 like '%Stanford%'").show()
