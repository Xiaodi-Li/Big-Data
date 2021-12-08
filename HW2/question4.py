import collections
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
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
from pyspark.sql.functions import col, avg
import time
print(f'start time = {time.time()}')
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
business = spark.read.csv('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/business.csv', sep='::')
reviews = spark.read.csv('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/review.csv', sep='::')
reviews = reviews.groupby('_c2').agg({"_c3": "avg"}).withColumnRenamed('avg(_c3)', 'avg_rating')
print(reviews.join(business, reviews._c2==business._c0).dropDuplicates().orderBy(reviews.avg_rating, ascending = [False]).select(business._c0, business._c1, business._c2, reviews.avg_rating).collect())

# window = Window.partitionBy(df['user_id']).orderBy(df['score'].desc())
# business.createOrReplaceTempView('table1')
# reviews.createOrReplaceTempView('table2')
# reviews.printSchema()
# spark.sql("SELECT t1._c0,t1._c1, t1._c2, t2.avg_rating FROM table1 t1 INNER JOIN table2 t2 on t1._c0 = t2._c2").show()