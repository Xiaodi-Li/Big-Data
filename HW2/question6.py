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
import re
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import time
print(f'start time = {time.time()}')
spark = SparkSession.builder.config("spark.driver.memory", "100g").getOrCreate()
sc = spark.sparkContext
users = spark.read.csv('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/user.csv', sep='::')
users = users.withColumnRenamed('_c0', 'user_id').withColumnRenamed('_c1', 'name').withColumnRenamed('_c2', 'url')
reviews = spark.read.csv('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/review.csv', sep='::')
reviews = reviews.withColumnRenamed('_c0', 'review_id').withColumnRenamed('_c1', 'user_id').withColumnRenamed('_c2', 'business_id').withColumnRenamed('_c3', 'stars')
print('data loaded')
users.createOrReplaceTempView('table1')
reviews.createOrReplaceTempView('table2')
joint = spark.sql("SELECT t2.review_id, t2.user_id, t1.name FROM table1 t1 INNER JOIN table2 t2 on t1.user_id = t2.user_id")
joint = joint.groupby('name').agg({'review_id': 'count'}).withColumnRenamed('count(review_id)', 'sum_user_reviews')
joint.orderBy('sum_user_reviews', ascending = [False]).show()
df = joint.withColumn('contribution', f.col('sum_user_reviews')/f.sum('sum_user_reviews').over(Window.partitionBy()))
print(df.orderBy('contribution', ascending=False).collect())

print('user_weights done')