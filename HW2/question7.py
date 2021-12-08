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
from operator import add
import time
print(f'start time = {time.time()}')
spark = SparkSession.builder.config("spark.driver.memory", "100g").getOrCreate()
sc = spark.sparkContext
rdd_data = sc.textFile('/media/data1/dingcheng/workspace/Xiaodi/BigData/BigData/HW2/userdata.txt')
# user_data = rdd_data.flatMap(lambda content: (content.lower.split(',')[0], content.lower().split(',')[1:])).map(lambda line, word: (word, [line])).reduceByKey(lambda a, b: a+b)
# print('inverted index constructed')

# output = rdd_data.flatMap(lambda line:[(line.lower().split(',')[0], word) for word in line.lower().split(',')[1:]]).map(lambda x: (x[1], x[0])) \
#       .reduceByKey(lambda a: a[0]+a[1])
# print('inverted index constructed')

invert_index = rdd_data.flatMap(lambda line:[(line.lower().split(',')[0], word) for word in line.lower().split(',')[1:]]) \
    .map(lambda x: (x[1], [x[0]]))
    #.reduceByKey(lambda a: a[0]+a[1])
invert_index = invert_index.reduceByKey(add)
print(invert_index.collect())
print('inverted index constructed')