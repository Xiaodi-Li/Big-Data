from pyspark.sql import SparkSession
from operator import add
import time
from pyspark.sql.functions import col, avg, size, sum
from pyspark.sql.types import IntegerType,BooleanType,DateType,DoubleType
start = time.time()
spark = SparkSession.builder.config("spark.driver.memory", "100g").getOrCreate()
sc = spark.sparkContext
rdd_emp = sc.textFile('/media/data1/dingcheng/workspace/Xiaodi/BigData/mutual.txt')
friendList1 = rdd_emp.map(lambda x: x.split('\t')).filter(lambda x: len(x)>1).map(lambda x: (x[0], x[1].split(',')))
joint = friendList1.map(lambda x: (x[0], x[1]))
edge = joint.flatMap(lambda x : [(x[0],x[1][i]) for i in range(len(x[1]))])
edge = edge.map(lambda x: (x[1], x[0]))
friend_pair = edge.join(edge).distinct().map(lambda x: (x[1], [x[0]]))
friend_pair = friend_pair.filter(lambda x: x[0][0]!=x[0][1] and x[0][0]<x[0][1])
friend_pair = friend_pair.reduceByKey(add)
friend_pair_df = friend_pair.toDF().withColumnRenamed('_1', 'friend_pair').withColumnRenamed('_2', 'array_common_friends')
friend_pair_df = friend_pair_df.select('friend_pair', 'array_common_friends', size('array_common_friends')).withColumnRenamed('size(array_common_friends)', 'num_common_friends').orderBy('num_common_friends', ascending=True)
avg=friend_pair_df.select(avg('num_common_friends').cast("int")).collect()[0][0]
print(friend_pair_df.filter(friend_pair_df['num_common_friends'] > avg).collect())
#print(friend_pair_df.select('num_common_friends').collect())
# print('summation of number of common friends')
# summation=friend_pair_df.select(sum('num_common_friends').cast("int")).collect()[0][0]
# print(summation +' count of friend_pair_df' + friend_pair_df.count())
# print('average number of common friends')
# print(summation/friend_pair_df.count()) ## 1.6
# #print(friend_pair_df.filter(friend_pair_df['num_common_friends'] > avg).collect())
# end = time.time()
# print(f"start={start} + '\t' end={end} '\t' total time = {end-start}")
# end = time.time()
# print(f"start={start} + '\t' end={end} '\t' total time = {end-start}")