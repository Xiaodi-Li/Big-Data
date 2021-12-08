from pyspark.sql import SparkSession
from operator import add
import time
from pyspark.sql.functions import col, avg, size
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
print(friend_pair_df.collect())
#print(sorted(friend_pair.collect()))
print('read txt file to form a rdd_emp')
end = time.time()
print(f"start={start} + '\t' end={end} '\t' total time = {end-start}")