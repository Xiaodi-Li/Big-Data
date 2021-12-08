from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add
from pyspark.sql.functions import explode
#sc = SparkContext.getOrCreate()
spark = SparkSession.builder.config("spark.driver.memory", "100g").getOrCreate()
sc = spark.sparkContext
#rdd_emp = sc.textFile('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/mutual.txt')
rdd_emp = sc.textFile('/media/data1/dingcheng/workspace/Xiaodi/BigData/mutual_test2.txt')
#rdd_emp.map(lambda x: x.split('\t')).collect()
friendList1 = rdd_emp.map(lambda x: x.split('\t')).filter(lambda x: len(x)>1).map(lambda x: (x[0], x[1].split(',')))
#friendList2 = rdd_emp.map(lambda x: x.split('\t')).filter(lambda x: len(x)>1).map(lambda x: x[1].split(','))
#friendList1.map(lambda x: print(x))
#friend_pairs = friendList1.map(lambda x: friendList1.map(lambda y: (x.interection(y))))
#friend_pairs = friendList1.cogroup(friendList1)

#friendList2 = friendList1.flatMap(lambda x: [(x[0], w) for w in x[1]])
# res = []
# for i in range(rdd_emp.count()):
#     res.append(friendList1.map(lambda x: {(i, x[0]): x[1].intersection(friendList1.collect()[i][1])}))
# print (rdd3.map(lambda x: (x[0][0], (x[0][1], x[1])))\
# .join(rdd1)\
# .map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1]))).join(rdd2)\
# .filter(lambda x: x[1][0][1] > x[1][0][2] + x[1][1])\
# .map(lambda x: ((x[1][0][0], x[0]), x[1][0][1]))\
# .collect())
joint = friendList1.map(lambda x: (x[0], x[1])) #.join(friendList1).map(lambda x: (x[0], x[1])).filter(lambda x: x[1].intersection(x[3]) and x[0]!=x[2]).map(lambda x: ((x[0], x[2]), x[1].intersection[3])).collect()
#joint2 = joint.cartesian(joint)
#print(joint2.collect())
#print(joint2.collect()[0:2])
edge = joint.flatMap(lambda x : [(x[0],x[1][i]) for i in range(len(x[1]))])
#edge2 = joint.flatMap(lambda x : [(x[1][i],x[0]) for i in range(len(x[1]))])
edge = edge.map(lambda x: (x[1], x[0]))
print(edge.collect())
# edge2 = edge2.map(lambda x: (x[1], x[0]))
#friend_pair = edge.join(edge2).distinct()
# friend_pair = edge.join(edge2).distinct().map(lambda x: (x[1], [x[0]]))
# friend_pair = friend_pair.filter(lambda x: x[0][0]!=x[0][1])
#print(sorted(friend_pair.collect()))
friend_pair = edge.join(edge).distinct().map(lambda x: (x[1], [x[0]]))
friend_pair = friend_pair.filter(lambda x: x[0][0]!=x[0][1] and x[0][0]<x[0][1])
friend_pair = friend_pair.reduceByKey(add)

#print(sorted(edge.join(edge2).distinct().collect()))
print(sorted(friend_pair.collect()))
print('read txt file to form a rdd_emp')
