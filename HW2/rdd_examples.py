from pyspark import SparkContext
sc = SparkContext.getOrCreate()
# rdd1 = sc.parallelize([("www.page1.html", "word1"), ("www.page2.html", "word1"),
#     ("www.page1.html", "word3")])
#
# rdd2 = sc.parallelize([("www.page1.html", 7.3), ("www.page2.html", 1.25),
#     ("www.page3.html", 5.41)])
#
# intersection_rdd = rdd1.cogroup(rdd2).filter(lambda x: x[1][0] and x[1][1])
# intersection_rdd.map(lambda x: (x[0], (list(x[1][0]), list(x[1][1])))).collect()
print('done')

rdd1 = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5)])
rdd2 = sc.parallelize([('aa', 1), ('bb', 2), ('cc', 3), ('dd', 4), ('ee', 5)])
rdd3 = sc.parallelize([(('a', 'aa'), 1), (('b', 'dd'), 8), (('e', 'aa'), 34), (('c', 'ab'), 23)])

print (rdd3.map(lambda x: (x[0][0], (x[0][1], x[1])))\
.join(rdd1)\
.map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1]))).join(rdd2)\
.filter(lambda x: x[1][0][1] > x[1][0][2] + x[1][1])\
.map(lambda x: ((x[1][0][0], x[0]), x[1][0][1]))\
.collect())

#--> [(('b', 'dd'), 8), (('e', 'aa'), 34)]