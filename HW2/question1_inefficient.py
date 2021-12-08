import collections
'''
the following approach is too inefficient
'''
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
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
mutual_txt = spark.read.csv('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/mutual.txt', sep='\t')
#print(mutual_txt.collect()[1][1].split(','))
print(mutual_txt.show())
print(mutual_txt.count())
print(set(mutual_txt.collect()[0][1].split(',')) & set(mutual_txt.collect()[1][1].split(',')))
mutual_txt_v2 = mutual_txt.withColumn('friends_arr', split(mutual_txt._c1, ','))
print(mutual_txt_v2)
# df_pairs_list = []
final_res = collections.defaultdict(list)
print(f'start time = {time.time()}')

#filteredInput = mutual_txt_v2.map(line => line.split("\t")).filter(line=>(line.length>1)).map(line =>{val friendsList = line(1).split(","); val friendsList1 = line(1).split(",");friendsList.map(x=>{friendsList1.map(y=>{if(x!=y) ((x,y),"1") else ((x,line(0)),"-1")})}  )})

# for i in range(mutual_txt_v2.count()):
#     df_1 = mutual_txt_v2.filter(mutual_txt_v2._c0==i)
#     df_rem = mutual_txt_v2.filter(mutual_txt_v2._c0>i)
#     #spark.parallelize(df_1.select('friends_arr'))
#     # df_1.select('friends_arr').intersect(df_rem.select('friends_arr')).show()
#     #df_1.show()
#     #print(df_rem.count())
#     #intersect_friends = array_intersect(df_rem.friends_arr, df_1.friends_arr)
#     df_pairs = df_rem.select(array_intersect(df_1.friends_arr, df_rem.friends_arr)).collect()
#     #df_pairs_list.append(df_pairs)
#     for j in range(len(df_pairs)):
#         if len(df_pairs[j]) > 0:
#             final_res[(i,j+i+1)] = list(df_pairs[j])
#     print(f'{i}-th end time = {time.time()}')
    #df_pairs.show()
    #res = df_rem.filter(intersect_friends==0)
    #res.show()
with open('ansewr2question1.txt', 'w') as out:
    for key, value in final_res.items():
        out.write(key + '\t' + value +'\n')
print(f'end time = {time.time()}')