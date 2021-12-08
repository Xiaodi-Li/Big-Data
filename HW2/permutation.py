from functools import reduce
from itertools import chain
from pyspark.conf import SparkConf
from pyspark import SparkContext
#s1 = SparkSession.builder.config("k1", "v1").getOrCreate()
appName = 'bigdata'
master = 'local'
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
rdd1=sc.parallelize(['P','T','K'])
rdd1.collect()
['P', 'T', 'K']

def combinations_of_length_n(rdd, n):
    # for n > 0
    return reduce(
        lambda a, b: a.cartesian(b).map(lambda x: tuple(chain.from_iterable(x))),
        [rdd]*n
    ).filter(lambda x: len(set(x))==n)

print(combinations_of_length_n(rdd1, n=2).collect())