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
import time
print(f'start time = {time.time()}')
spark = SparkSession.builder.config("spark.driver.memory", "100g").getOrCreate()
sc = spark.sparkContext
rdd_emp = sc.textFile('/media/data1/dingcheng/workspace/Xiaodi/BigData/hadoop_hdfs_data/business.csv')
#business_rdd = rdd_emp.map(lambda x: x.split('::')).filter(lambda x: len(x)>1).map(lambda x: (x[0], x[1].split(' ')))
business_rdd = rdd_emp.map(lambda x: x.split('::'))
state_business_counts = collections.defaultdict(int)
states = ['IA', 'KS', 'UT', 'VA', 'NC', 'NE', 'SD', 'AL', 'ID', 'FM', 'DE', 'AK', 'CT', 'PR', 'NM', 'MS', 'PW', 'CO', 'NJ', 'ON',
          'FL', 'MN', 'VI', 'NV', 'AZ', 'WI', 'ND', 'PA', 'OK', 'KY', 'RI', 'NH', 'MO', 'ME', 'VT', 'GA', 'GU', 'AS', 'NY',
          'CA', 'HI', 'IL', 'TN', 'MA', 'OH', 'MD', 'MI', 'WY', 'WA', 'OR', 'MH', 'SC', 'IN', 'LA', 'MP', 'DC', 'MT', 'AR', 'WV', 'TX']
for item in business_rdd.collect():
    regex = re.compile(r'\b(' + '|'.join(states) + r')\b', re.IGNORECASE)
    st = regex.search(item[1])
    stateAbb = st.group(0).upper()
    state_business_counts[stateAbb]+=1


for k, v in state_business_counts.items():
    print(k + '\t' + str(v))
