from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, array_intersect
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
from pyspark.sql.functions import explode
df = spark.createDataFrame(((1,[3, 4, 8]),(2,[8]),(3,[1]),(4,[1]),(5,[6]),(6,[7]),(7,[1]),(8,[1, 2, 4])),["c1",'c2'])
df.withColumn('c2',explode(df['c2'])).createOrReplaceTempView('table1')
df.show()
print('df created done')
spark.sql("SELECT t0.c1,t0.c2 FROM table1 t0 INNER JOIN table1 t1 ON t0.c1 = t1.c2 AND t0.c2 = t1.c1").show()
print('spark sql INNER JOIN done')
spark.sql("SELECT t0.c1,t0.c2 FROM table1 t0 CROSS JOIN table1 t1 ON t0.c1 = t1.c2 AND t0.c2 = t1.c1").show()
print('spark sql CROSS JOIN done')
# spark.sql("SELECT t0.c1,t0.c2 FROM table1 t0 OUTER JOIN table1 t1 ON t0.c1 = t1.c2 AND t0.c2 = t1.c1").show()
# print('spark sql OUTER JOIN done')
df = df.withColumn('c2',explode(df['c2']))
df.show()
print('df.explode operation done')
df.alias('df1') \
  .join(df.alias('df2'),((col('df1.c1') == col('df2.c2')) & (col('df2.c1') == col('df1.c2')))) \
  .select(col('df1.c1'),col('df1.c2')).show()
print('all done')
df.alias('df1') \
  .join(df.alias('df2'),((col('df1.c1') == col('df2.c2')) & (col('df2.c1') == col('df1.c2')))).show()
print('all done')
df.alias('df1') \
  .join(df.alias('df2'),(col('df1.c1') == col('df2.c2'))) \
  .select(col('df1.c1'),col('df1.c2')).show()
print('all done 2')