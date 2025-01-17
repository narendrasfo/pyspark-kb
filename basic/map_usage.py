from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark= SparkSession.builder.appName("map example").getOrCreate()
# calculate the sq for all elements

list=[1,2,3,4,5]
listrdd=spark.sparkContext.parallelize(list)
sq=listrdd.map(lambda x:x*x)
for i in  sq.collect():
    print(i)


# based on df
df=spark.createDataFrame([(x,) for x in list],["number"])
df=df.withColumn("sqt",pow(col("number"),2))
df.show()
spark.stop()


