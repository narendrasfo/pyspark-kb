from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("join rdd").getOrCreate()
rd1 = spark.sparkContext.textFile("../data/join1")
rd2 = spark.sparkContext.textFile("../data/join2")
rd1=rd1.map(lambda line: (line.split(",")[0],line.split(",")[1]))
rd2=rd2.map(lambda line: (line.split(",")[0],line.split(",")[1]))
res=rd1.join(rd2)
for x in res.collect():
    print(f"{x[0]},{x[1][0]},{x[1][1]}")

