from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,col,trim

spark=SparkSession.builder.appName("word count").getOrCreate()
df=spark.read.text("../data/file1")
res=df.select(explode(split(col("value"),",")).alias("word")).\
    select(trim(col("word")).alias("word")).groupby(col("word")).count()
res.show()


# RDD way to
rdd = spark.sparkContext.textFile("../data/file1")

# Step 3: Perform word count
# Split lines into words, map each word to a tuple (word, 1), and reduce by key (word)
word_counts = rdd.flatMap(lambda line: line.split(" ")) \
                 .map(lambda word: (word, 1)) \
                 .reduceByKey(lambda a, b: a + b)

# Step 4: Collect and display the results
for word, count in word_counts.collect():
    print(f"{word}: {count}")
