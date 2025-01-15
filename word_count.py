from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read the input text file
text_file = spark.read.text("data/wordcount/input.txt").rdd

# Perform word count
word_counts = (text_file.flatMap(lambda line: line.value.split(" "))
                          .map(lambda word: (word, 1))
                          .reduceByKey(lambda a, b: a + b))

# Collect and print the results
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Stop the Spark session
spark.stop()
