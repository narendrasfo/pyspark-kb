from pyspark.sql import SparkSession

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Find Overlap in Two Lists") \
    .getOrCreate()

# Step 2: Prepare the input lists
list1 = ["id28", "id1", "id2", "id3", "id25"]  # Example data
list2 = ["id25", "id30", "id1001", "id28"]  # Example data

# Step 3: Parallelize the lists to create RDDs
rdd1 = spark.sparkContext.parallelize(list1)
rdd2 = spark.sparkContext.parallelize(list2)

# Step 4: Find the intersection of the two RDDs
intersection = rdd1.intersection(rdd2)

# Step 5: Collect and display the result
overlap = intersection.collect()
print("Overlap between List 1 and List 2:", overlap)

# Step 6: Stop the Spark session
spark.stop()
