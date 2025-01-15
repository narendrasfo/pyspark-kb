from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split,col,trim
# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Find Overlap in Two Lists using DataFrame API") \
    .getOrCreate()

# Step 2: Set S3 configuration
# Replace with your AWS credentials and region if necessary
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

# Step 3: Define the S3 paths
# s3_path_list1 = "s3a://your-bucket-name/path-to-list1.txt"
# s3_path_list2 = "s3a://your-bucket-name/path-to-list2.txt"

s3_path_list1 = "/Users/narendra/pythonworkspace/pyspark-kb/data/file1"
s3_path_list2 = "/Users/narendra/pythonworkspace/pyspark-kb/data/file2"

# Step 4: Read the lists from S3 into DataFrames
df1_raw = spark.read.text(s3_path_list1)
df2_raw = spark.read.text(s3_path_list2)

# Step 5: Split the comma-separated values into individual rows
df1 = df1_raw.select(explode(split(df1_raw["value"], ",")).alias("id")).select(trim(col("id")).alias("id"))
df2 = df2_raw.select(explode(split(df2_raw["value"], ",")).alias("id")).select(trim(col("id")).alias("id"))

# # Step 6: Remove any leading/trailing quotes and spaces
# df1 = df1.withColumn("id", df1["id"].substr(2, len(df1["id"]) - 2).alias("id"))  # Removes surrounding quotes
# df2 = df2.withColumn("id", df2["id"].substr(2, len(df2["id"]) - 2).alias("id"))  # Removes surrounding quotes


# Step 5: Use DataFrame API to find overlap
df1.show()
df2.show()
intersection_df = df1.join(df2, on="id", how="inner")

# Step 6: Show the overlapping elements
intersection_df.show(truncate=False)

# Step 7: Optionally collect results to the driver
overlap = [row['id'] for row in intersection_df.collect()]
print("Overlap between List 1 and List 2:", overlap)

# Step 8: Stop the Spark session
spark.stop()
