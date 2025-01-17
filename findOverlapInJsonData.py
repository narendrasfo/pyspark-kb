from pyspark.sql import SparkSession

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("Find Overlapping IDs") \
    .getOrCreate()

# Step 2: Load JSON datasets
dataset1_path = "data/dataset1.json"
dataset2_path = "data/dataset2.json"

# Read datasets as arrays
df1 = spark.read.json(dataset1_path, multiLine=True)
df1.printSchema()
df2 = spark.read.json(dataset2_path, multiLine=True)

# Convert arrays into DataFrames with one value per row
df1_flat = df1.selectExpr("explode(id) as id")
df2_flat = df2.selectExpr("explode(id) as id")
df1_flat.show()
# Step 3: Find overlapping IDs using inner join
overlap_df = df1_flat.join(df2_flat, "id", "inner").distinct()

# Step 4: Show or save the result
overlap_df.show()

# Optionally save the output to a file
output_path = "path/to/output.json"
overlap_df.write.json(output_path)

# Stop the SparkSession
spark.stop()
