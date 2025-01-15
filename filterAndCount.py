from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("FilterAndCount").getOrCreate()

# Read the input log file
logs = spark.read.csv("data/filter_count/logs.csv", header=True, inferSchema=True)

# Filter logs for a specific user
user_logs = logs.filter(logs.user_id == "specific_user_id")

# Count the number of logs for the user
count = user_logs.count()

print(f"Number of logs for specific_user_id: {count}")

# Stop the Spark session
spark.stop()
