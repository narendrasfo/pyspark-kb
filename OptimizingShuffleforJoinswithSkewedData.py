from pyspark.sql.functions import expr, col
from pyspark.sql import SparkSession
# Initialize Spark session
spark = SparkSession.builder.master("local").appName("SkewedJoin").getOrCreate()

# Sample data
user_activity = [
    ("user1", "click"),
    ("user2", "click"),
    ("user1", "scroll"),
    ("user1", "purchase"),
    ("user3", "click"),
]

user_profiles = [
    ("user1", "premium"),
    ("user2", "basic"),
]

# Create DataFrames
activity_df = spark.createDataFrame(user_activity, ["user_id", "activity"])
profiles_df = spark.createDataFrame(user_profiles, ["user_id", "profile"])

# Add random salt to skewed dataset
activity_df = activity_df.withColumn("salt", expr("floor(rand() * 3)"))  # Random salt [0, 2]

# Cross-join to replicate `profiles_df` for all salts
salted_profiles_df = profiles_df.crossJoin(
    spark.range(3).withColumnRenamed("id", "salt")  # Add salt [0, 2]
)

# Join on `user_id` and `salt`
salted_join_df = activity_df.join(
    salted_profiles_df,
    (activity_df.user_id == salted_profiles_df.user_id) & (activity_df.salt == salted_profiles_df.salt),
    "inner"
).drop("salt")

salted_join_df.show()
