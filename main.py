from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Example App") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("data/concellos.csv", header=True, inferSchema=True)

# Show the first few rows of the DataFrame
df.show()

# Stop the SparkSession
spark.stop()