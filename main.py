from pyspark.sql import SparkSession

from transformations import rename_cols, get_rows_with_different_domains

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Example App") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("data/concellos.csv", header=True, inferSchema=True)

# Show the first few rows of the DataFrame
df = df.transform(rename_cols).transform(get_rows_with_different_domains)

print(f"Numero de casos con distintos dominios: {df.count()}")

df.show(truncate=False)
# Stop the SparkSession
spark.stop()