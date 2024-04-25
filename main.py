import configparser

from pyspark import SparkConf
from pyspark.sql import SparkSession

from lib.logger import Log4J
from transformations import rename_cols, get_rows_with_different_domains


def get_spark_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (k, v) in config.items("SPARK_CONF"):
        spark_conf.set(k, v)

    return spark_conf


# Create a SparkSession
spark = SparkSession.builder \
    .config(conf=get_spark_config()) \
    .getOrCreate()

logger = Log4J(spark)

logger.info("Starting application")

# Read CSV file into DataFrame
df = spark.read.csv("data/concellos.csv", header=True, inferSchema=True)

# Show the first few rows of the DataFrame
df = df.transform(rename_cols).transform(get_rows_with_different_domains)

print(f"Numero de casos con distintos dominios: {df.count()}")

df.show(truncate=False)
# Stop the SparkSession
print("Press any key to continue...")
input()
print("Continuing execution...")
logger.info("Finishing application")

spark.stop()
