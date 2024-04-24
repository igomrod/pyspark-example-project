from pyspark.sql import SparkSession

from transformations import rename_cols


def test_rename_transformation():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Example App") \
        .getOrCreate()

    # Read CSV file into DataFrame
    df = spark.read.csv("data/concellos.csv", header=True, inferSchema=True)

    renamed_columns = df.transform(rename_cols)

    assert renamed_columns.columns == \
           ['CONCELLO',
            'PROVINCIA',
            'ENDEREZO',
            'CÓDIGO_POSTAL',
            'TELÉFONO',
            'FAX',
            'MAIL',
            'WEB',
            'LATITUD',
            'LONGITUD']


