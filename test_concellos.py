from pyspark.sql import SparkSession

from transformations import rename_cols, get_rows_with_different_domains

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Example App") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("data/concellos.csv", header=True, inferSchema=True)


def test_rename_transformation():
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


def test_get_different_domains_transformation():
    renamed_columns = df.transform(rename_cols)

    df_rows_with_different_domains = renamed_columns.transform(get_rows_with_different_domains)

    assert 'web_domain' in df_rows_with_different_domains.columns
    assert 'mail_domain' in df_rows_with_different_domains.columns

    for row in df_rows_with_different_domains.collect():
        assert row['web_domain'] != row['mail_domain']




