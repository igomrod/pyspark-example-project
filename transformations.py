from pyspark.sql.functions import element_at, regexp_replace, split, col


def rename_cols(df):
    return (df
            .withColumnRenamed('C�DIGO POSTAL', 'CÓDIGO_POSTAL')
            .withColumnRenamed('TEL�FONO', 'TELÉFONO')
            .withColumnRenamed('CORREO ELECTR�NICO', 'MAIL')
            .withColumnRenamed('PORTAL WEB', 'WEB'))


def add_web_domain_col(renamed_columns):
    return (renamed_columns
            .withColumn('web_domain', element_at(split(col('WEB'), '\.'), -1))
            .withColumn('web_domain', regexp_replace(col('web_domain'), '/', '')))


def add_mail_domain_col(df):
    return df.withColumn('mail_domain', element_at(split(col('MAIL'), '\.'), -1))


def get_rows_with_different_domains(renamed_columns):
    return (renamed_columns
            .transform(add_web_domain_col)
            .transform(add_mail_domain_col)
            .where("web_domain != mail_domain"))