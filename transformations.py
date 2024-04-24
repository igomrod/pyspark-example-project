def rename_cols(df):
    return (df
            .withColumnRenamed('C�DIGO POSTAL', 'CÓDIGO_POSTAL')
            .withColumnRenamed('TEL�FONO', 'TELÉFONO')
            .withColumnRenamed('CORREO ELECTR�NICO', 'MAIL')
            .withColumnRenamed('PORTAL WEB', 'WEB'))