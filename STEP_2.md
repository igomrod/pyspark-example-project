# Añadiendo un test

- Crear fichero `test_concellos.py` con el siguiente contenido
```python
import pytest

def test_load_data():
    assert True
```

- Instalar pytest
- Configurar proyecto para usar pytest en el IDE
- Hagamos un test que use spark para leer el fichero y compruebe que las columnas son las correctas
```python
from pyspark.sql import SparkSession


def test_load_data():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Example App") \
        .getOrCreate()

    # Read CSV file into DataFrame
    df = spark.read.csv("data/concellos.csv", header=True, inferSchema=True)

    assert df.columns == \
           ['CONCELLO',
            'PROVINCIA',
            'ENDEREZO',
            'C�DIGO POSTAL',
            'TEL�FONO',
            'FAX',
            'CORREO ELECTR�NICO',
            'PORTAL WEB',
            'LATITUD',
            'LONGITUD']
```
