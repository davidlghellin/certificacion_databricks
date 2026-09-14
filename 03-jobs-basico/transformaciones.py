from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


def normalizar(df: DataFrame) -> DataFrame:
    # `select` explícito, pero construido desde `df.columns`: se sustituye solo
    # `nombre` y el resto de columnas pasan intactas y en su orden. Enumerarlas a
    # mano rompería el contrato de la función, que tiene que servir para
    # cualquier DataFrame que traiga `nombre`, tenga las columnas que tenga.
    return df.select([
        upper(col(c)).alias(c) if c == "nombre" else col(c)
        for c in df.columns
    ])


def contar_filas(df: DataFrame) -> int:
    return df.count()