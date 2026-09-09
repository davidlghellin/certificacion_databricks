from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


def normalizar(df: DataFrame) -> DataFrame:
    return df.withColumn("nombre", upper(col("nombre")))


def contar_filas(df: DataFrame) -> int:
    return df.count()