from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


def normalizar(df: DataFrame) -> DataFrame:
    return df.select(upper(col("nombre")).alias("nombre"), col("valor"))


def contar_filas(df: DataFrame) -> int:
    return df.count()