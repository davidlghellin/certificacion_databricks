"""Delta Lake OSS en local, fuera de Databricks.

    pip install pyspark delta-spark
    python demo_delta_local.py

No se llama `test_*.py` a propósito: pytest lo recogería como test y lo
ejecutaría al importarlo, con sus escrituras en /tmp y sin comprobar nada.
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("delta-local")
    # Explícito: así funciona igual con `python` que con `spark-submit`.
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = [(1, "David"), (2, "Ana")]

df = spark.createDataFrame(data, ["id", "nombre"])

df.write.format("delta").mode("overwrite").save("/tmp/clientes")

spark.read.format("delta").load("/tmp/clientes").show()