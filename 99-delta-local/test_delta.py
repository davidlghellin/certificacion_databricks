from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("delta-test")
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