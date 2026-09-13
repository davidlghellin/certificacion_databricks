# Databricks notebook source
# Una iteracion del for_each. Escribe una fila por item.
#
# Cada iteracion es una task-run independiente. Se usa MERGE en vez de INSERT
# para que el resultado sea idempotente si el job se reintenta.
#
# El bucle corre con `concurrency: 1` (resources/demo_yml.job.yml): dos MERGE a
# la vez sobre esta misma tabla chocarian por concurrencia optimista
# (ConcurrentAppendException). Idempotente no significa libre de conflictos.
from pyspark.sql.functions import current_timestamp, lit

dbutils.widgets.text("item", "?")
dbutils.widgets.text("tabla", "main.demo_yml.eventos")

item = dbutils.widgets.get("item")
tabla = dbutils.widgets.get("tabla")

print(f"cargando {item!r} en {tabla}")

nuevo = (
    spark.range(1)
    .select(
        lit(item).alias("item"),
        lit(len(item)).cast("int").alias("n"),
        current_timestamp().alias("ts"),
    )
)
nuevo.createOrReplaceTempView("nuevo")

spark.sql(f"""
    MERGE INTO {tabla} t
    USING nuevo s ON t.item = s.item
    WHEN MATCHED THEN UPDATE SET t.n = s.n, t.ts = s.ts
    WHEN NOT MATCHED THEN INSERT *
""")

dbutils.notebook.exit(f"ok:{item}")
