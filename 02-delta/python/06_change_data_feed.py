# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Change Data Feed (Python)
# MAGIC
# MAGIC El time travel te da **fotos** de la tabla. CDF te da el **vídeo**: qué filas
# MAGIC concretas se insertaron, borraron o cambiaron, y en qué versión.
# MAGIC
# MAGIC Sirve para propagar cambios bronze → silver → gold sin recalcularlo todo.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

CATALOGO, ESQUEMA = "main", "demo_delta_py"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

TABLA = f"{CATALOGO}.{ESQUEMA}.cuentas"

# COMMAND ----------

# Activar al crear...
spark.sql(f"""
    CREATE OR REPLACE TABLE {TABLA} (id INT, titular STRING, saldo DOUBLE)
    TBLPROPERTIES ('delta.enableChangeDataFeed' = true)
""")

# ...o después:
# spark.sql(f"ALTER TABLE {TABLA} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true)")

# Y a nivel de sesión, para que TODAS las tablas nuevas lo lleven:
# spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC **Trampa**: CDF solo registra cambios **a partir del momento en que lo activas**.
# MAGIC Pedir el feed de versiones anteriores es un error, no una lista vacía.

# COMMAND ----------

dt = DeltaTable.forName(spark, TABLA)

spark.createDataFrame([(1, "Ana", 1000.0), (2, "Bruno", 500.0)],
                      "id INT, titular STRING, saldo DOUBLE") \
    .write.mode("append").saveAsTable(TABLA)          # v1

dt.update("id = 1", {"saldo": "1200.0"})              # v2
dt.delete("id = 2")                                   # v3

spark.createDataFrame([(3, "Carla", 300.0)], "id INT, titular STRING, saldo DOUBLE") \
    .write.mode("append").saveAsTable(TABLA)          # v4

display(spark.table(TABLA).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer el feed
# MAGIC
# MAGIC Se lee con `.option("readChangeFeed", "true")` más un rango de versiones o
# MAGIC timestamps. Devuelve las columnas de la tabla más tres:
# MAGIC
# MAGIC | Columna | Contenido |
# MAGIC |---|---|
# MAGIC | `_change_type` | `insert`, `delete`, `update_preimage`, `update_postimage` |
# MAGIC | `_commit_version` | versión en la que ocurrió |
# MAGIC | `_commit_timestamp` | cuándo |
# MAGIC
# MAGIC Un `UPDATE` genera **dos** filas: cómo estaba y cómo quedó.

# COMMAND ----------

cdf = (spark.read
       .option("readChangeFeed", "true")
       .option("startingVersion", 1)
       .table(TABLA))

display(cdf.orderBy("_commit_version", "id"))

# COMMAND ----------

# Rango cerrado
display(
    spark.read
    .option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .option("endingVersion", 3)
    .table(TABLA)
    .orderBy("_commit_version")
)

# COMMAND ----------

# Por timestamp
ts = dt.history().where("version = 2").first()["timestamp"]

display(
    spark.read
    .option("readChangeFeed", "true")
    .option("startingTimestamp", str(ts))
    .table(TABLA)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Neto de cambios por clave
# MAGIC
# MAGIC Si solo quieres el estado final de cada id en un rango: descarta los `preimage` y
# MAGIC quédate con el último evento de cada clave.

# COMMAND ----------

w = Window.partitionBy("id").orderBy(col("_commit_version").desc())

neto = (
    cdf.where(col("_change_type") != "update_preimage")
    .select(
        col("id"), col("titular"), col("saldo"),
        col("_change_type"), col("_commit_version"),
        row_number().over(w).alias("rn"),
    )
    .where("rn = 1")
    .select("id", "titular", "saldo", "_change_type", "_commit_version")
)

display(neto.orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Propagar a silver
# MAGIC
# MAGIC El patrón real: leo el feed desde la última versión que procesé y lo aplico con
# MAGIC un `MERGE`. Los `delete` se traducen en `DELETE`, el resto en upsert.

# COMMAND ----------

SILVER = f"{CATALOGO}.{ESQUEMA}.cuentas_silver"

spark.table(TABLA).limit(0).write.mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(SILVER)

(DeltaTable.forName(spark, SILVER).alias("t")
    .merge(neto.alias("s"), "t.id = s.id")
    .whenMatchedDelete(condition="s._change_type = 'delete'")
    .whenMatchedUpdate(set={"titular": "s.titular", "saldo": "s.saldo"})
    .whenNotMatchedInsert(
        condition="s._change_type != 'delete'",
        values={"id": "s.id", "titular": "s.titular", "saldo": "s.saldo"},
    )
    .execute())

display(spark.table(SILVER).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer el feed en streaming
# MAGIC
# MAGIC Lo mismo con `readStream`: el checkpoint recuerda por qué versión ibas, así que
# MAGIC no hay que llevar la cuenta a mano. Es la forma de producción.

# COMMAND ----------

# (spark.readStream
#    .option("readChangeFeed", "true")
#    .option("startingVersion", 1)
#    .table(TABLA)
#    .writeStream
#    .option("checkpointLocation", "/Volumes/main/demo_delta_py/chk/cdf")
#    .trigger(availableNow=True)
#    .foreachBatch(aplicar_merge)     # el MERGE de arriba, por micro-batch
#    .start())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué NO genera CDF
# MAGIC
# MAGIC Las operaciones que solo mueven ficheros sin cambiar datos no aparecen:
# MAGIC `OPTIMIZE`, `VACUUM`, `ZORDER`. Un `INSERT` puro es *blind append* y Delta puede
# MAGIC resolverlo desde el log sin escribir ficheros de CDF aparte.
# MAGIC
# MAGIC Los ficheros de CDF viven en `_change_data/` y **el `VACUUM` también se los
# MAGIC lleva**: el feed caduca igual que el time travel.

# COMMAND ----------

dt.optimize().executeCompaction()

display(
    spark.read.option("readChangeFeed", "true").option("startingVersion", 1).table(TABLA)
    .groupBy("_commit_version", "_change_type").count()
    .orderBy("_commit_version")
)

# COMMAND ----------

# Los ficheros de CDF viven en `_change_data/`, pero en una tabla MANAGED de Unity
# Catalog no se pueden listar: `location` viene vacío. Solo se puede curiosear en
# tablas por ruta (ver notebook 00).
loc = spark.sql(f"DESCRIBE DETAIL {TABLA}").first()["location"]
if not loc:
    print("Tabla managed de UC: el almacenamiento no es inspeccionable, y es correcto.")
else:
    display(dbutils.fs.ls(loc + "/_change_data"))
