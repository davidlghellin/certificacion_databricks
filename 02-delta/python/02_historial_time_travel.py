# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Historial, time travel y RESTORE (Python)
# MAGIC
# MAGIC Cada escritura crea una **versión** (0, 1, 2…). Como los ficheros viejos no se
# MAGIC borran al momento, puedes leer el pasado.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

CATALOGO, ESQUEMA = "main", "demo_delta_py"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

TABLA = f"{CATALOGO}.{ESQUEMA}.inventario"

# COMMAND ----------

# Se borra antes de crear: `mode("overwrite")` sobre una tabla existente NO
# reinicia el historial, añade una versión más. En la segunda ejecución los
# `versionAsOf` fijos de abajo apuntarían a la ejecución anterior.
spark.sql(f"DROP TABLE IF EXISTS {TABLA}")

# v0: crear
spark.createDataFrame([("A", 10), ("B", 20)], "sku STRING, stock INT") \
    .write.saveAsTable(TABLA)

dt = DeltaTable.forName(spark, TABLA)
dt.update("sku = 'A'", {"stock": "5"})                                    # v1
dt.delete("sku = 'B'")                                                    # v2
spark.createDataFrame([("C", 30)], "sku STRING, stock INT") \
    .write.mode("append").saveAsTable(TABLA)                              # v3

display(spark.table(TABLA))

# COMMAND ----------

# MAGIC %md
# MAGIC ## El historial
# MAGIC
# MAGIC `dt.history()` devuelve un DataFrame, así que se filtra y agrega como cualquier
# MAGIC otro. Columnas clave: `version`, `timestamp`, `operation`, `operationMetrics`,
# MAGIC `userName`, `isBlindAppend`.

# COMMAND ----------

display(dt.history())

# COMMAND ----------

display(
    dt.history()
    .select("version", "timestamp", "operation", "operationMetrics")
    .orderBy("version")
)

# COMMAND ----------

# Solo las N últimas
display(dt.history(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel
# MAGIC
# MAGIC | Forma | Sintaxis |
# MAGIC |---|---|
# MAGIC | Por versión | `.option("versionAsOf", 2)` |
# MAGIC | Por timestamp | `.option("timestampAsOf", "2026-09-01 10:00:00")` |
# MAGIC | SQL desde Python | `spark.sql("SELECT * FROM t VERSION AS OF 2")` |
# MAGIC
# MAGIC Por versión es determinista. Por timestamp devuelve **la última versión anterior
# MAGIC o igual** a ese instante; pedir un momento previo a la creación es error.

# COMMAND ----------

for v in [0, 1, 2, 3]:
    df = spark.read.option("versionAsOf", v).table(TABLA)
    print(f"--- versión {v} ---")
    df.show()

# COMMAND ----------

ts_v1 = dt.history().where("version = 1").first()["timestamp"]
print("timestamp de la v1:", ts_v1)

display(spark.read.option("timestampAsOf", ts_v1).table(TABLA))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diff entre dos versiones
# MAGIC
# MAGIC Sin CDF: dos `exceptAll` en las dos direcciones.

# COMMAND ----------

antes = spark.read.option("versionAsOf", 1).table(TABLA)
ahora = spark.table(TABLA)

print("Filas que estaban y ya no:")
antes.exceptAll(ahora).show()

print("Filas nuevas:")
ahora.exceptAll(antes).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## RESTORE
# MAGIC
# MAGIC **No borra historia**: crea una versión nueva con el contenido de la antigua.
# MAGIC Siempre puedes deshacer el deshacer.

# COMMAND ----------

dt.restoreToVersion(1)
display(spark.table(TABLA))

# COMMAND ----------

# También por timestamp
# dt.restoreToTimestamp("2026-09-01 10:00:00")

# COMMAND ----------

# La v1 sigue ahí, y ahora hay una versión nueva con operation = RESTORE
display(dt.history().select("version", "operation").orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuánto dura el time travel
# MAGIC
# MAGIC | Propiedad | Controla | Defecto |
# MAGIC |---|---|---|
# MAGIC | `delta.logRetentionDuration` | cuánto se guardan los **commits** | 30 días |
# MAGIC | `delta.deletedFileRetentionDuration` | cuánto sobreviven los **ficheros** | 7 días |
# MAGIC
# MAGIC El alcance real es el **mínimo de los dos**. Con los defectos, un `VACUUM` se
# MAGIC lleva los ficheros de más de 7 días y las versiones antiguas dejan de ser
# MAGIC legibles aunque el log siga listándolas.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {TABLA} SET TBLPROPERTIES (
      'delta.logRetentionDuration'         = 'interval 90 days',
      'delta.deletedFileRetentionDuration' = 'interval 30 days'
    )
""")

display(spark.sql(f"SHOW TBLPROPERTIES {TABLA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Un uso muy práctico: auditar quién tocó qué
# MAGIC
# MAGIC El historial guarda el usuario y el job de cada commit.

# COMMAND ----------

display(
    dt.history()
    .select("version", "timestamp", "userName", "operation", "job", "notebook")
    .orderBy(col("version").desc())
)
