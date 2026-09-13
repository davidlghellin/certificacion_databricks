# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Clones (Python)
# MAGIC
# MAGIC Copiar una tabla sin copiar los datos (o copiándolos, tú eliges).
# MAGIC
# MAGIC | | SHALLOW | DEEP |
# MAGIC |---|---|---|
# MAGIC | Qué copia | solo el `_delta_log` | log **y** ficheros |
# MAGIC | Velocidad | instantáneo | lo que tarde copiar |
# MAGIC | Espacio | ~0 | el de la tabla |
# MAGIC | Depende del origen | **sí** | no |
# MAGIC | `VACUUM` en el origen | **puede romperlo** | no le afecta |
# MAGIC | Para qué | pruebas de corta vida | backup, migración |
# MAGIC
# MAGIC En los dos casos, escribir en el clon **no** toca al original.
# MAGIC
# MAGIC No hay API de DataFrames para clonar: es DDL, vía `spark.sql`.

# COMMAND ----------

from delta.tables import DeltaTable

CATALOGO, ESQUEMA = "main", "demo_delta_py"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

spark.createDataFrame([(1, "uno"), (2, "dos"), (3, "tres")], "id INT, valor STRING") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("origen")

display(spark.table("origen").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SHALLOW CLONE
# MAGIC
# MAGIC Caso de uso típico: probar un `MERGE` destructivo contra datos de producción sin
# MAGIC tocar producción ni esperar a copiar 2 TB.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE origen_shallow SHALLOW CLONE origen")
display(spark.table("origen_shallow").orderBy("id"))

# COMMAND ----------

# `numFiles` cuenta ficheros que en realidad viven en el directorio del ORIGEN
display(spark.sql("DESCRIBE DETAIL origen_shallow"))

# COMMAND ----------

# Escribo en el clon: el original ni se entera
DeltaTable.forName(spark, "origen_shallow").delete("id = 2")

print("clon  :", spark.table("origen_shallow").count())
print("origen:", spark.table("origen").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### El peligro del shallow clone
# MAGIC
# MAGIC El clon apunta a ficheros del origen. Si alguien hace `VACUUM` en el origen y se
# MAGIC los lleva, el clon **queda roto**. Por eso son para vidas cortas, no para backup.

# COMMAND ----------

# MAGIC %md
# MAGIC ## DEEP CLONE
# MAGIC
# MAGIC Copia completa e independiente del **estado actual**: datos y metadatos. La opción
# MAGIC para backups o para mover una tabla a otro catálogo o región.
# MAGIC
# MAGIC Ojo: **no copia el historial**. El clon arranca uno nuevo (versión 0 = `CLONE`).

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE origen_deep DEEP CLONE origen")

display(spark.sql("DESCRIBE DETAIL origen_deep"))   # ficheros propios, location propio

# COMMAND ----------

# MAGIC %md
# MAGIC ## El deep clone es incremental
# MAGIC
# MAGIC Repetirlo **no** vuelve a copiarlo todo: solo trae lo que cambió. Eso lo hace una
# MAGIC estrategia de backup razonable, programado cada noche.

# COMMAND ----------

spark.createDataFrame([(4, "cuatro"), (5, "cinco")], "id INT, valor STRING") \
    .write.mode("append").saveAsTable("origen")

# Segunda pasada: solo copia las filas nuevas
spark.sql("CREATE OR REPLACE TABLE origen_deep DEEP CLONE origen")

display(spark.table("origen_deep").orderBy("id"))

# COMMAND ----------

display(
    DeltaTable.forName(spark, "origen_deep").history()
    .select("version", "operation", "operationMetrics")
    .orderBy("version")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clonar una versión concreta
# MAGIC
# MAGIC Se combina con time travel. Muy útil para reproducir un bug: "dame la tabla tal y
# MAGIC como estaba el martes".

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE origen_v0 SHALLOW CLONE origen VERSION AS OF 0")
display(spark.table("origen_v0").orderBy("id"))

# COMMAND ----------

# spark.sql("CREATE OR REPLACE TABLE x DEEP CLONE origen TIMESTAMP AS OF '2026-09-01T00:00:00'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clonar por ruta
# MAGIC
# MAGIC Si trabajas con rutas en vez de nombres de tabla (Delta OSS o Volumes):

# COMMAND ----------

# spark.sql("""
#     CREATE OR REPLACE TABLE destino
#     DEEP CLONE delta.`/Volumes/main/demo/datos/origen`
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué se lleva el clon y qué no
# MAGIC
# MAGIC **Sí**: los datos del estado actual, esquema, particionado/clustering,
# MAGIC propiedades (`TBLPROPERTIES`), constraints y comentarios.
# MAGIC
# MAGIC **No**:
# MAGIC - **El historial del origen.** El clon empieza el suyo propio, con una única
# MAGIC   versión 0 cuya operación es `CLONE`. Viajar en el clon a una versión antigua
# MAGIC   del origen falla (verificado):
# MAGIC   `DELTA_VERSION_NOT_FOUND: Cannot time travel Delta table to version 1. Available versions: [0, 0]`.
# MAGIC   Si necesitas una foto del pasado, clona **esa versión**: `DEEP CLONE t VERSION AS OF 3`.
# MAGIC - Los permisos de Unity Catalog. El clon es un objeto nuevo y los `GRANT`
# MAGIC   hay que rehacerlos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `CREATE OR REPLACE` vs `IF NOT EXISTS`
# MAGIC
# MAGIC - `CREATE OR REPLACE ... DEEP CLONE` → sincroniza (el patrón de backup).
# MAGIC - `CREATE TABLE IF NOT EXISTS ... DEEP CLONE` → solo la primera vez.

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS origen_deep DEEP CLONE origen")   # no-op
print("filas:", spark.table("origen_deep").count())
