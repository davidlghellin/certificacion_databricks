# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · DML y MERGE (Python)
# MAGIC
# MAGIC En Python el `UPDATE`/`DELETE`/`MERGE` van por la clase **`DeltaTable`**, no por
# MAGIC el DataFrame. Es la diferencia principal con la versión SQL.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, row_number
from pyspark.sql.window import Window

CATALOGO, ESQUEMA = "main", "demo_delta_py"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

# COMMAND ----------

clientes = spark.createDataFrame(
    [(1, "Ana", "Madrid", True), (2, "Bruno", "Sevilla", True), (3, "Carla", "Bilbao", True)],
    "id INT, nombre STRING, ciudad STRING, activo BOOLEAN",
)
clientes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("clientes")

display(spark.table("clientes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coger la referencia a la tabla
# MAGIC
# MAGIC | Constructor | Cuándo |
# MAGIC |---|---|
# MAGIC | `DeltaTable.forName(spark, "cat.esq.tabla")` | tabla registrada en el metastore |
# MAGIC | `DeltaTable.forPath(spark, "/ruta")` | tabla por ruta (Delta OSS o Volume) |
# MAGIC | `DeltaTable.isDeltaTable(spark, "/ruta")` | comprobar si una ruta es Delta |

# COMMAND ----------

dt = DeltaTable.forName(spark, f"{CATALOGO}.{ESQUEMA}.clientes")
display(dt.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPDATE y DELETE
# MAGIC
# MAGIC Dos formas de expresar la condición: string SQL o `Column` de PySpark. Las dos
# MAGIC valen; el string es más corto, la columna se compone mejor en código.

# COMMAND ----------

# Con string SQL
dt.update(condition="id = 1", set={"ciudad": "'Barcelona'"})

# Con expresiones de PySpark (ojo: los valores literales van con lit())
dt.update(condition=col("id") == 2, set={"activo": lit(True)})

dt.delete("id = 3")

display(dt.toDF().orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Cuidado con las comillas**: en el `set={}` con strings, el valor es una
# MAGIC *expresión SQL*. `{"ciudad": "Barcelona"}` buscaría una **columna** llamada
# MAGIC Barcelona y fallaría. Tiene que ser `"'Barcelona'"` o `lit("Barcelona")`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE: los tres universos
# MAGIC
# MAGIC | Cláusula Python | Qué filas coge |
# MAGIC |---|---|
# MAGIC | `whenMatchedUpdate` / `whenMatchedDelete` | están en los dos |
# MAGIC | `whenNotMatchedInsert` | solo en el origen |
# MAGIC | `whenNotMatchedBySourceUpdate` / `...Delete` | solo en el destino |
# MAGIC
# MAGIC Se evalúan **en orden**: gana la primera cuya condición casa. Si dos filas del
# MAGIC origen casan con la misma fila destino, el `MERGE` falla: deduplica antes.

# COMMAND ----------

cambios = spark.createDataFrame(
    [(1, "Ana", "Valencia", "UPDATE"), (2, None, None, "DELETE"), (4, "Diego", "Malaga", "INSERT")],
    "id INT, nombre STRING, ciudad STRING, op STRING",
)

(dt.alias("t")
    .merge(cambios.alias("s"), "t.id = s.id")
    .whenMatchedDelete(condition="s.op = 'DELETE'")
    .whenMatchedUpdate(condition="s.op = 'UPDATE'", set={"ciudad": "s.ciudad"})
    .whenNotMatchedInsert(
        condition="s.op = 'INSERT'",
        values={"id": "s.id", "nombre": "s.nombre", "ciudad": "s.ciudad", "activo": "true"},
    )
    # Lo que ya no llega en el origen: baja lógica
    .whenNotMatchedBySourceUpdate(set={"activo": "false"})
    .execute())

display(dt.toDF().orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Métricas del MERGE
# MAGIC
# MAGIC Salen en el historial, no del `.execute()` (que no devuelve nada).

# COMMAND ----------

display(
    dt.history()
    .where(col("operation") == "MERGE")
    .select("version", "operation", "operationMetrics")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `updateAll` / `insertAll`
# MAGIC
# MAGIC El equivalente a `UPDATE SET *` e `INSERT *`: casan columnas por nombre. Cómodo,
# MAGIC pero frágil si los esquemas divergen.

# COMMAND ----------

spark.table("clientes").select("id", "nombre", "ciudad").write \
    .mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_scd1")

nuevos = spark.createDataFrame(
    [(1, "Ana", "Sevilla"), (9, "Elena", "Vigo")], "id INT, nombre STRING, ciudad STRING"
)

(DeltaTable.forName(spark, "dim_scd1").alias("t")
    .merge(nuevos.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

display(spark.table("dim_scd1").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicar antes del MERGE
# MAGIC
# MAGIC Si el origen trae dos versiones de la misma clave, el `MERGE` revienta con
# MAGIC *"multiple source rows matched"*. La ventana con `row_number()` es el patrón.

# COMMAND ----------

sucio = spark.createDataFrame(
    [(1, "Ana", "Madrid", "2026-06-01 10:00"), (1, "Ana", "Valencia", "2026-06-01 12:00")],
    "id INT, nombre STRING, ciudad STRING, ts STRING",
)

w = Window.partitionBy("id").orderBy(col("ts").desc())
limpio = (
    sucio.select("*", row_number().over(w).alias("rn"))
    .where("rn = 1")
    .select(sucio.columns)          # todas las originales; solo se va `rn`
)

display(limpio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotencia
# MAGIC
# MAGIC Un `MERGE` bien escrito se puede repetir sin duplicar. Ejecuta esta celda dos
# MAGIC veces: el conteo no cambia. Un `append` sí duplicaría.

# COMMAND ----------

fran = spark.createDataFrame([(5, "Fran", "Oviedo", True)], "id INT, nombre STRING, ciudad STRING, activo BOOLEAN")

(dt.alias("t")
    .merge(fran.alias("s"), "t.id = s.id")
    .whenNotMatchedInsertAll()
    .execute())

print("filas =", spark.table("clientes").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrituras idempotentes en batch
# MAGIC
# MAGIC Alternativa cuando no puedes usar `MERGE`: etiquetas el lote y Delta descarta el
# MAGIC que ya aplicó. Ejecuta dos veces y comprueba que no duplica.

# COMMAND ----------

lote = spark.createDataFrame([(7, "Gema", "Leon", True)], "id INT, nombre STRING, ciudad STRING, activo BOOLEAN")

(lote.write
    .format("delta")
    .option("txnAppId", "carga_diaria_clientes")
    .option("txnVersion", 1)          # el mismo id de lote no se aplica dos veces
    .mode("append")
    .saveAsTable("clientes"))

print("filas =", spark.table("clientes").count())
