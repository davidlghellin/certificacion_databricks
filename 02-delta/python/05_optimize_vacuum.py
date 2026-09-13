# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Mantenimiento: OPTIMIZE, clustering, VACUUM (Python)
# MAGIC
# MAGIC El problema de fondo es el **small files problem**: muchas escrituras pequeñas
# MAGIC dejan miles de ficheros diminutos y leerlos cuesta más por el overhead de abrir
# MAGIC cada uno que por los datos.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

CATALOGO, ESQUEMA = "main", "demo_delta_py"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

TABLA = f"{CATALOGO}.{ESQUEMA}.metricas"


def n_ficheros(tabla=TABLA):
    d = spark.sql(f"DESCRIBE DETAIL {tabla}").first()
    return d["numFiles"], d["sizeInBytes"]

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {TABLA} (id INT, sensor STRING, valor DOUBLE)")

# 5 escrituras = 5 commits = al menos 5 ficheros
for i, s in enumerate(["a", "b", "a", "c", "b"], start=1):
    spark.createDataFrame([(i, s, float(i))], "id INT, sensor STRING, valor DOUBLE") \
        .write.mode("append").saveAsTable(TABLA)

print("antes de OPTIMIZE:", n_ficheros())

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE
# MAGIC
# MAGIC Compacta los ficheros pequeños en otros grandes. No cambia los datos, solo cómo
# MAGIC están repartidos: crea una versión nueva con operación `OPTIMIZE`.
# MAGIC
# MAGIC En Python hay dos caminos: `DeltaTable.optimize()` o `spark.sql("OPTIMIZE ...")`.

# COMMAND ----------

dt = DeltaTable.forName(spark, TABLA)

res = dt.optimize().executeCompaction()
display(res)          # devuelve las métricas de la compactación

print("después de OPTIMIZE:", n_ficheros())

# COMMAND ----------

# MAGIC %md
# MAGIC ### `.where()` en OPTIMIZE solo acepta columnas de PARTICIÓN
# MAGIC
# MAGIC `dt.optimize().where("sensor = 'a'")` sobre esta tabla falla con
# MAGIC `DELTA_NON_PARTITION_COLUMN_REFERENCE`: el predicado acota **qué particiones**
# MAGIC compactar, no filtra filas. Sobre una tabla particionada sí vale.

# COMMAND ----------

PART = f"{CATALOGO}.{ESQUEMA}.metricas_part"

spark.table(TABLA).write.mode("overwrite").option("overwriteSchema", "true") \
    .partitionBy("sensor").saveAsTable(PART)

DeltaTable.forName(spark, PART).optimize().where("sensor = 'a'").executeCompaction()
print("compactada solo la partición sensor='a'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data skipping
# MAGIC
# MAGIC Delta guarda **min/max de las primeras 32 columnas** de cada fichero en el log.
# MAGIC Al filtrar, se salta los ficheros cuyo rango no puede contener el valor. Funciona
# MAGIC solo, no hay que activarlo.
# MAGIC
# MAGIC Pero salta bien únicamente si los datos están **agrupados** por la columna que
# MAGIC filtras. Si están desperdigados, cada fichero abarca todo el rango y no se salta
# MAGIC nada. De eso van ZORDER y liquid clustering.

# COMMAND ----------

spark.sql(f"ALTER TABLE {TABLA} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '8')")

# Ver el skipping en acción: mira `files pruned` / `files read` en el plan
spark.table(TABLA).where("sensor = 'a'").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZORDER
# MAGIC
# MAGIC Reordena los datos para que valores parecidos caigan en los mismos ficheros.
# MAGIC
# MAGIC - Solo en columnas de **alta cardinalidad** que uses en filtros o joins.
# MAGIC - 1-2 columnas va bien; a partir de 3-4 se diluye.
# MAGIC - **No es incremental**: cada ejecución reordena de nuevo. Es caro.

# COMMAND ----------

dt.optimize().executeZOrderBy("sensor")

display(
    dt.history()
    .where(col("operation") == "OPTIMIZE")
    .select("version", "operationParameters", "operationMetrics")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Liquid clustering: lo que se usa hoy
# MAGIC
# MAGIC Sustituye a la vez al particionado y al ZORDER.
# MAGIC
# MAGIC | | Particionado / ZORDER | Liquid clustering |
# MAGIC |---|---|---|
# MAGIC | Cambiar columnas | reescribir la tabla | `ALTER TABLE ... CLUSTER BY` |
# MAGIC | Incremental | no | **sí** |
# MAGIC | Skew / alta cardinalidad | sufre | lo maneja |
# MAGIC
# MAGIC No se combinan: una tabla es particionada/zordered **o** clustered.
# MAGIC
# MAGIC Desde Python se declara con `.clusterBy(...)` en el writer, o en DDL.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOGO}.{ESQUEMA}.metricas_lc (
      id INT, sensor STRING, valor DOUBLE, ts TIMESTAMP
    )
    CLUSTER BY (sensor, ts)
""")

spark.sql(f"""
    INSERT INTO {CATALOGO}.{ESQUEMA}.metricas_lc VALUES
      (1, 'a', 1.0, TIMESTAMP'2026-01-01 00:00'),
      (2, 'b', 2.0, TIMESTAMP'2026-01-02 00:00')
""")

# El clustering se materializa al ejecutar OPTIMIZE
DeltaTable.forName(spark, f"{CATALOGO}.{ESQUEMA}.metricas_lc").optimize().executeCompaction()

display(spark.sql(f"DESCRIBE DETAIL {CATALOGO}.{ESQUEMA}.metricas_lc"))

# COMMAND ----------

# Cambiar las columnas de clustering: solo metadatos, no reescribe
spark.sql(f"ALTER TABLE {CATALOGO}.{ESQUEMA}.metricas_lc CLUSTER BY (sensor)")

# Y quitarlo
# spark.sql(f"ALTER TABLE {CATALOGO}.{ESQUEMA}.metricas_lc CLUSTER BY NONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deletion vectors
# MAGIC
# MAGIC Sin ellos, borrar una fila obliga a reescribir el fichero entero
# MAGIC (*copy-on-write*). Con ellos, Delta apunta aparte "en este fichero ignora las
# MAGIC filas 3 y 17" (*merge-on-read*) y el `DELETE` es casi instantáneo.
# MAGIC
# MAGIC El coste se paga en la lectura. El siguiente `OPTIMIZE` materializa los borrados.

# COMMAND ----------

spark.sql(f"ALTER TABLE {TABLA} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")

dt.delete("id = 3")

display(dt.history().where(col("operation") == "DELETE").select("version", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM
# MAGIC
# MAGIC `OPTIMIZE` y `DELETE` **no liberan espacio**: los ficheros viejos siguen ahí para
# MAGIC el time travel. `VACUUM` es lo que los borra de verdad.
# MAGIC
# MAGIC - Retención por defecto: **7 días (168 h)**.
# MAGIC - Bajar de 7 días está bloqueado: un lector o un stream en curso podría quedarse
# MAGIC   sin ficheros a media consulta.
# MAGIC - **Destruye el time travel** a las versiones afectadas. Irreversible.
# MAGIC - No toca el `_delta_log`: eso es `delta.logRetentionDuration`.

# COMMAND ----------

# DRY RUN primero, siempre: enseña qué borraría sin borrar
display(dt.vacuum(retentionHours=168))   # sin dryRun, DeltaTable.vacuum ya ejecuta

# Equivalente SQL con dry run explícito:
display(spark.sql(f"VACUUM {TABLA} RETAIN 168 HOURS DRY RUN"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Forzar retención 0 (solo en demos)
# MAGIC
# MAGIC En una tabla real esto es una forma excelente de romper un stream en producción.

# COMMAND ----------

# Esta config puede estar bloqueada en serverless, igual que la de autoMerge.
try:
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    dt.vacuum(retentionHours=0)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    print("VACUUM con retención 0 ejecutado")
except Exception as e:
    print("No se ha podido forzar retención 0 (normal en serverless):")
    print(str(e)[:250])

# COMMAND ----------

# El historial sigue listando las versiones antiguas...
display(dt.history().select("version", "operation").orderBy("version"))

# COMMAND ----------

# ...pero leerlas ya falla: los ficheros no están.
try:
    spark.read.option("versionAsOf", 1).table(TABLA).show()
except Exception as e:
    print("Time travel roto por el VACUUM, como se esperaba:")
    print(str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predictive optimization
# MAGIC
# MAGIC Databricks ejecuta `OPTIMIZE` y `VACUUM` por su cuenta cuando compensa, solo en
# MAGIC tablas **managed** de Unity Catalog. Si está activo, no programes tus propios
# MAGIC jobs de mantenimiento: duplicarías trabajo.

# COMMAND ----------

# spark.sql("ALTER CATALOG main ENABLE PREDICTIVE OPTIMIZATION")
# spark.sql("ALTER SCHEMA demo_delta_py ENABLE PREDICTIVE OPTIMIZATION")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrituras auto-optimizadas
# MAGIC
# MAGIC | Propiedad | Cuándo actúa |
# MAGIC |---|---|
# MAGIC | `delta.autoOptimize.optimizeWrite` | **antes** de escribir: reparticiona para no generar ficheros diminutos |
# MAGIC | `delta.autoOptimize.autoCompact` | **después**: si quedaron muchos pequeños, los compacta |

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {TABLA} SET TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

display(spark.sql(f"SHOW TBLPROPERTIES {TABLA}"))
