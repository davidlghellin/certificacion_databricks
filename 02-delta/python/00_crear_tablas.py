# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Crear tablas Delta (Python)
# MAGIC
# MAGIC Mismo temario que `sql/00_crear_tablas.sql`, con la API de DataFrames.
# MAGIC
# MAGIC **Qué es una tabla Delta**: un directorio de Parquet + una carpeta `_delta_log/`
# MAGIC con el registro de transacciones. Delta es el formato por defecto en Databricks.

# COMMAND ----------

CATALOGO = "main"        # en Free Edition suele ser "workspace"
ESQUEMA = "demo_delta_py"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

print(f"Trabajando en {CATALOGO}.{ESQUEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear con la API de DataFrames
# MAGIC
# MAGIC | `mode` | Qué hace si la tabla existe |
# MAGIC |---|---|
# MAGIC | `append` | añade filas |
# MAGIC | `overwrite` | sustituye los datos (el esquema, solo con `overwriteSchema`) |
# MAGIC | `error` (defecto) | falla |
# MAGIC | `ignore` | no hace nada |

# COMMAND ----------

from pyspark.sql.functions import col, lit, to_date

datos = [
    (1, "teclado", 49.90, "ES", "2026-01-10"),
    (2, "raton", 19.90, "ES", "2026-01-11"),
    (3, "monitor", 199.00, "PT", "2026-01-11"),
    (4, "teclado", 49.90, "FR", "2026-01-12"),
]

ventas = (
    spark.createDataFrame(datos, "id INT, producto STRING, importe DOUBLE, pais STRING, fecha STRING")
    # `select` explícito: el esquema de salida se lee entero aquí, en un sitio.
    .select(
        col("id"),
        col("producto"),
        col("importe"),
        col("pais"),
        to_date(col("fecha")).alias("fecha"),
    )
)

# `format("delta")` es redundante en Databricks, pero explícito se lee mejor.
(ventas.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ventas"))

display(spark.table("ventas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## `saveAsTable` vs `save`
# MAGIC
# MAGIC | | `saveAsTable("nombre")` | `save("/ruta")` |
# MAGIC |---|---|---|
# MAGIC | Registra en el metastore | **sí** | no |
# MAGIC | Se consulta con `spark.table(...)` / SQL | sí | solo por ruta |
# MAGIC | Gobernanza de Unity Catalog | sí | no |
# MAGIC
# MAGIC En Databricks moderno se usa `saveAsTable`. El `save()` a una ruta es el estilo
# MAGIC "Delta OSS" y en UC solo tiene sentido dentro de un Volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear con DDL explícito
# MAGIC
# MAGIC Cuando quieres fijar tipos exactos (`DECIMAL(10,2)`), comentarios o propiedades,
# MAGIC el DDL es más cómodo que el DataFrame.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE ventas_ddl (
      id        INT,
      producto  STRING,
      importe   DECIMAL(10,2),
      pais      STRING,
      fecha     DATE
    )
    COMMENT 'Tabla con tipos declarados a mano'
    TBLPROPERTIES ('quality' = 'bronze')
""")

spark.table("ventas").write.mode("append").saveAsTable("ventas_ddl")
display(spark.table("ventas_ddl"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspeccionar

# COMMAND ----------

display(spark.sql("DESCRIBE TABLE EXTENDED ventas"))

# COMMAND ----------

detalle = spark.sql("DESCRIBE DETAIL ventas")
display(detalle)

# Los campos que más se miran
d = detalle.first()
print(f"formato          = {d['format']}")
print(f"ubicacion        = {d['location']!r}   <- vacía: es una MANAGED de UC")
print(f"numFiles         = {d['numFiles']}")
print(f"sizeInBytes      = {d['sizeInBytes']}")
print(f"minReaderVersion = {d['minReaderVersion']}")
print(f"minWriterVersion = {d['minWriterVersion']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## El transaction log, por dentro
# MAGIC
# MAGIC Cada `.json` del `_delta_log/` es una lista de acciones:
# MAGIC
# MAGIC | Acción | Significa |
# MAGIC |---|---|
# MAGIC | `add` | fichero que pasa a formar parte de la tabla |
# MAGIC | `remove` | fichero que deja de contar (sigue en disco) |
# MAGIC | `metaData` | esquema, particionado, propiedades |
# MAGIC | `protocol` | versiones mínimas de lector/escritor |
# MAGIC | `commitInfo` | quién, cuándo, qué operación, métricas |
# MAGIC
# MAGIC Lo esencial: **un fichero de datos nunca se modifica**. Un `UPDATE` escribe
# MAGIC ficheros nuevos y marca los viejos como `remove`. De ahí salen el time travel y
# MAGIC la necesidad de `VACUUM`.
# MAGIC
# MAGIC **Pero**: en una tabla **managed** de Unity Catalog no puedes mirar los ficheros.
# MAGIC `DESCRIBE DETAIL` devuelve el `location` vacío y `dbutils.fs.ls` falla. UC oculta
# MAGIC el almacenamiento de las managed a propósito.
# MAGIC
# MAGIC Para verlo por dentro necesitamos una tabla cuya ruta controlemos: la creamos en
# MAGIC un **Volume**.

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS datos")

RUTA = f"/Volumes/{CATALOGO}/{ESQUEMA}/datos/ventas_raw"

# Tabla Delta "por ruta": mismo formato, pero los ficheros sí son visibles
spark.table("ventas").write.format("delta").mode("overwrite").save(RUTA)

print(RUTA)
display(dbutils.fs.ls(RUTA))

# COMMAND ----------

display(dbutils.fs.ls(RUTA + "/_delta_log"))

# COMMAND ----------

log = spark.read.json(RUTA + "/_delta_log/*.json")
log.printSchema()
display(log)

# COMMAND ----------

# Una tabla por ruta se consulta con delta.`...`
display(spark.sql(f"SELECT * FROM delta.`{RUTA}` ORDER BY id"))

# COMMAND ----------

# Solo el commitInfo, que es lo legible
display(log.select("commitInfo.*").where(col("commitInfo").isNotNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cada 10 commits, un checkpoint
# MAGIC
# MAGIC Reconstruir el estado leyendo 10.000 JSON sería lentísimo. Cada 10 commits Delta
# MAGIC escribe un **checkpoint** en Parquet con el estado completo, y a partir de ahí
# MAGIC solo hay que leer el checkpoint más los JSON posteriores.

# COMMAND ----------

# Lo hacemos sobre la tabla por ruta, que es la que podemos inspeccionar
for i in range(12):
    spark.createDataFrame(
        [(100 + i, "x", 1.0, "ES", None)],
        "id INT, producto STRING, importe DOUBLE, pais STRING, fecha DATE",
    ).write.format("delta").mode("append").save(RUTA)

ficheros = [f.name for f in dbutils.fs.ls(RUTA + "/_delta_log")]
print("commits   :", sorted(f for f in ficheros if f.endswith(".json"))[:5], "...")
print("checkpoint:", [f for f in ficheros if "checkpoint" in f])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Particionado: cuándo NO
# MAGIC
# MAGIC Regla de Databricks: **no particiones tablas de menos de 1 TB**. Particionar por
# MAGIC una columna de alta cardinalidad genera miles de directorios con ficheros
# MAGIC diminutos. Hoy la respuesta correcta casi siempre es liquid clustering (nb 05).

# COMMAND ----------

(spark.table("ventas").write
    .mode("overwrite")
    .partitionBy("pais")
    .saveAsTable("ventas_part"))

display(spark.sql("SHOW PARTITIONS ventas_part"))
