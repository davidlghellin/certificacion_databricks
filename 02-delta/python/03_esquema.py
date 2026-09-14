# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Esquema: enforcement y evolución (Python)
# MAGIC
# MAGIC Delta valida el esquema **al escribir**. Si los datos no encajan, la escritura
# MAGIC falla en vez de corromper la tabla. Para que el esquema cambie hay que pedirlo.

# COMMAND ----------

from pyspark.sql.functions import col, lit

CATALOGO, ESQUEMA = "main", "demo_delta_py"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

spark.createDataFrame(
    [(1, "teclado", 49.90), (2, "raton", 19.90)], "id INT, nombre STRING, precio DOUBLE"
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("productos")

display(spark.table("productos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enforcement: esto falla, y está bien

# COMMAND ----------

extra = spark.createDataFrame(
    [(3, "monitor", 199.00, "negro")], "id INT, nombre STRING, precio DOUBLE, color STRING"
)

try:
    extra.write.mode("append").saveAsTable("productos")
except Exception as e:
    print("Rechazado, como debe ser:")
    print(str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Las tres opciones de evolución
# MAGIC
# MAGIC | Opción | Modo | Qué hace |
# MAGIC |---|---|---|
# MAGIC | `mergeSchema` | `append` u `overwrite` | **añade** columnas nuevas, conserva las que había |
# MAGIC | `overwriteSchema` | solo `overwrite` | **sustituye** el esquema entero (permite cambiar tipos y quitar columnas) |
# MAGIC | ninguna | cualquiera | enforcement estricto |
# MAGIC
# MAGIC `mergeSchema` es aditivo y seguro. `overwriteSchema` es destructivo: se carga las
# MAGIC columnas que no vengan en el DataFrame nuevo.

# COMMAND ----------

extra.write.mode("append").option("mergeSchema", "true").saveAsTable("productos")

# Las filas antiguas salen con NULL en `color`: no se ha reescrito nada,
# una columna ausente en un fichero viejo se lee como NULL.
display(spark.table("productos").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### La config de sesión NO vale en serverless
# MAGIC
# MAGIC En clásico existe `spark.databricks.delta.schema.autoMerge.enabled` para no
# MAGIC repetir la opción en cada write. En **serverless está bloqueada** y falla con
# MAGIC `CONFIG_NOT_AVAILABLE.SERVERLESS_DELTA_SCHEMA_AUTO_MERGE_ENABLED`.
# MAGIC
# MAGIC O sea: en serverless, la evolución se pide **por escritura**.

# COMMAND ----------

try:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    print("Estás en clásico: la config de sesión ha funcionado")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
except Exception as e:
    print("Serverless: config bloqueada, hay que usar .option('mergeSchema')")
    print(str(e)[:200])

# COMMAND ----------

# La forma que funciona en los dos sitios
spark.createDataFrame(
    [(4, "webcam", 79.00, "blanco", "ES")],
    "id INT, nombre STRING, precio DOUBLE, color STRING, pais STRING",
).write.mode("append").option("mergeSchema", "true").saveAsTable("productos")

display(spark.table("productos").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## overwriteSchema: cambiar tipos
# MAGIC
# MAGIC No hay `ALTER COLUMN ... TYPE`. Para cambiar un tipo hay que reescribir la tabla.
# MAGIC `overwriteSchema` **conserva el historial** (es una versión más), a diferencia de
# MAGIC borrar y recrear la tabla.

# COMMAND ----------

origen = spark.table("productos")
# Solo cambia el tipo de `precio`; el resto de columnas pasan tal cual. Se construye
# desde `origen.columns` para no perder ninguna si la tabla tiene más de las que
# esperamos (por ejemplo, si una celda anterior de evolución de esquema añadió otra).
nuevo = origen.select([
    col(c).cast("decimal(10,2)").alias(c) if c == "precio" else col(c)
    for c in origen.columns
])

(nuevo.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("productos"))

spark.table("productos").printSchema()

# COMMAND ----------

# El historial sigue entero
from delta.tables import DeltaTable
display(DeltaTable.forName(spark, "productos").history().select("version", "operation").orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ALTER TABLE desde Python
# MAGIC
# MAGIC | Operación | ¿Se puede? |
# MAGIC |---|---|
# MAGIC | `ADD COLUMN` | sí |
# MAGIC | `ALTER COLUMN ... COMMENT` / `DROP NOT NULL` | sí |
# MAGIC | `RENAME COLUMN` | sí, **con** column mapping |
# MAGIC | `DROP COLUMN` | sí, **con** column mapping |
# MAGIC | Cambiar tipo | no: reescribir |

# COMMAND ----------

spark.sql("ALTER TABLE productos ADD COLUMN peso_kg DOUBLE COMMENT 'Peso en kilos'")
spark.table("productos").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column mapping
# MAGIC
# MAGIC Sin él, el nombre lógico de la columna **es** el nombre en Parquet, así que
# MAGIC renombrar exigiría reescribirlo todo. Con column mapping, Delta usa un id interno
# MAGIC y el rename es solo metadatos.
# MAGIC
# MAGIC Activarlo **sube el protocolo de la tabla** y es irreversible en la práctica:
# MAGIC clientes antiguos dejan de poder leerla.

# COMMAND ----------

spark.sql("""
    ALTER TABLE productos SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion'   = '2',
      'delta.minWriterVersion'   = '5'
    )
""")

spark.sql("ALTER TABLE productos RENAME COLUMN peso_kg TO peso")
spark.sql("ALTER TABLE productos DROP COLUMN peso")

spark.table("productos").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir el esquema a mano
# MAGIC
# MAGIC Para ingesta, inferir el esquema cuesta un escaneo extra de los ficheros y puede
# MAGIC equivocarse. En producción se declara.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

esquema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("nombre", StringType()),
    StructField("precio", DoubleType()),
    StructField("alta", DateType()),
])

vacio = spark.createDataFrame([], esquema)
vacio.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("productos_tipado")

spark.table("productos_tipado").printSchema()
