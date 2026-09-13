# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Propiedades, protocolo y concurrencia (Python)
# MAGIC
# MAGIC La parte "de dentro" de Delta: cómo se configura una tabla y qué pasa cuando dos
# MAGIC escritores van a la vez.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

CATALOGO, ESQUEMA = "main", "demo_delta_py"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

TABLA = f"{CATALOGO}.{ESQUEMA}.config_demo"

spark.sql(f"CREATE OR REPLACE TABLE {TABLA} (id INT, v STRING)")
spark.sql(f"INSERT INTO {TABLA} VALUES (1, 'a')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Las propiedades que hay que conocer
# MAGIC
# MAGIC | Propiedad | Para qué | Defecto |
# MAGIC |---|---|---|
# MAGIC | `delta.logRetentionDuration` | cuánto vive el historial de commits | 30 días |
# MAGIC | `delta.deletedFileRetentionDuration` | cuánto sobreviven los ficheros borrados | 7 días |
# MAGIC | `delta.enableChangeDataFeed` | activa CDF | false |
# MAGIC | `delta.enableDeletionVectors` | borrados merge-on-read | según DBR |
# MAGIC | `delta.autoOptimize.optimizeWrite` | reparticiona antes de escribir | false |
# MAGIC | `delta.autoOptimize.autoCompact` | compacta después de escribir | false |
# MAGIC | `delta.dataSkippingNumIndexedCols` | columnas con estadísticas min/max | 32 |
# MAGIC | `delta.columnMapping.mode` | permite `RENAME`/`DROP COLUMN` | none |
# MAGIC | `delta.appendOnly` | prohíbe `UPDATE`/`DELETE` | false |
# MAGIC | `delta.targetFileSize` | tamaño objetivo de fichero | auto |

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {TABLA} SET TBLPROPERTIES (
      'delta.logRetentionDuration'         = 'interval 60 days',
      'delta.deletedFileRetentionDuration' = 'interval 14 days',
      'delta.targetFileSize'               = '32mb'
    )
""")

display(spark.sql(f"SHOW TBLPROPERTIES {TABLA}"))

# COMMAND ----------

spark.sql(f"ALTER TABLE {TABLA} UNSET TBLPROPERTIES ('delta.targetFileSize')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defaults de sesión (bloqueados en serverless)
# MAGIC
# MAGIC En clásico, `spark.databricks.delta.properties.defaults.<propiedad>` hace que
# MAGIC TODAS las tablas nuevas nazcan con esa propiedad, sin repetirla.
# MAGIC
# MAGIC En **serverless está bloqueado**:
# MAGIC `CONFIG_NOT_AVAILABLE.SERVERLESS_DELTA_PROPERTIES_DEFAULTS_ENABLE_CHANGE_DATA_FEED`.
# MAGIC Hay que ponerlo tabla a tabla, con `TBLPROPERTIES` o con la opción del writer.
# MAGIC
# MAGIC Es el mismo patrón que ya vimos con `schema.autoMerge` (nb 03) y
# MAGIC `retentionDurationCheck` (nb 05): **serverless no deja tocar configs de Spark
# MAGIC que cambien el comportamiento global**. Todo se pide por tabla o por escritura.

# COMMAND ----------

CDF_TBL = f"{CATALOGO}.{ESQUEMA}.nace_con_cdf"

try:
    spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    spark.sql(f"CREATE OR REPLACE TABLE {CDF_TBL} (id INT)")
    spark.conf.unset("spark.databricks.delta.properties.defaults.enableChangeDataFeed")
    print("Estás en clásico: la tabla nace con CDF por el default de sesión")
except Exception as e:
    print("Serverless: default de sesión bloqueado. Se pone por tabla:")
    print(str(e)[:200])
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CDF_TBL} (id INT)
        TBLPROPERTIES ('delta.enableChangeDataFeed' = true)
    """)

display(spark.sql(f"SHOW TBLPROPERTIES {CDF_TBL}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## appendOnly: tabla inmutable
# MAGIC
# MAGIC Bloquea `UPDATE` y `DELETE`. El candado típico de una bronze de auditoría, donde
# MAGIC el histórico crudo no se toca nunca.

# COMMAND ----------

BRONZE = f"{CATALOGO}.{ESQUEMA}.bronze_log"
# DROP + CREATE, y no `CREATE OR REPLACE`: sobre una tabla que ya es appendOnly,
# un REPLACE cuenta como borrar sus datos y falla con DELTA_CANNOT_MODIFY_APPEND_ONLY
# en la segunda ejecución. `DROP TABLE` sí se permite.
spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")
spark.sql(f"""
    CREATE TABLE {BRONZE} (ts TIMESTAMP, evento STRING)
    TBLPROPERTIES ('delta.appendOnly' = true)
""")
spark.sql(f"INSERT INTO {BRONZE} VALUES (current_timestamp(), 'arranque')")

try:
    DeltaTable.forName(spark, BRONZE).delete("evento = 'arranque'")
except Exception as e:
    print("Bloqueado por appendOnly, como debe ser:")
    print(str(e)[:250])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Versiones de protocolo
# MAGIC
# MAGIC Cada tabla declara `minReaderVersion` y `minWriterVersion`. Activar ciertas
# MAGIC features los sube, y **es una puerta de un solo sentido**: clientes más antiguos
# MAGIC dejan de poder leer o escribir esa tabla.
# MAGIC
# MAGIC Suben el protocolo: column mapping, deletion vectors, CDF, generated columns,
# MAGIC identity, liquid clustering.

# COMMAND ----------

d = spark.sql(f"DESCRIBE DETAIL {TABLA}").first()
print("minReaderVersion:", d["minReaderVersion"])
print("minWriterVersion:", d["minWriterVersion"])
print("tableFeatures   :", d["tableFeatures"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concurrencia optimista (OCC)
# MAGIC
# MAGIC Delta no bloquea nada. Cada escritor:
# MAGIC
# MAGIC 1. Lee el estado actual (una versión concreta).
# MAGIC 2. Prepara sus ficheros.
# MAGIC 3. Intenta escribir el commit `N+1`.
# MAGIC 4. Si otro llegó primero, mira si hay **conflicto real**: si no, reintenta sobre
# MAGIC    el estado nuevo; si sí, lanza excepción.
# MAGIC
# MAGIC Consecuencia: **lecturas y escrituras nunca se bloquean entre sí**. Un lector ve
# MAGIC la versión que había cuando empezó, entera y coherente (*snapshot isolation*).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Las excepciones que hay que reconocer
# MAGIC
# MAGIC | Excepción | Cuándo salta |
# MAGIC |---|---|
# MAGIC | `ConcurrentAppendException` | otro añadió ficheros a la partición/rango que leías |
# MAGIC | `ConcurrentDeleteReadException` | leíste un fichero que otro acababa de borrar |
# MAGIC | `ConcurrentDeleteDeleteException` | los dos borrasteis el mismo fichero |
# MAGIC | `ConcurrentTransactionException` | dos streams con el **mismo** `checkpointLocation` |
# MAGIC | `ProtocolChangedException` | otro subió el protocolo mientras escribías |
# MAGIC
# MAGIC La cura de `ConcurrentAppendException` casi siempre es **acotar mejor la
# MAGIC condición**: si dos jobs escriben en países distintos, mete el país en el `ON` del
# MAGIC `MERGE` para que Delta sepa que no se pisan.

# COMMAND ----------

# Mal: los dos jobs "tocan" toda la tabla desde el punto de vista de Delta
#   .merge(lote, "t.id = s.id")
#
# Bien: el filtro de partición va DENTRO de la condición del merge
#   .merge(lote, "t.pais = 'ES' AND t.id = s.id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Niveles de aislamiento
# MAGIC
# MAGIC | Nivel | Qué garantiza |
# MAGIC |---|---|
# MAGIC | `WriteSerializable` (**defecto**) | los appends ciegos pueden colarse entre medias; más concurrencia |
# MAGIC | `Serializable` | orden total estricto; más conflictos |

# COMMAND ----------

spark.sql(f"ALTER TABLE {TABLA} SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')")
display(spark.sql(f"SHOW TBLPROPERTIES {TABLA}"))

spark.sql(f"ALTER TABLE {TABLA} SET TBLPROPERTIES ('delta.isolationLevel' = 'WriteSerializable')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reintentar un conflicto
# MAGIC
# MAGIC En un pipeline real, un `ConcurrentAppendException` suele resolverse
# MAGIC reintentando: el segundo intento parte del estado nuevo y ya no choca.

# COMMAND ----------

import time

def escribir_con_reintentos(fn, intentos=3, espera=5):
    for i in range(intentos):
        try:
            return fn()
        except Exception as e:
            nombre = type(e).__name__
            if "Concurrent" not in nombre and "Concurrent" not in str(e):
                raise
            print(f"intento {i + 1}: conflicto ({nombre}), reintentando…")
            time.sleep(espera)
    raise RuntimeError("Agotados los reintentos por conflicto de concurrencia")


escribir_con_reintentos(
    lambda: spark.createDataFrame([(2, "b")], "id INT, v STRING")
    .write.mode("append").saveAsTable(TABLA)
)

display(spark.table(TABLA))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming sobre Delta, en dos frases
# MAGIC
# MAGIC Una tabla Delta es fuente de streaming válida: cada commit es un micro-batch. Por
# MAGIC defecto el stream **solo admite appends**; si la tabla recibe `UPDATE` o `DELETE`,
# MAGIC falla.
# MAGIC
# MAGIC Salidas: `ignoreDeletes` / `ignoreChanges` (los salta, con riesgo de duplicados
# MAGIC aguas abajo), o activar CDF y leer el feed, que es lo limpio. El resto, en el
# MAGIC apéndice B de los apuntes.

# COMMAND ----------

# (spark.readStream
#    .option("ignoreChanges", "true")
#    .table(f"{CATALOGO}.{ESQUEMA}.cuentas")
#    .writeStream
#    .option("checkpointLocation", "/Volumes/main/demo_delta_py/chk/cuentas")
#    .trigger(availableNow=True)
#    .toTable(f"{CATALOGO}.{ESQUEMA}.cuentas_stream"))
