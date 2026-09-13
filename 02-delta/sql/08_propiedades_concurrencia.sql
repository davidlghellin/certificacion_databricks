-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 08 · Propiedades, versiones de protocolo y concurrencia
-- MAGIC
-- MAGIC La parte "de dentro" de Delta: cómo se configura una tabla y qué pasa cuando
-- MAGIC dos escritores van a la vez.

-- COMMAND ----------

USE CATALOG main;
USE SCHEMA demo_delta;

CREATE OR REPLACE TABLE config_demo (id INT, v STRING);
INSERT INTO config_demo VALUES (1, 'a');

SHOW TBLPROPERTIES config_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Las propiedades que hay que conocer
-- MAGIC
-- MAGIC | Propiedad | Para qué | Defecto |
-- MAGIC |---|---|---|
-- MAGIC | `delta.logRetentionDuration` | cuánto vive el historial de commits | 30 días |
-- MAGIC | `delta.deletedFileRetentionDuration` | cuánto sobreviven los ficheros borrados | 7 días |
-- MAGIC | `delta.enableChangeDataFeed` | activa CDF | false |
-- MAGIC | `delta.enableDeletionVectors` | borrados merge-on-read | según DBR |
-- MAGIC | `delta.autoOptimize.optimizeWrite` | reparticiona antes de escribir | false |
-- MAGIC | `delta.autoOptimize.autoCompact` | compacta después de escribir | false |
-- MAGIC | `delta.dataSkippingNumIndexedCols` | columnas con estadísticas min/max | 32 |
-- MAGIC | `delta.columnMapping.mode` | permite `RENAME`/`DROP COLUMN` | none |
-- MAGIC | `delta.appendOnly` | prohíbe `UPDATE`/`DELETE` | false |
-- MAGIC | `delta.targetFileSize` | tamaño objetivo de fichero | auto |

-- COMMAND ----------

ALTER TABLE config_demo SET TBLPROPERTIES (
  'delta.logRetentionDuration'         = 'interval 60 days',
  'delta.deletedFileRetentionDuration' = 'interval 14 days',
  'delta.targetFileSize'               = '32mb'
);

SHOW TBLPROPERTIES config_demo;

-- COMMAND ----------

-- Quitar una propiedad
ALTER TABLE config_demo UNSET TBLPROPERTIES ('delta.targetFileSize');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## appendOnly: tabla inmutable
-- MAGIC
-- MAGIC Bloquea `UPDATE` y `DELETE`. Es el candado típico de una capa bronze de
-- MAGIC auditoría, donde el histórico crudo no se toca nunca.

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_log (ts TIMESTAMP, evento STRING)
TBLPROPERTIES ('delta.appendOnly' = true);

INSERT INTO bronze_log VALUES (current_timestamp(), 'arranque');

-- Descomenta: falla con "This table is configured to only allow appends"
-- DELETE FROM bronze_log WHERE evento = 'arranque';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Versiones de protocolo
-- MAGIC
-- MAGIC Cada tabla declara un `minReaderVersion` y un `minWriterVersion`. Activar
-- MAGIC ciertas features los sube, y **eso es una puerta de un solo sentido**: clientes
-- MAGIC más antiguos dejan de poder leer o escribir esa tabla.
-- MAGIC
-- MAGIC Ejemplos de features que suben el protocolo: column mapping, deletion vectors,
-- MAGIC CDF, generated columns, identity, liquid clustering.

-- COMMAND ----------

DESCRIBE DETAIL config_demo;   -- mira `minReaderVersion`, `minWriterVersion`, `tableFeatures`

-- COMMAND ----------

-- Subida explícita (no se puede bajar después)
-- ALTER TABLE config_demo SET TBLPROPERTIES (
--   'delta.minReaderVersion' = '3',
--   'delta.minWriterVersion' = '7'
-- );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Concurrencia optimista (OCC)
-- MAGIC
-- MAGIC Delta no bloquea nada. Cada escritor:
-- MAGIC
-- MAGIC 1. Lee el estado actual (una versión concreta).
-- MAGIC 2. Hace su trabajo y prepara los ficheros.
-- MAGIC 3. Intenta escribir el commit `N+1`.
-- MAGIC 4. Si otro lo escribió primero, mira si los cambios **entran en conflicto**:
-- MAGIC    si no, reintenta sobre el nuevo estado; si sí, lanza excepción.
-- MAGIC
-- MAGIC Consecuencia: **lecturas y escrituras nunca se bloquean entre sí**. Un lector
-- MAGIC ve la versión que había cuando empezó, entera y coherente (*snapshot isolation*).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Las excepciones que hay que reconocer
-- MAGIC
-- MAGIC | Excepción | Cuándo salta |
-- MAGIC |---|---|
-- MAGIC | `ConcurrentAppendException` | otro añadió ficheros a la partición/rango que tu operación leía |
-- MAGIC | `ConcurrentDeleteReadException` | tu operación leyó un fichero que otro acababa de borrar |
-- MAGIC | `ConcurrentDeleteDeleteException` | los dos intentasteis borrar el mismo fichero |
-- MAGIC | `ConcurrentTransactionException` | dos streams con el **mismo** `checkpointLocation` |
-- MAGIC | `ProtocolChangedException` | otro subió la versión de protocolo mientras tú escribías |
-- MAGIC
-- MAGIC La cura de `ConcurrentAppendException` casi siempre es la misma: **acota mejor
-- MAGIC la condición**. Si dos jobs escriben en países distintos, mete el país en el
-- MAGIC `ON` del `MERGE` para que Delta sepa que no se pisan:
-- MAGIC
-- MAGIC ```sql
-- MAGIC MERGE INTO ventas t
-- MAGIC USING lote s ON t.pais = 'ES' AND t.id = s.id   -- ← el filtro de partición aquí
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Niveles de aislamiento
-- MAGIC
-- MAGIC | Nivel | Qué garantiza |
-- MAGIC |---|---|
-- MAGIC | `WriteSerializable` (**defecto**) | los *appends* ciegos pueden colarse entre medias; más concurrencia |
-- MAGIC | `Serializable` | orden total estricto; más conflictos |

-- COMMAND ----------

ALTER TABLE config_demo SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable');
SHOW TBLPROPERTIES config_demo;

ALTER TABLE config_demo SET TBLPROPERTIES ('delta.isolationLevel' = 'WriteSerializable');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Escrituras idempotentes
-- MAGIC
-- MAGIC Para batch, Delta puede descartar un lote que ya aplicó, identificado por un par
-- MAGIC (aplicación, id de lote). Si el job se reintenta, no duplica.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # En un notebook SQL, una celda %python no trae nada importado: hay que pedirlo.
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC lote = (spark.range(3)
-- MAGIC         .select(col("id").cast("int").alias("id"),
-- MAGIC                 col("id").cast("string").alias("v")))
-- MAGIC
-- MAGIC (lote.write
-- MAGIC   .option("txnAppId", "mi_job_diario")
-- MAGIC   .option("txnVersion", 42)          # el mismo 42 no se aplica dos veces
-- MAGIC   .mode("append")
-- MAGIC   .saveAsTable("main.demo_delta.config_demo"))
-- MAGIC
-- MAGIC # Ejecuta la celda dos veces: la segunda no añade nada.
-- MAGIC print("filas =", spark.table("main.demo_delta.config_demo").count())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Streaming sobre Delta, en dos frases
-- MAGIC
-- MAGIC Una tabla Delta es una fuente de streaming válida: cada commit nuevo es un
-- MAGIC micro-batch. Por defecto el stream **solo admite appends**; si la tabla recibe
-- MAGIC `UPDATE` o `DELETE`, falla.
-- MAGIC
-- MAGIC Salidas: `ignoreDeletes` / `ignoreChanges` (los salta), o activar CDF y leer el
-- MAGIC feed, que es la opción limpia. Lo demás, en el apéndice B de los apuntes.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # (spark.readStream.table("main.demo_delta.cuentas")
-- MAGIC #   .writeStream
-- MAGIC #   .option("checkpointLocation", "/Volumes/main/demo_delta/chk/cuentas")
-- MAGIC #   .trigger(availableNow=True)
-- MAGIC #   .toTable("main.demo_delta.cuentas_stream"))
