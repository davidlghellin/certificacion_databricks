-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Mantenimiento: OPTIMIZE, clustering, VACUUM
-- MAGIC
-- MAGIC El problema que resuelve todo esto es el **small files problem**: muchas
-- MAGIC escrituras pequeñas dejan miles de ficheros diminutos, y leerlos cuesta más
-- MAGIC por el overhead de abrir cada uno que por los datos en sí.

-- COMMAND ----------

USE CATALOG main;
USE SCHEMA demo_delta;

-- DROP antes de crear: `CREATE OR REPLACE` conserva el historial y las propiedades
-- que una ejecución anterior dejó puestas (deletion vectors, autoOptimize...).
DROP TABLE IF EXISTS metricas;
CREATE TABLE metricas (id INT, sensor STRING, valor DOUBLE);

-- 5 INSERT = 5 commits = al menos 5 ficheros
INSERT INTO metricas VALUES (1, 'a', 1.0);
INSERT INTO metricas VALUES (2, 'b', 2.0);
INSERT INTO metricas VALUES (3, 'a', 3.0);
INSERT INTO metricas VALUES (4, 'c', 4.0);
INSERT INTO metricas VALUES (5, 'b', 5.0);

DESCRIBE DETAIL metricas;   -- mira `numFiles`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OPTIMIZE: compactar
-- MAGIC
-- MAGIC Junta los ficheros pequeños en otros grandes (objetivo ~1 GB, o ~32 MB si hay
-- MAGIC auto-tuning por tamaño de tabla). No cambia los datos, solo cómo están
-- MAGIC repartidos: crea una versión nueva con operación `OPTIMIZE`.

-- COMMAND ----------

OPTIMIZE metricas;

-- COMMAND ----------

DESCRIBE DETAIL metricas;   -- `numFiles` ahora es 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `OPTIMIZE ... WHERE` solo acepta columnas de PARTICIÓN
-- MAGIC
-- MAGIC Esto falla con `DELTA_NON_PARTITION_COLUMN_REFERENCE`, porque `metricas` no está
-- MAGIC particionada y `sensor` es una columna normal:
-- MAGIC
-- MAGIC ```sql
-- MAGIC OPTIMIZE metricas WHERE sensor = 'a';   -- ❌
-- MAGIC ```
-- MAGIC
-- MAGIC El predicado sirve para acotar **qué particiones** compactar, no para filtrar
-- MAGIC filas. Sobre una tabla particionada sí vale.

-- COMMAND ----------

CREATE OR REPLACE TABLE metricas_part
PARTITIONED BY (sensor)
AS SELECT * FROM metricas;

-- Ahora sí: `sensor` es columna de partición
OPTIMIZE metricas_part WHERE sensor = 'a';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data skipping: por qué importa el orden
-- MAGIC
-- MAGIC Delta guarda **min/max de las primeras 32 columnas** de cada fichero en el log.
-- MAGIC Al filtrar, se salta los ficheros cuyo rango no puede contener el valor. Eso es
-- MAGIC *data skipping*, y funciona solo: no hay que activarlo.
-- MAGIC
-- MAGIC Pero salta bien únicamente si los datos están **agrupados** por la columna que
-- MAGIC filtras. Si están desperdigados, cada fichero tiene un rango ancho y no se
-- MAGIC salta nada. De eso van ZORDER y liquid clustering.

-- COMMAND ----------

-- Nº de columnas sobre las que se recogen estadísticas
ALTER TABLE metricas SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '8');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ZORDER: la forma clásica
-- MAGIC
-- MAGIC Reordena físicamente los datos para que valores parecidos de las columnas
-- MAGIC indicadas caigan en los mismos ficheros.
-- MAGIC
-- MAGIC - Solo tiene sentido en columnas de **alta cardinalidad** que uses en filtros
-- MAGIC   o joins.
-- MAGIC - Con 1 o 2 columnas va bien; a partir de 3-4 se diluye.
-- MAGIC - **No es incremental**: cada `OPTIMIZE ZORDER BY` reordena de nuevo lo que
-- MAGIC   toca. Es caro.

-- COMMAND ----------

OPTIMIZE metricas ZORDER BY (sensor);

SELECT version, operation, operationParameters
FROM (DESCRIBE HISTORY metricas)
WHERE operation = 'OPTIMIZE';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Liquid clustering: lo que se usa hoy
-- MAGIC
-- MAGIC Sustituye a la vez al particionado y al ZORDER.
-- MAGIC
-- MAGIC | | Particionado / ZORDER | Liquid clustering |
-- MAGIC |---|---|---|
-- MAGIC | Cambiar las columnas | reescribir la tabla | `ALTER TABLE ... CLUSTER BY` y ya |
-- MAGIC | Incremental | no | **sí** |
-- MAGIC | Skew / cardinalidad alta | sufre | lo maneja |
-- MAGIC
-- MAGIC No se pueden combinar: una tabla es particionada/zordered **o** clustered.

-- COMMAND ----------

CREATE OR REPLACE TABLE metricas_lc (
  id INT, sensor STRING, valor DOUBLE, ts TIMESTAMP
)
CLUSTER BY (sensor, ts);

INSERT INTO metricas_lc VALUES
  (1, 'a', 1.0, TIMESTAMP'2026-01-01 00:00'),
  (2, 'b', 2.0, TIMESTAMP'2026-01-02 00:00');

-- El clustering se materializa al ejecutar OPTIMIZE
OPTIMIZE metricas_lc;

DESCRIBE DETAIL metricas_lc;   -- mira `clusteringColumns`

-- COMMAND ----------

-- Cambiar las columnas de clustering: solo metadatos, no reescribe nada
ALTER TABLE metricas_lc CLUSTER BY (sensor);

-- Y quitarlo del todo
-- ALTER TABLE metricas_lc CLUSTER BY NONE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Automatic liquid clustering
-- MAGIC
-- MAGIC Databricks elige las columnas por ti mirando cómo se consulta la tabla.

-- COMMAND ----------

CREATE OR REPLACE TABLE metricas_auto (id INT, sensor STRING, valor DOUBLE)
CLUSTER BY AUTO;

DESCRIBE DETAIL metricas_auto;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Deletion vectors
-- MAGIC
-- MAGIC Sin ellos, borrar una fila obliga a reescribir el fichero entero
-- MAGIC (*copy-on-write*). Con ellos, Delta apunta aparte "en este fichero, ignora las
-- MAGIC filas 3 y 17" (*merge-on-read*): el `DELETE` es casi instantáneo.
-- MAGIC
-- MAGIC El precio es que las lecturas tienen que aplicar el vector. El siguiente
-- MAGIC `OPTIMIZE` materializa los borrados y limpia los vectores.

-- COMMAND ----------

ALTER TABLE metricas SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);

DELETE FROM metricas WHERE id = 3;

SELECT version, operation, operationMetrics
FROM (DESCRIBE HISTORY metricas) WHERE operation = 'DELETE';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## VACUUM: borrar de verdad
-- MAGIC
-- MAGIC `OPTIMIZE` y `DELETE` **no liberan espacio**: los ficheros viejos siguen ahí
-- MAGIC para el time travel. `VACUUM` es lo que los borra físicamente.
-- MAGIC
-- MAGIC - Retención por defecto: **7 días (168 h)**.
-- MAGIC - Bajar de 7 días está bloqueado por seguridad: un lector o un stream en curso
-- MAGIC   podría quedarse sin ficheros a media consulta.
-- MAGIC - **`VACUUM` destruye el time travel** a las versiones cuyos ficheros se lleva.
-- MAGIC   Es irreversible.
-- MAGIC - No borra el `_delta_log`: eso lo controla `delta.logRetentionDuration`.

-- COMMAND ----------

-- DRY RUN: enseña qué borraría, sin borrar nada. Úsalo siempre primero.
VACUUM metricas RETAIN 168 HOURS DRY RUN;

-- COMMAND ----------

VACUUM metricas;        -- retención por defecto: 7 días

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Forzar una retención corta (solo en demos)
-- MAGIC
-- MAGIC En una tabla real esto es una forma estupenda de romper un stream en producción.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Esta config puede estar bloqueada en serverless, igual que la de autoMerge.
-- MAGIC # Por eso va con red: el notebook no debe morirse por intentarlo.
-- MAGIC try:
-- MAGIC     spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
-- MAGIC     spark.sql("VACUUM metricas RETAIN 0 HOURS")
-- MAGIC     spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
-- MAGIC     print("VACUUM con retención 0 ejecutado: el time travel antiguo ya no existe")
-- MAGIC except Exception as e:
-- MAGIC     print("No se ha podido forzar retención 0 (normal en serverless):")
-- MAGIC     print(str(e)[:250])

-- COMMAND ----------

-- El historial sigue listando las versiones antiguas...
SELECT version, operation FROM (DESCRIBE HISTORY metricas) ORDER BY version;

-- COMMAND ----------

-- ...pero leerlas falla, porque los ficheros ya no están. OJO: solo si el VACUUM
-- con retención 0 de la celda anterior SÍ llegó a ejecutarse. En serverless esa
-- config está bloqueada, el VACUUM no se hace y la versión 1 sigue siendo legible.
-- En compute clásico, descomenta para ver el error de fichero no encontrado:
-- SELECT * FROM metricas VERSION AS OF 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Predictive optimization
-- MAGIC
-- MAGIC Databricks ejecuta `OPTIMIZE` y `VACUUM` por su cuenta cuando le compensa, solo
-- MAGIC en tablas **managed** de Unity Catalog. Es lo recomendado: si está activo, no
-- MAGIC programes tus propios jobs de mantenimiento.

-- COMMAND ----------

-- Se activa a nivel de cuenta/catálogo/esquema:
-- ALTER CATALOG main ENABLE PREDICTIVE OPTIMIZATION;
-- ALTER SCHEMA demo_delta ENABLE PREDICTIVE OPTIMIZATION;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Escrituras auto-optimizadas
-- MAGIC
-- MAGIC | Propiedad | Cuándo actúa |
-- MAGIC |---|---|
-- MAGIC | `delta.autoOptimize.optimizeWrite` | **antes** de escribir: reparticiona para no generar ficheros diminutos |
-- MAGIC | `delta.autoOptimize.autoCompact` | **después** de escribir: si quedaron muchos pequeños, los compacta |

-- COMMAND ----------

ALTER TABLE metricas SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

SHOW TBLPROPERTIES metricas;
