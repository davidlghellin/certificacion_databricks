-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Historial, time travel y RESTORE
-- MAGIC
-- MAGIC Cada operación de escritura crea una **versión** nueva (0, 1, 2…). Como los
-- MAGIC ficheros viejos no se borran al momento, puedes leer el pasado.

-- COMMAND ----------

USE CATALOG main;
USE SCHEMA demo_delta;

CREATE OR REPLACE TABLE inventario (sku STRING, stock INT);   -- v0
INSERT INTO inventario VALUES ('A', 10), ('B', 20);           -- v1
UPDATE inventario SET stock = 5 WHERE sku = 'A';              -- v2
DELETE FROM inventario WHERE sku = 'B';                       -- v3
INSERT INTO inventario VALUES ('C', 30);                      -- v4

SELECT * FROM inventario;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DESCRIBE HISTORY
-- MAGIC
-- MAGIC Columnas que importan: `version`, `timestamp`, `operation`, `operationMetrics`
-- MAGIC (filas afectadas), `userName` y `isBlindAppend`.

-- COMMAND ----------

DESCRIBE HISTORY inventario;

-- COMMAND ----------

-- Solo lo esencial, en orden cronológico
SELECT version, timestamp, operation, operationMetrics
FROM (DESCRIBE HISTORY inventario)
ORDER BY version;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time travel
-- MAGIC
-- MAGIC Dos formas, y las dos sintaxis de cada una:
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT * FROM t VERSION AS OF 2;      SELECT * FROM t@v2;
-- MAGIC SELECT * FROM t TIMESTAMP AS OF '…';  SELECT * FROM t@20260101000000000;
-- MAGIC ```
-- MAGIC
-- MAGIC Por versión es determinista. Por timestamp te da **la última versión anterior
-- MAGIC o igual** a ese instante; si pides un momento anterior a la creación de la
-- MAGIC tabla, es error.

-- COMMAND ----------

SELECT 'v1' AS momento, * FROM inventario VERSION AS OF 1
UNION ALL
SELECT 'v2', * FROM inventario VERSION AS OF 2
UNION ALL
SELECT 'v3', * FROM inventario VERSION AS OF 3
ORDER BY momento, sku;

-- COMMAND ----------

-- Sintaxis corta equivalente
SELECT * FROM inventario@v1;

-- COMMAND ----------

-- Por timestamp: coge el de la v1 del historial
SELECT * FROM inventario TIMESTAMP AS OF (
  SELECT timestamp FROM (DESCRIBE HISTORY inventario) WHERE version = 1
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Diffs entre versiones
-- MAGIC
-- MAGIC Qué cambió entre dos puntos, sin Change Data Feed. Útil para auditar.

-- COMMAND ----------

SELECT 'solo en v1' AS donde, * FROM (
  SELECT * FROM inventario VERSION AS OF 1
  EXCEPT
  SELECT * FROM inventario
)
UNION ALL
SELECT 'solo en actual', * FROM (
  SELECT * FROM inventario
  EXCEPT
  SELECT * FROM inventario VERSION AS OF 1
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## RESTORE: deshacer
-- MAGIC
-- MAGIC `RESTORE` **no borra historia**: crea una versión NUEVA cuyo contenido es el de
-- MAGIC la versión antigua. Así que siempre puedes deshacer el deshacer.

-- COMMAND ----------

RESTORE TABLE inventario TO VERSION AS OF 1;

SELECT * FROM inventario ORDER BY sku;

-- COMMAND ----------

-- Fíjate: la v1 sigue ahí y ahora hay una v5 con operation = RESTORE
SELECT version, operation FROM (DESCRIBE HISTORY inventario) ORDER BY version;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CLONE de una versión antigua
-- MAGIC
-- MAGIC Si en vez de sobrescribir quieres una copia del pasado en otra tabla:

-- COMMAND ----------

CREATE OR REPLACE TABLE inventario_v2 SHALLOW CLONE inventario VERSION AS OF 2;
SELECT * FROM inventario_v2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cuánto dura el time travel
-- MAGIC
-- MAGIC Dos retenciones distintas, y confundirlas es un clásico:
-- MAGIC
-- MAGIC | Propiedad | Controla | Defecto |
-- MAGIC |---|---|---|
-- MAGIC | `delta.logRetentionDuration` | cuánto se guardan los **commits** del log | 30 días |
-- MAGIC | `delta.deletedFileRetentionDuration` | cuánto sobreviven los **ficheros** borrados | 7 días |
-- MAGIC
-- MAGIC El time travel real es el **mínimo de los dos**: con los defectos, aunque el log
-- MAGIC guarde 30 días, un `VACUUM` se lleva los ficheros de más de 7 y las versiones
-- MAGIC antiguas dejan de ser legibles.

-- COMMAND ----------

ALTER TABLE inventario SET TBLPROPERTIES (
  'delta.logRetentionDuration'         = 'interval 90 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
);

SHOW TBLPROPERTIES inventario;
