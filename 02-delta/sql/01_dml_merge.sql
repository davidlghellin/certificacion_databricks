-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · DML y MERGE
-- MAGIC
-- MAGIC Lo que Delta añade sobre Parquet: `UPDATE`, `DELETE` y `MERGE` con garantías
-- MAGIC ACID. En Parquet puro no existen.

-- COMMAND ----------

USE CATALOG main;
USE SCHEMA demo_delta;

CREATE OR REPLACE TABLE clientes (
  id      INT,
  nombre  STRING,
  ciudad  STRING,
  activo  BOOLEAN
);

INSERT INTO clientes VALUES
  (1, 'Ana',   'Madrid',  true),
  (2, 'Bruno', 'Sevilla', true),
  (3, 'Carla', 'Bilbao',  true);

SELECT * FROM clientes ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## UPDATE y DELETE
-- MAGIC
-- MAGIC Ambos reescriben **ficheros enteros**, no filas. Si un fichero de 500 MB
-- MAGIC contiene una fila que cambias, se reescribe el fichero completo… salvo que la
-- MAGIC tabla tenga **deletion vectors** activados (notebook 05).

-- COMMAND ----------

UPDATE clientes SET ciudad = 'Barcelona' WHERE id = 1;

DELETE FROM clientes WHERE id = 3;

SELECT * FROM clientes ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## MERGE INTO: los tres universos
-- MAGIC
-- MAGIC Un `MERGE` cruza destino (`t`) con origen (`s`) por la condición `ON` y parte
-- MAGIC las filas en tres grupos:
-- MAGIC
-- MAGIC | Cláusula | Qué filas coge |
-- MAGIC |---|---|
-- MAGIC | `WHEN MATCHED` | están en los dos → `UPDATE` o `DELETE` |
-- MAGIC | `WHEN NOT MATCHED` | están solo en origen → `INSERT` |
-- MAGIC | `WHEN NOT MATCHED BY SOURCE` | están solo en destino → `UPDATE` o `DELETE` |
-- MAGIC
-- MAGIC Ese tercero es el que se olvida todo el mundo, y es justo el que necesitas
-- MAGIC para "lo que ya no venga en el origen, márcalo de baja".
-- MAGIC
-- MAGIC Reglas:
-- MAGIC - Puedes tener varias cláusulas del mismo tipo, cada una con su `AND`.
-- MAGIC - Se evalúan **en orden**: gana la primera que casa.
-- MAGIC - `WHEN MATCHED` solo admite `UPDATE` o `DELETE`; `NOT MATCHED` solo `INSERT`.
-- MAGIC - Si el origen tiene **dos filas que casan con la misma fila destino**, el
-- MAGIC   `MERGE` falla. Hay que deduplicar antes.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cambios AS
SELECT * FROM VALUES
  (1, 'Ana',   'Valencia', 'UPDATE'),
  (2, NULL,    NULL,       'DELETE'),
  (4, 'Diego', 'Malaga',   'INSERT')
AS t(id, nombre, ciudad, op);

SELECT * FROM cambios;

-- COMMAND ----------

MERGE INTO clientes t
USING cambios s
ON t.id = s.id

WHEN MATCHED AND s.op = 'DELETE' THEN
  DELETE

WHEN MATCHED AND s.op = 'UPDATE' THEN
  UPDATE SET t.ciudad = s.ciudad

WHEN NOT MATCHED AND s.op = 'INSERT' THEN
  INSERT (id, nombre, ciudad, activo) VALUES (s.id, s.nombre, s.ciudad, true)

-- Filas que están en clientes pero NO llegan en `cambios`: baja lógica.
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET t.activo = false;

SELECT * FROM clientes ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Métricas del MERGE
-- MAGIC
-- MAGIC El historial guarda cuántas filas se insertaron, actualizaron y borraron.
-- MAGIC Es la forma de auditar un pipeline sin contar tablas a mano.

-- COMMAND ----------

SELECT version, operation, operationMetrics
FROM (DESCRIBE HISTORY clientes)
WHERE operation = 'MERGE';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Patrón 1 · SCD Tipo 1 (sobrescribir)
-- MAGIC
-- MAGIC El registro solo tiene su valor actual. No hay historia.

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_cliente_scd1 AS SELECT id, nombre, ciudad FROM clientes;

CREATE OR REPLACE TEMP VIEW nuevos AS
SELECT * FROM VALUES (1, 'Ana', 'Sevilla'), (9, 'Elena', 'Vigo') AS t(id, nombre, ciudad);

-- Ojo a los dos niveles, que se confunden:
--   ON t.id = s.id  -> empareja FILAS (el cruce)
--   SET *           -> expande COLUMNAS: equivale a escribir
--                      SET t.nombre = s.nombre, t.ciudad = s.ciudad, ...
--                      casando las columnas de origen y destino POR NOMBRE.
-- El origen debe traer todas las columnas del destino, o falla.
MERGE INTO dim_cliente_scd1 t
USING nuevos s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM dim_cliente_scd1 ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Patrón 2 · SCD Tipo 2 (guardar la historia)
-- MAGIC
-- MAGIC Cada cambio cierra la fila vigente y abre una nueva. El truco: un `MERGE` no
-- MAGIC puede a la vez cerrar la vieja e insertar la nueva para la **misma** clave, así
-- MAGIC que el origen se duplica: una fila con clave (cierra) y otra con clave `NULL`
-- MAGIC (no casa → inserta).

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_cliente_scd2 (
  id      INT,
  ciudad  STRING,
  desde   DATE,
  hasta   DATE,
  vigente BOOLEAN
);

INSERT INTO dim_cliente_scd2 VALUES
  (1, 'Madrid', DATE'2026-01-01', NULL, true);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW scd2_origen AS
WITH entrada AS (
  SELECT 1 AS id, 'Valencia' AS ciudad, DATE'2026-06-01' AS desde
)
-- Fila A: casa con la vigente → la cierra
SELECT e.id AS clave, e.* FROM entrada e
UNION ALL
-- Fila B: clave NULL → no casa → se inserta como nueva versión
SELECT NULL AS clave, e.* FROM entrada e
JOIN dim_cliente_scd2 d ON d.id = e.id AND d.vigente
WHERE d.ciudad <> e.ciudad;

SELECT * FROM scd2_origen;

-- COMMAND ----------

MERGE INTO dim_cliente_scd2 t
USING scd2_origen s
ON t.id = s.clave AND t.vigente

WHEN MATCHED AND t.ciudad <> s.ciudad THEN
  UPDATE SET t.vigente = false, t.hasta = s.desde

WHEN NOT MATCHED THEN
  INSERT (id, ciudad, desde, hasta, vigente)
  VALUES (s.id, s.ciudad, s.desde, NULL, true);

SELECT * FROM dim_cliente_scd2 ORDER BY id, desde;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Patrón 3 · Deduplicar antes del MERGE
-- MAGIC
-- MAGIC Si el origen trae varias versiones de la misma clave, quédate con una sola.
-- MAGIC Si no, el `MERGE` revienta con *"Cannot perform Merge as multiple source rows
-- MAGIC matched..."*.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sucio AS
SELECT * FROM VALUES
  (1, 'Ana', 'Madrid',   TIMESTAMP'2026-06-01 10:00'),
  (1, 'Ana', 'Valencia', TIMESTAMP'2026-06-01 12:00')   -- más reciente, gana
AS t(id, nombre, ciudad, ts);

CREATE OR REPLACE TEMP VIEW limpio AS
SELECT id, nombre, ciudad FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn
  FROM sucio
) WHERE rn = 1;

SELECT * FROM limpio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Patrón 4 · Idempotencia
-- MAGIC
-- MAGIC Un `MERGE` bien escrito se puede repetir sin duplicar nada: por eso es la
-- MAGIC operación preferida en pipelines que se reintentan. Un `INSERT INTO`, no.

-- COMMAND ----------

-- Ejecuta esta celda dos veces: el resultado no cambia.
MERGE INTO clientes t
USING (SELECT 5 AS id, 'Fran' AS nombre, 'Oviedo' AS ciudad) s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (id, nombre, ciudad, activo) VALUES (s.id, s.nombre, s.ciudad, true);

SELECT count(*) AS filas FROM clientes;
