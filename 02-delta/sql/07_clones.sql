-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07 · Clones
-- MAGIC
-- MAGIC Copiar una tabla sin copiar los datos (o copiándolos, tú eliges).
-- MAGIC
-- MAGIC | | SHALLOW | DEEP |
-- MAGIC |---|---|---|
-- MAGIC | Qué copia | solo el `_delta_log` | log **y** ficheros de datos |
-- MAGIC | Velocidad | instantáneo | tarda lo que tarde copiar |
-- MAGIC | Espacio | ~0 | el de la tabla |
-- MAGIC | Depende del origen | **sí** | no, es independiente |
-- MAGIC | `VACUUM` en el origen | **puede romper el clon** | no le afecta |
-- MAGIC | Para qué | pruebas, experimentos de corta vida | backup, migración, copia a otra región |
-- MAGIC
-- MAGIC En los dos casos: escribir en el clon **no** toca al original, y viceversa.

-- COMMAND ----------

USE CATALOG main;
USE SCHEMA demo_delta;

CREATE OR REPLACE TABLE origen (id INT, valor STRING);
INSERT INTO origen VALUES (1, 'uno'), (2, 'dos'), (3, 'tres');

SELECT * FROM origen ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SHALLOW CLONE
-- MAGIC
-- MAGIC El caso de uso típico: quiero probar un `MERGE` destructivo contra datos de
-- MAGIC producción sin tocar producción ni esperar a copiar 2 TB.

-- COMMAND ----------

CREATE OR REPLACE TABLE origen_shallow SHALLOW CLONE origen;

SELECT * FROM origen_shallow ORDER BY id;

-- COMMAND ----------

-- `numFiles` cuenta ficheros que en realidad viven en el directorio del ORIGEN
DESCRIBE DETAIL origen_shallow;

-- COMMAND ----------

-- Escribo en el clon: el original no se entera
DELETE FROM origen_shallow WHERE id = 2;

SELECT 'clon' AS tabla, count(*) AS filas FROM origen_shallow
UNION ALL
SELECT 'origen', count(*) FROM origen;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### El peligro del shallow clone
-- MAGIC
-- MAGIC El clon apunta a ficheros del origen. Si en el origen alguien hace `VACUUM` y se
-- MAGIC lleva esos ficheros, el clon **se queda roto**. Por eso los shallow clones son
-- MAGIC para vidas cortas, no para backup.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DEEP CLONE
-- MAGIC
-- MAGIC Copia completa e independiente del **estado actual**: datos y metadatos. Es la
-- MAGIC opción para backups o para mover una tabla a otro catálogo/región.
-- MAGIC
-- MAGIC Ojo: **no copia el historial**. El clon arranca uno nuevo (versión 0 = `CLONE`).

-- COMMAND ----------

CREATE OR REPLACE TABLE origen_deep DEEP CLONE origen;

SELECT * FROM origen_deep ORDER BY id;

-- COMMAND ----------

DESCRIBE DETAIL origen_deep;   -- ficheros propios, en su propio `location`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## El deep clone es incremental
-- MAGIC
-- MAGIC Repetir un `CREATE OR REPLACE ... DEEP CLONE` **no** vuelve a copiarlo todo:
-- MAGIC copia solo lo que ha cambiado desde la última vez. Eso lo convierte en una
-- MAGIC estrategia de backup razonable: lo programas cada noche y solo paga el delta.

-- COMMAND ----------

INSERT INTO origen VALUES (4, 'cuatro'), (5, 'cinco');

-- Segunda pasada: solo trae las filas nuevas
CREATE OR REPLACE TABLE origen_deep DEEP CLONE origen;

SELECT * FROM origen_deep ORDER BY id;

-- COMMAND ----------

SELECT version, operation, operationMetrics
FROM (DESCRIBE HISTORY origen_deep)
ORDER BY version;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clonar una versión concreta
-- MAGIC
-- MAGIC Combinable con time travel. Muy útil para reproducir un bug: "dame la tabla tal
-- MAGIC y como estaba el martes".

-- COMMAND ----------

CREATE OR REPLACE TABLE origen_v1 SHALLOW CLONE origen VERSION AS OF 1;
SELECT * FROM origen_v1 ORDER BY id;

-- COMMAND ----------

-- CREATE TABLE ... DEEP CLONE ... TIMESTAMP AS OF '2026-09-01T00:00:00'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CREATE OR REPLACE vs IF NOT EXISTS
-- MAGIC
-- MAGIC - `CREATE OR REPLACE TABLE x DEEP CLONE y` → sincroniza (el patrón de backup).
-- MAGIC - `CREATE TABLE IF NOT EXISTS x DEEP CLONE y` → solo la primera vez; si ya
-- MAGIC   existe, no hace nada.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS origen_deep DEEP CLONE origen;   -- no-op

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Qué se lleva el clon y qué no
-- MAGIC
-- MAGIC **Sí**: los datos del estado actual, esquema, particionado/clustering,
-- MAGIC propiedades (`TBLPROPERTIES`), constraints y comentarios.
-- MAGIC
-- MAGIC **No**:
-- MAGIC - **El historial del origen.** El clon empieza el suyo propio, con una única
-- MAGIC   versión 0 cuya operación es `CLONE`. Viajar en el clon a una versión antigua
-- MAGIC   del origen falla (verificado):
-- MAGIC   `DELTA_VERSION_NOT_FOUND: Cannot time travel Delta table to version 1. Available versions: [0, 0]`.
-- MAGIC   Si necesitas una foto del pasado, clona **esa versión**: `DEEP CLONE t VERSION AS OF 3`.
-- MAGIC - Los permisos de Unity Catalog. El clon es un objeto nuevo y los `GRANT`
-- MAGIC   hay que rehacerlos.
