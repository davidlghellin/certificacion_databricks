-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · Change Data Feed (CDF)
-- MAGIC
-- MAGIC El time travel te da **fotos** de la tabla. CDF te da el **vídeo**: qué filas
-- MAGIC concretas se insertaron, borraron o cambiaron, y en qué versión.
-- MAGIC
-- MAGIC Sirve para propagar cambios aguas abajo (bronze → silver → gold) sin recalcular
-- MAGIC la tabla entera.

-- COMMAND ----------

USE CATALOG main;
USE SCHEMA demo_delta;

-- Se puede activar al crear...
CREATE OR REPLACE TABLE cuentas (
  id      INT,
  titular STRING,
  saldo   DOUBLE
)
TBLPROPERTIES ('delta.enableChangeDataFeed' = true);

-- ...o después, con ALTER TABLE:
-- ALTER TABLE cuentas SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Trampa importante**: CDF solo registra los cambios **a partir del momento en
-- MAGIC que lo activas**. Pedir el feed de versiones anteriores es un error, no una
-- MAGIC lista vacía.

-- COMMAND ----------

INSERT INTO cuentas VALUES (1, 'Ana', 1000.0), (2, 'Bruno', 500.0);   -- v1
UPDATE cuentas SET saldo = 1200.0 WHERE id = 1;                        -- v2
DELETE FROM cuentas WHERE id = 2;                                      -- v3
INSERT INTO cuentas VALUES (3, 'Carla', 300.0);                        -- v4

SELECT * FROM cuentas ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Leer el feed: `table_changes()`
-- MAGIC
-- MAGIC Devuelve las columnas de la tabla más tres:
-- MAGIC
-- MAGIC | Columna | Contenido |
-- MAGIC |---|---|
-- MAGIC | `_change_type` | `insert`, `delete`, `update_preimage`, `update_postimage` |
-- MAGIC | `_commit_version` | versión en la que ocurrió |
-- MAGIC | `_commit_timestamp` | cuándo |
-- MAGIC
-- MAGIC Un `UPDATE` genera **dos** filas: cómo estaba (`preimage`) y cómo quedó
-- MAGIC (`postimage`). Es lo que permite saber qué cambió exactamente.

-- COMMAND ----------

SELECT * FROM table_changes('cuentas', 1)
ORDER BY _commit_version, id;

-- COMMAND ----------

-- Un rango concreto de versiones
SELECT * FROM table_changes('cuentas', 2, 3) ORDER BY _commit_version;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Los argumentos de `table_changes()` tienen que ser CONSTANTES
-- MAGIC
-- MAGIC Esto falla con `DELTA_CDC_NON_CONSTANT_ARGUMENT`:
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT * FROM table_changes('cuentas', (SELECT timestamp FROM ...));  -- ❌
-- MAGIC ```
-- MAGIC
-- MAGIC No admite subconsultas. En un pipeline real la versión de arranque la lleva el
-- MAGIC checkpoint del stream, o la inyectas como parámetro del job.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Desde Python sí puedes: lees el timestamp y lo metes como literal
-- MAGIC ts = spark.sql("DESCRIBE HISTORY cuentas").where("version = 2").first()["timestamp"]
-- MAGIC display(spark.sql(f"SELECT * FROM table_changes('cuentas', '{ts}')"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Neto de cambios por fila
-- MAGIC
-- MAGIC Si solo quieres el estado final de cada clave en un rango, quédate con el
-- MAGIC último `postimage`/`insert` y descarta los `preimage`.

-- COMMAND ----------

SELECT id, titular, saldo, _change_type, _commit_version
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _commit_version DESC) AS rn
  FROM table_changes('cuentas', 1)
  WHERE _change_type <> 'update_preimage'
)
WHERE rn = 1
ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Propagar a una tabla silver
-- MAGIC
-- MAGIC El patrón real: leo el feed desde la última versión que procesé y lo aplico con
-- MAGIC un `MERGE`. Los `delete` del feed se traducen en `DELETE`, el resto en upsert.

-- COMMAND ----------

CREATE OR REPLACE TABLE cuentas_silver AS SELECT * FROM cuentas WHERE 1 = 0;

CREATE OR REPLACE TEMP VIEW feed AS
SELECT id, titular, saldo, _change_type
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _commit_version DESC) AS rn
  FROM table_changes('cuentas', 1)
  WHERE _change_type <> 'update_preimage'
)
WHERE rn = 1;

MERGE INTO cuentas_silver t
USING feed s ON t.id = s.id
WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.titular = s.titular, t.saldo = s.saldo
WHEN NOT MATCHED AND s._change_type <> 'delete' THEN
  INSERT (id, titular, saldo) VALUES (s.id, s.titular, s.saldo);

SELECT * FROM cuentas_silver ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Qué NO genera CDF
-- MAGIC
-- MAGIC Las operaciones que solo mueven ficheros sin cambiar datos **no** aparecen en
-- MAGIC el feed: `OPTIMIZE`, `VACUUM`, `ZORDER`. Y un `INSERT` puro se marca como
-- MAGIC *blind append*: Delta puede resolverlo leyendo el log, sin escribir ficheros
-- MAGIC de CDF aparte.
-- MAGIC
-- MAGIC Los ficheros de CDF viven en `_change_data/` y los limpia el `VACUUM` con la
-- MAGIC misma retención que el resto. O sea: **CDF también caduca**.

-- COMMAND ----------

OPTIMIZE cuentas;

-- El OPTIMIZE no ha añadido filas al feed
SELECT _commit_version, _change_type, count(*) AS filas
FROM table_changes('cuentas', 1)
GROUP BY _commit_version, _change_type
ORDER BY _commit_version;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CDF vs otras formas de capturar cambios
-- MAGIC
-- MAGIC | Herramienta | Para qué |
-- MAGIC |---|---|
-- MAGIC | **CDF** | leer los cambios que ya ocurrieron en una tabla Delta |
-- MAGIC | **`AUTO CDC INTO`** (antes `APPLY CHANGES`) | aplicar CDC en Declarative Pipelines, con SCD 1/2 automáticos |
-- MAGIC | **Structured Streaming sobre Delta** | leer solo los *appends* nuevos (por defecto falla si hay updates/deletes) |
