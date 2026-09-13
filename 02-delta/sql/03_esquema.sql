-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Esquema: enforcement y evolución
-- MAGIC
-- MAGIC Delta valida el esquema **en la escritura** (*schema enforcement*): si los datos
-- MAGIC no encajan, la escritura falla en vez de corromper la tabla. Y si quieres que
-- MAGIC el esquema cambie, tienes que pedirlo explícitamente (*schema evolution*).

-- COMMAND ----------

USE CATALOG main;
USE SCHEMA demo_delta;

CREATE OR REPLACE TABLE productos (
  id      INT,
  nombre  STRING,
  precio  DECIMAL(10,2)
);

INSERT INTO productos VALUES (1, 'teclado', 49.90), (2, 'raton', 19.90);
SELECT * FROM productos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Enforcement: esto falla, y está bien que falle

-- COMMAND ----------

-- Descomenta para ver el error: la columna `color` no existe en la tabla.
-- INSERT INTO productos SELECT 3, 'monitor', 199.00, 'negro';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Evolución explícita con ALTER TABLE
-- MAGIC
-- MAGIC | Operación | ¿Se puede? |
-- MAGIC |---|---|
-- MAGIC | `ADD COLUMN` | sí, siempre |
-- MAGIC | `ALTER COLUMN ... COMMENT` | sí |
-- MAGIC | `ALTER COLUMN ... DROP NOT NULL` | sí |
-- MAGIC | `RENAME COLUMN` | sí, **si** hay column mapping |
-- MAGIC | `DROP COLUMN` | sí, **si** hay column mapping |
-- MAGIC | Cambiar el **tipo** de una columna | no directamente: hay que reescribir |

-- COMMAND ----------

ALTER TABLE productos ADD COLUMN color STRING COMMENT 'Color del producto';

-- Las filas que ya existían se leen con NULL en la columna nueva:
-- no se reescribe nada, el valor por defecto de una columna ausente es NULL.
SELECT * FROM productos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Column mapping: requisito para RENAME y DROP
-- MAGIC
-- MAGIC Sin column mapping, el nombre de la columna en Parquet **es** el nombre lógico,
-- MAGIC así que renombrar obligaría a reescribirlo todo. Con column mapping, Delta
-- MAGIC guarda un id interno y el rename es solo metadatos.
-- MAGIC
-- MAGIC Ojo: activarlo **sube la versión mínima de lector/escritor** de la tabla.
-- MAGIC Clientes antiguos dejarán de poder leerla. Es irreversible en la práctica.

-- COMMAND ----------

ALTER TABLE productos SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion'   = '2',
  'delta.minWriterVersion'   = '5'
);

ALTER TABLE productos RENAME COLUMN color TO color_principal;
SELECT * FROM productos;

-- COMMAND ----------

-- DROP COLUMN también es solo metadatos: los datos siguen en el Parquet
-- hasta el siguiente OPTIMIZE/reescritura.
ALTER TABLE productos DROP COLUMN color_principal;
DESCRIBE TABLE productos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Evolución automática al escribir
-- MAGIC
-- MAGIC | Opción | Qué hace |
-- MAGIC |---|---|
-- MAGIC | `mergeSchema` | **añade** columnas nuevas, respeta las existentes |
-- MAGIC | `overwriteSchema` | **sustituye** el esquema entero (solo con `overwrite`) |
-- MAGIC
-- MAGIC En clásico existe la config de sesión
-- MAGIC `spark.databricks.delta.schema.autoMerge.enabled`, pero **en serverless está
-- MAGIC bloqueada**: falla con `CONFIG_NOT_AVAILABLE.SERVERLESS_DELTA_SCHEMA_AUTO_MERGE_ENABLED`.
-- MAGIC
-- MAGIC En serverless las vías que quedan son tres, y todas mejores:
-- MAGIC
-- MAGIC 1. `.option("mergeSchema", "true")` en el writer (Python).
-- MAGIC 2. `MERGE WITH SCHEMA EVOLUTION` (SQL, celda de abajo).
-- MAGIC 3. `ALTER TABLE ... ADD COLUMN` explícito, que es lo más controlado.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # La opción por escritura sí funciona en serverless
-- MAGIC nuevo = spark.createDataFrame(
-- MAGIC     [(3, "monitor", 199.00, "negro")],
-- MAGIC     "id INT, nombre STRING, precio DOUBLE, color STRING",
-- MAGIC )
-- MAGIC
-- MAGIC (nuevo.write
-- MAGIC   .mode("append")
-- MAGIC   .option("mergeSchema", "true")
-- MAGIC   .saveAsTable("productos"))

-- COMMAND ----------

SELECT * FROM productos ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## MERGE WITH SCHEMA EVOLUTION
-- MAGIC
-- MAGIC Sintaxis dedicada, más limpia que la config de sesión: permite que el `MERGE`
-- MAGIC añada columnas que trae el origen y la tabla no tiene.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW nuevos_prod AS
SELECT * FROM VALUES
  (4, 'webcam', 79.00, 'blanco', 'ES')
AS t(id, nombre, precio, color, pais_origen);

MERGE WITH SCHEMA EVOLUTION INTO productos t
USING nuevos_prod s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM productos ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cambiar el tipo de una columna
-- MAGIC
-- MAGIC No hay `ALTER COLUMN ... TYPE` general. La vía es reescribir la tabla con
-- MAGIC `CREATE OR REPLACE ... AS SELECT` y el `CAST` dentro.
-- MAGIC
-- MAGIC `CREATE OR REPLACE` **conserva el historial** de la tabla (crea una versión
-- MAGIC nueva); `DROP` + `CREATE` lo pierde. Esa diferencia cae en el examen.

-- COMMAND ----------

CREATE OR REPLACE TABLE productos AS
SELECT id, nombre, CAST(precio AS DOUBLE) AS precio, color, pais_origen
FROM productos;

DESCRIBE TABLE productos;

-- COMMAND ----------

-- El historial sigue entero, con un REPLACE TABLE AS SELECT al final
SELECT version, operation FROM (DESCRIBE HISTORY productos) ORDER BY version;
