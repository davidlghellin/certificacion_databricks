-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Crear tablas Delta
-- MAGIC
-- MAGIC **Qué es una tabla Delta**: un directorio de ficheros Parquet + una carpeta
-- MAGIC `_delta_log/` con el registro de transacciones. Eso es todo. El log es lo que
-- MAGIC convierte un montón de Parquet en una tabla con ACID, versiones y time travel.
-- MAGIC
-- MAGIC En Databricks, **Delta es el formato por defecto**: un `CREATE TABLE` sin
-- MAGIC `USING` ya es Delta.

-- COMMAND ----------

-- Ajusta el catálogo si el tuyo no es `main` (en Free Edition suele ser `workspace`).
USE CATALOG main;
CREATE SCHEMA IF NOT EXISTS demo_delta;
USE SCHEMA demo_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed vs External
-- MAGIC
-- MAGIC | | Managed | External |
-- MAGIC |---|---|---|
-- MAGIC | Ubicación | la decide Unity Catalog | la fijas tú con `LOCATION` |
-- MAGIC | `DROP TABLE` | borra metadatos **y datos** | borra **solo** los metadatos |
-- MAGIC | Mantenimiento | Databricks puede optimizar por ti | tuyo |
-- MAGIC
-- MAGIC Esa fila del `DROP` es la pregunta de examen. Managed = los datos se van.

-- COMMAND ----------

-- Tabla MANAGED (sin LOCATION)
DROP TABLE IF EXISTS ventas;

CREATE TABLE ventas (
  id        INT,
  producto  STRING,
  importe   DECIMAL(10,2),
  pais      STRING,
  fecha     DATE
)
COMMENT 'Tabla de ejemplo para el curso de Delta'
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

INSERT INTO ventas VALUES
  (1, 'teclado',  49.90, 'ES', DATE'2026-01-10'),
  (2, 'raton',    19.90, 'ES', DATE'2026-01-11'),
  (3, 'monitor', 199.00, 'PT', DATE'2026-01-11'),
  (4, 'teclado',  49.90, 'FR', DATE'2026-01-12');

SELECT * FROM ventas ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS: crear a partir de una consulta
-- MAGIC
-- MAGIC `CREATE TABLE ... AS SELECT` hereda el esquema del `SELECT`. No puedes
-- MAGIC declarar tipos a mano en un CTAS: si necesitas forzarlos, haz el `CAST`
-- MAGIC dentro del `SELECT`.

-- COMMAND ----------

CREATE OR REPLACE TABLE ventas_es AS
SELECT id, producto, importe, fecha
FROM ventas
WHERE pais = 'ES';

SELECT * FROM ventas_es;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tabla EXTERNAL
-- MAGIC
-- MAGIC Los datos viven donde tú digas (un Volume o una ruta de cloud storage).
-- MAGIC Descomenta si tienes un external location o un volume disponible.

-- COMMAND ----------

-- CREATE TABLE ventas_ext (
--   id INT, producto STRING, importe DECIMAL(10,2)
-- )
-- LOCATION '/Volumes/main/demo_delta/datos/ventas_ext';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inspeccionar la tabla
-- MAGIC
-- MAGIC Tres comandos que hay que tener en los dedos:
-- MAGIC
-- MAGIC | Comando | Te dice |
-- MAGIC |---|---|
-- MAGIC | `DESCRIBE TABLE` | columnas y tipos |
-- MAGIC | `DESCRIBE TABLE EXTENDED` | + tipo (MANAGED/EXTERNAL), ubicación, propiedades, owner |
-- MAGIC | `DESCRIBE DETAIL` | + nº de ficheros, tamaño, `minReaderVersion`/`minWriterVersion`, features |

-- COMMAND ----------

DESCRIBE TABLE ventas;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED ventas;

-- COMMAND ----------

-- Fíjate en `numFiles`, `sizeInBytes`, `location` y `tableFeatures`.
DESCRIBE DETAIL ventas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ver el transaction log con tus propios ojos
-- MAGIC
-- MAGIC Aquí hay una limitación de Unity Catalog que conviene conocer: en una tabla
-- MAGIC **managed**, `DESCRIBE DETAIL` devuelve el `location` **vacío** y no puedes
-- MAGIC listar sus ficheros. UC oculta el almacenamiento de las managed a propósito:
-- MAGIC son suyas, y solo se tocan a través de la tabla.
-- MAGIC
-- MAGIC Para mirar el log por dentro hace falta una tabla cuya ruta controles tú. La
-- MAGIC creamos en un **Volume**.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Compruébalo: en una managed de UC el location viene vacío
-- MAGIC print("location de una managed:", repr(spark.sql("DESCRIBE DETAIL ventas").first()["location"]))

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS datos;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cat = spark.sql("SELECT current_catalog()").first()[0]
-- MAGIC esq = spark.sql("SELECT current_schema()").first()[0]
-- MAGIC RUTA = f"/Volumes/{cat}/{esq}/datos/ventas_raw"
-- MAGIC
-- MAGIC # Tabla Delta "por ruta": mismo formato, pero los ficheros son visibles
-- MAGIC spark.table("ventas").write.format("delta").mode("overwrite").save(RUTA)
-- MAGIC
-- MAGIC print(RUTA)
-- MAGIC display(dbutils.fs.ls(RUTA))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Un .json por cada commit, más el _delta_log entero
-- MAGIC display(dbutils.fs.ls(RUTA + "/_delta_log"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Qué hay dentro de un commit
-- MAGIC
-- MAGIC Cada `.json` del log es una lista de **acciones**: `add` (fichero nuevo),
-- MAGIC `remove` (fichero que deja de contar), `metaData` (esquema), `commitInfo`
-- MAGIC (quién, cuándo, qué operación).
-- MAGIC
-- MAGIC Clave: **nunca se reescribe un fichero de datos existente**. Un `UPDATE` no
-- MAGIC modifica el Parquet, escribe uno nuevo y marca el viejo como `remove`. Por eso
-- MAGIC el time travel es gratis y por eso hace falta `VACUUM`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.json(RUTA + "/_delta_log/*.json"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Y una tabla por ruta se consulta con delta.`...`
-- MAGIC display(spark.sql(f"SELECT * FROM delta.`{RUTA}` ORDER BY id"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Particionado: cuándo NO hacerlo
-- MAGIC
-- MAGIC Regla práctica de Databricks: **no particiones tablas de menos de 1 TB**.
-- MAGIC El particionado por una columna de alta cardinalidad crea miles de
-- MAGIC directorios con ficheros diminutos y destroza el rendimiento.
-- MAGIC
-- MAGIC Hoy la respuesta correcta casi siempre es **liquid clustering** (notebook 05),
-- MAGIC no `PARTITIONED BY`.

-- COMMAND ----------

-- Así se haría, si de verdad hiciera falta:
CREATE OR REPLACE TABLE ventas_part
PARTITIONED BY (pais)
AS SELECT * FROM ventas;

SHOW PARTITIONS ventas_part;
