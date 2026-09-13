-- 03 · AUTO CDC INTO (antes `APPLY CHANGES INTO`)
--
-- Es el gemelo declarativo de MERGE INTO. Resuelve en cinco líneas lo que en
-- MERGE cuesta una subconsulta con UNION ALL, pero solo vive dentro de un
-- pipeline y solo sabe hacer SCD 1 y SCD 2.
--
-- Los tres problemas de MERGE que arregla:
--
--   MERGE                                        AUTO CDC
--   -------------------------------------------- ----------------------------
--   Dos filas de origen con la misma clave -> ❌  Las ordena por SEQUENCE BY
--   SCD 2 exige el truco del mergeKey NULL        STORED AS SCD TYPE 2
--   Eventos desordenados hay que ordenarlos       SEQUENCE BY lo garantiza
--
-- Sintaxis completa:
--
--   AUTO CDC INTO destino
--     FROM STREAM origen
--     KEYS (col, ...)                       <- OBLIGATORIO
--     [ WHERE <cond> ]
--     [ IGNORE NULL UPDATES ]
--     [ APPLY AS DELETE WHEN <cond> ]
--     [ APPLY AS TRUNCATE WHEN <cond> ]     <- solo SCD TYPE 1
--     SEQUENCE BY <col | (c1, c2)>          <- OBLIGATORIO
--     [ COLUMNS { * EXCEPT (...) | (...) } ]
--     [ STORED AS { SCD TYPE 1 | SCD TYPE 2 } ]   <- por defecto TYPE 1
--     [ TRACK HISTORY ON { * EXCEPT (...) | (...) } ]  <- solo SCD TYPE 2


-- ---------------------------------------------------------------------------
-- Vista intermedia: tipar los eventos antes de aplicarlos
-- ---------------------------------------------------------------------------
-- El ts viene como string del JSON. SEQUENCE BY necesita un orden real, y
-- ordenar strings de fecha funciona por casualidad solo si el formato es ISO.
-- Mejor castear y no depender de la suerte.

CREATE OR REFRESH STREAMING TABLE clientes_cdc_tipado
COMMENT 'Eventos CDC con los tipos correctos'
AS
SELECT
  CAST(id AS INT)       AS id,
  nombre,
  ciudad,
  operacion,
  CAST(ts AS TIMESTAMP) AS ts
FROM STREAM bronze_clientes_cdc;


-- ---------------------------------------------------------------------------
-- SCD TIPO 1 — solo el estado actual, sin historia
-- ---------------------------------------------------------------------------
-- ATENCIÓN al patrón: el destino se declara VACÍO, sin `AS SELECT`.
-- Es el flujo AUTO CDC quien lo puebla. Intentar declararlo con una query es
-- uno de los errores de código que más aparecen en las preguntas.

CREATE OR REFRESH STREAMING TABLE dim_cliente_scd1
COMMENT 'Clientes, solo el valor vigente (SCD 1)';

CREATE FLOW flujo_scd1 AS
AUTO CDC INTO dim_cliente_scd1
FROM STREAM clientes_cdc_tipado
  KEYS (id)
  APPLY AS DELETE WHEN operacion = 'DELETE'
  SEQUENCE BY ts
  -- Sin esto, `operacion` acabaría como columna de la dimensión, que no es
  -- lo que quieres.
  -- OJO: EXCEPT solo admite columnas que EXISTAN en el origen. Aquí el origen
  -- es la vista tipada, que ya no tiene `_rescued_data` (esa la trae bronze):
  -- nombrarla daría UNRESOLVED_COLUMN.
  COLUMNS * EXCEPT (operacion)
  STORED AS SCD TYPE 1;


-- ---------------------------------------------------------------------------
-- SCD TIPO 2 — guardar la historia
-- ---------------------------------------------------------------------------
-- Mismos datos de entrada, distinto resultado: en vez de sobrescribir, cierra la
-- fila vigente y abre una nueva. Añade dos columnas automáticas:
--
--   __START_AT   desde cuándo vale esta versión  (toma el valor del SEQUENCE BY)
--   __END_AT     hasta cuándo. NULL = es la vigente
--
-- El filtro para "dame el estado de hoy" es siempre `WHERE __END_AT IS NULL`.

CREATE OR REFRESH STREAMING TABLE dim_cliente_scd2
COMMENT 'Clientes con historia completa (SCD 2)';

CREATE FLOW flujo_scd2 AS
AUTO CDC INTO dim_cliente_scd2
FROM STREAM clientes_cdc_tipado
  KEYS (id)
  APPLY AS DELETE WHEN operacion = 'DELETE'
  SEQUENCE BY ts
  COLUMNS * EXCEPT (operacion)
  STORED AS SCD TYPE 2
  -- Solo un cambio de `ciudad` abre versión nueva. Si cambia `nombre`, se
  -- actualiza la fila vigente sin crear historia.
  -- Sin TRACK HISTORY, CUALQUIER columna que cambie genera versión nueva.
  --
  -- OJO a la sintaxis: la lista de columnas va SIN paréntesis.
  -- `TRACK HISTORY ON (ciudad)` da PARSE_SYNTAX_ERROR.
  -- Con paréntesis solo se escribe la variante de exclusión:
  --   TRACK HISTORY ON * EXCEPT (nombre)
  TRACK HISTORY ON ciudad;


-- ---------------------------------------------------------------------------
-- Vista de conveniencia sobre el SCD 2
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_cliente_vigente
COMMENT 'Solo la versión actual de cada cliente del SCD 2'
AS
SELECT id, nombre, ciudad, __START_AT AS vigente_desde
FROM dim_cliente_scd2
WHERE __END_AT IS NULL;


-- ---------------------------------------------------------------------------
-- Qué esperar al ejecutar
-- ---------------------------------------------------------------------------
-- Con el lote 1 (tres INSERT) y luego el lote 2:
--
--   id=1  dos UPDATE en el mismo lote (Valencia ts=08:00, Barcelona ts=20:00)
--         -> SCD1: queda Barcelona. SCD2: una sola versión nueva, Barcelona.
--            MERGE habría fallado aquí con "multiple source rows matched".
--
--   id=2  UPDATE con ts de agosto, ANTERIOR al estado actual
--         -> se ignora. Ese es el trabajo de SEQUENCE BY.
--
--   id=3  DELETE
--         -> SCD1: desaparece la fila.
--            SCD2: se cierra la versión (__END_AT deja de ser NULL) pero la
--            historia se conserva. Un borrado no borra el pasado.
--
--   id=4  INSERT nuevo -> aparece en las dos.
