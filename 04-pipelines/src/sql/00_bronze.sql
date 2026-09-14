-- 00 · BRONZE — ingesta incremental con Auto Loader
--
-- En un pipeline declarativo NO escribes "cómo" cargar: declaras QUÉ tabla quieres
-- y el motor se encarga del orden, los checkpoints y el reprocesado.
--
-- STREAMING TABLE vs MATERIALIZED VIEW — la decisión de diseño más preguntada:
--
--   STREAMING TABLE      procesa cada fila UNA VEZ. El origen debe ser append-only.
--                        Barata: solo mira lo nuevo. Es lo que quieres en bronze.
--
--   MATERIALIZED VIEW    recalcula el resultado completo (o lo refresca de forma
--                        incremental si puede). Soporta agregados y joins que
--                        cambian el pasado. Es lo que quieres en gold.
--
-- Regla: si la pregunta dice "cada registro se procesa exactamente una vez" o
-- "los datos solo se añaden", es STREAMING TABLE. Si dice "agregación sobre todo
-- el histórico" o "los datos de origen cambian", es MATERIALIZED VIEW.

-- `${ruta.landing}` viene de la sección `configuration` del pipeline (resources/pipelines.yml).
-- Los parámetros de pipeline se interpolan con ${...}, igual que en un bundle.

CREATE OR REFRESH STREAMING TABLE bronze_pedidos
COMMENT 'Pedidos crudos tal cual llegan del Volume, sin limpiar'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT
  *,
  -- `_metadata` es una columna oculta que expone read_files: de qué fichero salió
  -- cada fila y cuándo se modificó. Capturarla en bronze es práctica estándar,
  -- porque es lo único que te permite auditar el origen después.
  _metadata.file_name          AS _fichero,
  _metadata.file_modification_time AS _fichero_ts,
  current_timestamp()          AS _ingesta_ts
FROM STREAM read_files(
  '${ruta.landing}/pedidos',
  format => 'json',
  -- El esquema se infiere y se guarda; si aparecen columnas nuevas, evolucionan.
  -- Lo que no encaje acaba en `_rescued_data` en lugar de romper la ingesta.
  schemaEvolutionMode => 'addNewColumns'
);

-- La misma idea para los eventos CDC de clientes: bronze los recoge crudos,
-- y es el notebook 03 quien los aplica con AUTO CDC.
CREATE OR REFRESH STREAMING TABLE bronze_clientes_cdc
COMMENT 'Eventos CDC de clientes, sin aplicar'
AS
SELECT
  *,
  _metadata.file_name AS _fichero
FROM STREAM read_files(
  '${ruta.landing}/clientes_cdc',
  format => 'json'
);
