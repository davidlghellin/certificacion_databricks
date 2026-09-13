-- 01 · SILVER — expectations
--
-- Una expectation es una regla de calidad que se evalúa fila a fila DENTRO del
-- pipeline. Las tres acciones, y esto es exactamente lo que preguntan:
--
--   EXPECT (cond)                          -> AVISA. La fila mala PASA igual.
--                                             Solo registra el conteo en el event log.
--   EXPECT (cond) ON VIOLATION DROP ROW    -> DESCARTA la fila. El pipeline sigue.
--   EXPECT (cond) ON VIOLATION FAIL UPDATE -> PARA el pipeline. Requiere intervención.
--
-- El defecto es el primero: sin `ON VIOLATION`, una expectation NO filtra nada.
-- Ese es el error clásico: creer que `EXPECT` a secas limpia los datos.
--
-- Expectation ≠ CHECK constraint de Delta:
--
--   Expectation      vive en el pipeline. Solo se aplica a lo que entra POR AQUÍ.
--                    Puede avisar sin bloquear. Genera métricas.
--   CHECK constraint vive en la tabla. Se aplica a CUALQUIER escritura, venga de
--                    donde venga. Siempre bloquea. No genera métricas.
--
-- Si la pregunta dice "quiero que cualquier escritura rechace la fila", la
-- respuesta es un constraint de Delta, no una expectation.

CREATE OR REFRESH STREAMING TABLE silver_pedidos (
  -- Solo avisa: las filas con país raro entran igual, pero quedan contadas.
  -- Útil cuando quieres detectar un problema sin cortar el flujo.
  CONSTRAINT pais_conocido
    EXPECT (pais IN ('ES', 'PT', 'FR')),

  -- Descarta: un pedido sin cliente no sirve para nada aguas abajo,
  -- pero tampoco es motivo para parar la ingesta entera.
  CONSTRAINT cliente_presente
    EXPECT (cliente_id IS NOT NULL) ON VIOLATION DROP ROW,

  -- Para el pipeline: un importe negativo significa que el origen está roto.
  -- Mejor detenerse que propagar basura a gold.
  CONSTRAINT importe_positivo
    EXPECT (importe > 0) ON VIOLATION FAIL UPDATE,

  -- Varias condiciones en una sola expectation: se combinan con AND.
  CONSTRAINT fecha_razonable
    EXPECT (ts IS NOT NULL AND ts > '2026-01-01') ON VIOLATION DROP ROW
)
COMMENT 'Pedidos validados y tipados'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
  CAST(pedido_id  AS BIGINT)     AS pedido_id,
  CAST(cliente_id AS INT)        AS cliente_id,
  UPPER(pais)                    AS pais,
  CAST(importe AS DECIMAL(10,2)) AS importe,
  CAST(ts AS TIMESTAMP)          AS ts,
  _fichero
-- Referencia a otra tabla del mismo pipeline: basta el nombre.
-- (El viejo prefijo `LIVE.` ya no hace falta.)
FROM STREAM bronze_pedidos;


-- ---------------------------------------------------------------------------
-- Cuarentena: quedarse con las filas malas en vez de tirarlas
-- ---------------------------------------------------------------------------
-- `DROP ROW` descarta y no deja rastro de la fila. Si necesitas auditarlas o
-- reprocesarlas, el patrón es invertir la condición en una tabla paralela.
-- Fíjate en que aquí NO hay expectations: queremos exactamente lo contrario.

CREATE OR REFRESH STREAMING TABLE silver_pedidos_cuarentena
COMMENT 'Las filas que silver_pedidos descarta, para poder investigarlas'
AS
SELECT
  *,
  -- Etiquetar POR QUÉ falló cada fila es lo que hace útil la cuarentena
  CASE
    WHEN cliente_id IS NULL THEN 'sin_cliente'
    WHEN ts IS NULL         THEN 'sin_fecha'
    ELSE 'otro'
  END AS motivo_rechazo
FROM STREAM bronze_pedidos
WHERE cliente_id IS NULL OR ts IS NULL;
