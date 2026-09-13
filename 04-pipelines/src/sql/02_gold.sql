-- 02 · GOLD — materialized views
--
-- Aquí NO se puede usar STREAMING TABLE, y entender por qué es media pregunta
-- de examen: una agregación cambia filas que ya existían. Si llega un pedido
-- nuevo de España, la fila "ES" del resultado tiene que actualizarse, no
-- añadirse. Una streaming table solo sabe añadir.
--
--   ¿El resultado de una fila nueva modifica filas anteriores?
--       SÍ  -> MATERIALIZED VIEW
--       NO  -> STREAMING TABLE
--
-- La MV se recalcula en cada ejecución, pero el optimizador hace refresco
-- incremental cuando puede (agregaciones simples, joins con claves estables).
-- Con un `count(DISTINCT)` o una window function normalmente no puede, y hace
-- recálculo completo.

CREATE OR REFRESH MATERIALIZED VIEW gold_ventas_por_pais
COMMENT 'Ventas agregadas por país'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  pais,
  count(*)                    AS pedidos,
  sum(importe)                AS importe_total,
  round(avg(importe), 2)      AS importe_medio,
  max(ts)                     AS ultimo_pedido
FROM silver_pedidos          -- sin STREAM: leemos la tabla entera, no el flujo
GROUP BY pais;


CREATE OR REFRESH MATERIALIZED VIEW gold_ventas_por_cliente
COMMENT 'Ranking de clientes'
AS
SELECT
  cliente_id,
  count(*)     AS pedidos,
  sum(importe) AS importe_total
FROM silver_pedidos
GROUP BY cliente_id;


-- ---------------------------------------------------------------------------
-- Expectations sobre AGREGADOS: validar el dataset completo
-- ---------------------------------------------------------------------------
-- Una expectation normal mira fila a fila. Si lo que quieres es comprobar algo
-- del conjunto ("no puede haber menos de N filas", "ningún país puede superar el
-- 90% del total"), la haces sobre una MV agregada.
--
-- Este patrón es el equivalente declarativo de un test de calidad, y es la forma
-- de que el pipeline se pare si el LOTE ENTERO es sospechoso, no una fila suelta.

CREATE OR REFRESH MATERIALIZED VIEW gold_control_calidad (
  CONSTRAINT hay_datos
    EXPECT (total_pedidos > 0) ON VIOLATION FAIL UPDATE,

  CONSTRAINT importes_coherentes
    EXPECT (importe_total > 0)
)
COMMENT 'Una sola fila con los totales del pipeline, para validar el conjunto'
AS
SELECT
  count(*)                          AS total_pedidos,
  sum(importe)                      AS importe_total,
  count(DISTINCT cliente_id)        AS clientes_distintos,
  count(DISTINCT pais)              AS paises_distintos,
  current_timestamp()               AS calculado_en
FROM silver_pedidos;
