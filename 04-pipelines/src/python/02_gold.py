# 02 · GOLD — materialized views (Python)
#
# La clave: el decorador es el MISMO `@dp.table`. Lo que decide si sale una
# streaming table o una materialized view es cómo lees:
#
#   spark.readStream.table(...)  -> STREAMING TABLE
#   spark.read.table(...)        -> MATERIALIZED VIEW
#
# Aquí toca `spark.read` porque una agregación modifica filas que ya existían:
# si llega un pedido nuevo de España, la fila "ES" del resultado se actualiza,
# no se añade. Una streaming table solo sabe añadir.

from pyspark import pipelines as dp
from pyspark.sql.functions import avg, col, count, countDistinct, current_timestamp, max as smax, round as sround, sum as ssum


@dp.table(
    name="gold_ventas_por_pais",
    comment="Ventas agregadas por país",
    table_properties={"quality": "gold"},
)
def gold_ventas_por_pais():
    return (
        spark.read.table("silver_pedidos")     # <- read, no readStream
        .groupBy("pais")
        .agg(
            count("*").alias("pedidos"),
            ssum("importe").alias("importe_total"),
            sround(avg("importe"), 2).alias("importe_medio"),
            smax("ts").alias("ultimo_pedido"),
        )
    )


@dp.table(
    name="gold_ventas_por_cliente",
    comment="Ranking de clientes",
)
def gold_ventas_por_cliente():
    return (
        spark.read.table("silver_pedidos")
        .groupBy("cliente_id")
        .agg(
            count("*").alias("pedidos"),
            ssum("importe").alias("importe_total"),
        )
    )


# ---------------------------------------------------------------------------
# Expectations sobre AGREGADOS
# ---------------------------------------------------------------------------
# Una expectation normal mira fila a fila. Para validar el CONJUNTO ("no puede
# haber menos de N filas", "el total no puede ser negativo") la pones sobre una
# MV agregada de una sola fila.
#
# Es la forma de que el pipeline se pare si el lote entero es sospechoso, no una
# fila suelta.


@dp.table(
    name="gold_control_calidad",
    comment="Una sola fila con los totales del pipeline, para validar el conjunto",
)
@dp.expect_or_fail("hay_datos", "total_pedidos > 0")
@dp.expect("importes_coherentes", "importe_total > 0")
def gold_control_calidad():
    return (
        spark.read.table("silver_pedidos")
        .agg(
            count("*").alias("total_pedidos"),
            ssum("importe").alias("importe_total"),
            countDistinct("cliente_id").alias("clientes_distintos"),
            countDistinct("pais").alias("paises_distintos"),
            # La agregación ya define el esquema entero, así que la columna
            # derivada entra aquí mismo en vez de en un withColumn aparte.
            smax(current_timestamp()).alias("calculado_en"),
        )
    )
