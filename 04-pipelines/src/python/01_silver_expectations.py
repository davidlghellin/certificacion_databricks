# 01 · SILVER — expectations (Python)
#
# Las tres acciones, con su equivalencia exacta:
#
#   @dp.expect("nombre", "cond")           = EXPECT (cond)                          AVISA
#   @dp.expect_or_drop("nombre", "cond")   = EXPECT (cond) ON VIOLATION DROP ROW    DESCARTA
#   @dp.expect_or_fail("nombre", "cond")   = EXPECT (cond) ON VIOLATION FAIL UPDATE PARA
#
# Y las versiones plurales, que reciben un diccionario {nombre: condición}:
#
#   @dp.expect_all, @dp.expect_all_or_drop, @dp.expect_all_or_fail
#
# Las plurales son la ventaja real de Python sobre SQL: puedes GENERAR las reglas
# desde una tabla de configuración en vez de escribirlas a mano.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, upper, when

# Reglas en un diccionario: esto en SQL habría que escribirlo a mano una a una.
# En un proyecto real este dict saldría de un fichero YAML o de una tabla.
REGLAS_DESCARTE = {
    "cliente_presente": "cliente_id IS NOT NULL",
    "fecha_razonable": "ts IS NOT NULL AND ts > '2026-01-01'",
}


@dp.table(
    name="silver_pedidos",
    comment="Pedidos validados y tipados",
    table_properties={"quality": "silver"},
)
# Solo avisa: las filas con país raro entran igual, pero quedan contadas.
@dp.expect("pais_conocido", "pais IN ('ES','PT','FR')")
# Descarta: varias reglas de golpe desde el diccionario.
@dp.expect_all_or_drop(REGLAS_DESCARTE)
# Para el pipeline: un importe negativo significa que el origen está roto.
@dp.expect_or_fail("importe_positivo", "importe > 0")
def silver_pedidos():
    return (
        # Referencia a otra tabla del pipeline: spark.readStream.table(nombre).
        # El viejo prefijo `LIVE.` ya no hace falta.
        spark.readStream.table("bronze_pedidos")
        .select(
            col("pedido_id").cast("bigint").alias("pedido_id"),
            col("cliente_id").cast("int").alias("cliente_id"),
            upper(col("pais")).alias("pais"),
            col("importe").cast("decimal(10,2)").alias("importe"),
            col("ts").cast("timestamp").alias("ts"),
            col("_fichero"),
        )
    )


# ---------------------------------------------------------------------------
# Cuarentena
# ---------------------------------------------------------------------------
# `expect_or_drop` descarta y no deja rastro. Si necesitas auditar o reprocesar
# las filas malas, el patrón es una tabla paralela con la condición invertida.
# Fíjate en que aquí NO hay decoradores de expectation: queremos lo contrario.


@dp.table(
    name="silver_pedidos_cuarentena",
    comment="Las filas que silver_pedidos descarta, para poder investigarlas",
)
def silver_pedidos_cuarentena():
    # La condición tiene que ser el COMPLEMENTO EXACTO de `REGLAS_DESCARTE`,
    # evaluada sobre los MISMOS valores que ve silver: ya tipados. Un
    # `cliente_id = 'abc'` no es NULL en crudo, pero sí tras el cast, y silver lo
    # descarta; mirando el crudo, esa fila no aparecería en ningún sitio.
    cliente_tipado = col("cliente_id").cast("int")
    ts_tipado = col("ts").cast("timestamp")
    return (
        spark.readStream.table("bronze_pedidos")
        .select(
            "*",
            cliente_tipado.alias("_cliente_id_tipado"),
            ts_tipado.alias("_ts_tipado"),
        )
        # NOT (cliente_presente) OR NOT (fecha_razonable)
        .where("_cliente_id_tipado IS NULL "
               "OR NOT (_ts_tipado IS NOT NULL AND _ts_tipado > '2026-01-01')")
        # Etiquetar POR QUÉ falló cada fila es lo que hace útil la cuarentena
        .select(
            "*",
            when(col("_cliente_id_tipado").isNull(), "sin_cliente")
            .when(col("_ts_tipado").isNull(), "sin_fecha")
            .otherwise("fecha_anterior_a_2026")
            .alias("motivo_rechazo"),
        )
    )
