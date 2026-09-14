# 03 · AUTO CDC (Python)
#
# En Python, AUTO CDC NO es un decorador: son dos llamadas a función.
#
#   1. dp.create_streaming_table("destino")   <- crea el destino VACÍO
#   2. dp.create_auto_cdc_flow(...)           <- el flujo que lo puebla
#
# Ese patrón de dos pasos es exactamente el equivalente del SQL:
#
#   CREATE OR REFRESH STREAMING TABLE destino;      -- sin AS SELECT
#   CREATE FLOW ... AS AUTO CDC INTO destino ...
#
# Equivalencia de parámetros con la sintaxis SQL:
#
#   target                  INTO destino
#   source                  FROM STREAM origen
#   keys                    KEYS (...)
#   sequence_by             SEQUENCE BY ...
#   apply_as_deletes        APPLY AS DELETE WHEN ...
#   apply_as_truncates      APPLY AS TRUNCATE WHEN ...   (solo SCD 1)
#   except_column_list      COLUMNS * EXCEPT (...)
#   column_list             COLUMNS (...)
#   stored_as_scd_type      STORED AS SCD TYPE 1|2
#   track_history_column_list / track_history_except_column_list
#                           TRACK HISTORY ON ...         (solo SCD 2)
#   ignore_null_updates     IGNORE NULL UPDATES

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# ---------------------------------------------------------------------------
# Tipar los eventos antes de aplicarlos
# ---------------------------------------------------------------------------
# `sequence_by` necesita un orden real. El ts viene como string del JSON, y
# ordenar strings de fecha solo funciona por casualidad si el formato es ISO.


@dp.table(
    name="clientes_cdc_tipado",
    comment="Eventos CDC con los tipos correctos",
)
def clientes_cdc_tipado():
    return (
        spark.readStream.table("bronze_clientes_cdc")
        .select(
            col("id").cast("int").alias("id"),
            col("nombre"),
            col("ciudad"),
            col("operacion"),
            col("ts").cast("timestamp").alias("ts"),
        )
    )


# ---------------------------------------------------------------------------
# SCD TIPO 1 — solo el estado actual
# ---------------------------------------------------------------------------

dp.create_streaming_table(
    name="dim_cliente_scd1",
    comment="Clientes, solo el valor vigente (SCD 1)",
)

dp.create_auto_cdc_flow(
    target="dim_cliente_scd1",
    source="clientes_cdc_tipado",
    keys=["id"],
    sequence_by=col("ts"),
    apply_as_deletes=expr("operacion = 'DELETE'"),
    # Sin esto, `operacion` acabaría como columna de la dimensión.
    # OJO: solo admite columnas que EXISTAN en el origen. `_rescued_data` la
    # trae bronze, pero la vista tipada ya no, así que nombrarla aquí falla.
    except_column_list=["operacion"],
    stored_as_scd_type=1,
)


# ---------------------------------------------------------------------------
# SCD TIPO 2 — con historia
# ---------------------------------------------------------------------------
# Mismos datos de entrada, distinto resultado. Añade dos columnas automáticas:
#
#   __START_AT   desde cuándo vale esta versión (toma el valor de sequence_by)
#   __END_AT     hasta cuándo. NULL = es la vigente
#
# El filtro para "el estado de hoy" es siempre `__END_AT IS NULL`.

dp.create_streaming_table(
    name="dim_cliente_scd2",
    comment="Clientes con historia completa (SCD 2)",
)

dp.create_auto_cdc_flow(
    target="dim_cliente_scd2",
    source="clientes_cdc_tipado",
    keys=["id"],
    sequence_by=col("ts"),
    apply_as_deletes=expr("operacion = 'DELETE'"),
    except_column_list=["operacion"],
    stored_as_scd_type=2,
    # Solo un cambio de `ciudad` abre versión nueva. Si cambia `nombre`, se
    # actualiza la vigente sin crear historia.
    # Sin esto, CUALQUIER columna que cambie genera versión nueva.
    track_history_column_list=["ciudad"],
)


# ---------------------------------------------------------------------------
# Vista de conveniencia
# ---------------------------------------------------------------------------


@dp.table(
    name="dim_cliente_vigente",
    comment="Solo la versión actual de cada cliente del SCD 2",
)
def dim_cliente_vigente():
    return (
        spark.read.table("dim_cliente_scd2")
        .where("__END_AT IS NULL")
        .select("id", "nombre", "ciudad", col("__START_AT").alias("vigente_desde"))
    )
