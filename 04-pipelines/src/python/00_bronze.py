# 00 · BRONZE — ingesta incremental (Python)
#
# El módulo cambió de nombre: antes `import dlt`, ahora
# `from pyspark import pipelines as dp`. Los decoradores son los mismos.
#
# Equivalencias SQL -> Python:
#
#   CREATE OR REFRESH STREAMING TABLE      @dp.table  + spark.readStream
#   CREATE OR REFRESH MATERIALIZED VIEW    @dp.table  + spark.read
#   CREATE TEMPORARY VIEW                  @dp.view
#
# O sea: en Python NO eliges el tipo de tabla con el decorador, sino con si lees
# en streaming o en batch. Es la diferencia más confusa entre las dos APIs.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# Los parámetros del pipeline (sección `configuration`) se leen con spark.conf.
# En SQL serían ${ruta.landing}.
LANDING = spark.conf.get("ruta.landing")


@dp.table(
    name="bronze_pedidos",
    comment="Pedidos crudos tal cual llegan del Volume, sin limpiar",
    table_properties={"quality": "bronze"},
)
def bronze_pedidos():
    return (
        spark.readStream                       # <- readStream => STREAMING TABLE
        .format("cloudFiles")                  # <- Auto Loader
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{LANDING}/pedidos")
        # `select` explícito con `*` más las columnas derivadas: el esquema de
        # salida se lee de un vistazo.
        # `_metadata` es una columna oculta: de qué fichero salió cada fila.
        # Capturarla en bronze es lo único que permite auditar el origen después.
        .select(
            "*",
            col("_metadata.file_name").alias("_fichero"),
            col("_metadata.file_modification_time").alias("_fichero_ts"),
            current_timestamp().alias("_ingesta_ts"),
        )
    )


@dp.table(
    name="bronze_clientes_cdc",
    comment="Eventos CDC de clientes, sin aplicar",
)
def bronze_clientes_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{LANDING}/clientes_cdc")
        .select("*", col("_metadata.file_name").alias("_fichero"))
    )
