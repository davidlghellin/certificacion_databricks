# Databricks notebook source
# MAGIC %md
# MAGIC # Validar el resultado del pipeline
# MAGIC
# MAGIC Dos cosas:
# MAGIC
# MAGIC 1. Mirar las tablas que ha producido el pipeline.
# MAGIC 2. Leer el **event log**, que es donde viven las métricas de las expectations.
# MAGIC    Esto es lo que la gente no encuentra: el porcentaje de filas que falló una
# MAGIC    expectation **no está en la tabla**, está en el log del pipeline.

# COMMAND ----------

dbutils.widgets.text("catalogo", "main")
dbutils.widgets.text("esquema", "demo_pipeline_sql")

CAT = dbutils.widgets.get("catalogo")
ESQ = dbutils.widgets.get("esquema")

print(f"Validando {CAT}.{ESQ}")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CAT}.{ESQ}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Las tablas del medallón

# COMMAND ----------

for t in ["bronze_pedidos", "silver_pedidos", "silver_pedidos_cuarentena",
          "gold_ventas_por_pais", "gold_control_calidad"]:
    try:
        n = spark.table(f"{CAT}.{ESQ}.{t}").count()
        print(f"{t:32} {n:5} filas")
    except Exception as e:
        print(f"{t:32} ERROR: {str(e)[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze vs silver: la diferencia son las expectations
# MAGIC
# MAGIC En bronze están todas las filas del fichero. En silver faltan las que violaron
# MAGIC un `DROP ROW`, y **sí está** la que solo violó un `EXPECT` a secas (el país
# MAGIC `XX`), porque esa expectation avisa pero no filtra.

# COMMAND ----------

display(spark.table(f"{CAT}.{ESQ}.bronze_pedidos").orderBy("pedido_id"))

# COMMAND ----------

display(spark.table(f"{CAT}.{ESQ}.silver_pedidos").orderBy("pedido_id"))

# COMMAND ----------

# La fila con pais='XX' pasó el filtro: `EXPECT` sin ON VIOLATION no descarta nada
display(spark.sql(f"SELECT * FROM {CAT}.{ESQ}.silver_pedidos WHERE pais NOT IN ('ES','PT','FR')"))

# COMMAND ----------

display(spark.table(f"{CAT}.{ESQ}.silver_pedidos_cuarentena"))

# COMMAND ----------

display(spark.table(f"{CAT}.{ESQ}.gold_ventas_por_pais").orderBy("pais"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado del AUTO CDC
# MAGIC
# MAGIC Comprueba aquí las cuatro cosas que el generador metió a propósito:
# MAGIC
# MAGIC | Cliente | Qué debe haber pasado |
# MAGIC |---|---|
# MAGIC | `id=1` | dos UPDATE en el mismo lote → gana el de `ts` mayor (Barcelona) |
# MAGIC | `id=2` | UPDATE con `ts` anterior → **ignorado**, sigue en Sevilla |
# MAGIC | `id=3` | DELETE → desaparece del SCD 1, se **cierra** en el SCD 2 |
# MAGIC | `id=4` | INSERT nuevo → aparece en ambas |

# COMMAND ----------

print("SCD 1 — solo el estado actual:")
display(spark.table(f"{CAT}.{ESQ}.dim_cliente_scd1").orderBy("id"))

# COMMAND ----------

print("SCD 2 — con historia. __END_AT NULL = version vigente")
display(spark.table(f"{CAT}.{ESQ}.dim_cliente_scd2").orderBy("id", "__START_AT"))

# COMMAND ----------

# El id=3, borrado: en SCD1 no está, pero en SCD2 su historia se conserva cerrada.
# Un DELETE no borra el pasado.
display(spark.sql(f"""
    SELECT 'scd1' AS tabla, id, ciudad, NULL AS __START_AT, NULL AS __END_AT
    FROM {CAT}.{ESQ}.dim_cliente_scd1 WHERE id = 3
    UNION ALL
    SELECT 'scd2', id, ciudad, CAST(__START_AT AS STRING), CAST(__END_AT AS STRING)
    FROM {CAT}.{ESQ}.dim_cliente_scd2 WHERE id = 3
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## El event log: métricas de las expectations
# MAGIC
# MAGIC Cada pipeline publica su event log. Desde 2025 se puede declarar como tabla en
# MAGIC Unity Catalog (`event_log` en los settings del pipeline), y si no, se consulta
# MAGIC con la función `event_log()` pasándole el id del pipeline.
# MAGIC
# MAGIC Los eventos de tipo `flow_progress` traen, en `details`, el bloque
# MAGIC `data_quality.expectations` con **nombre, filas que pasaron y filas que
# MAGIC fallaron** por cada expectation y cada ejecución.

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "")
PIPELINE_ID = dbutils.widgets.get("pipeline_id")

if not PIPELINE_ID:
    print("Sin pipeline_id: sáltate esta parte o pásalo como parámetro.")
else:
    log = spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")
    log.createOrReplaceTempView("evlog")
    display(spark.sql("""
        SELECT timestamp, event_type, message
        FROM evlog
        ORDER BY timestamp DESC
        LIMIT 20
    """))

# COMMAND ----------

# Las métricas de calidad, desanidadas
if PIPELINE_ID:
    display(spark.sql("""
        SELECT
          timestamp,
          details:flow_progress.status                    AS estado,
          exp.name                                        AS expectation,
          exp.dataset                                     AS tabla,
          exp.passed_records                              AS filas_ok,
          exp.failed_records                              AS filas_ko
        FROM evlog
        LATERAL VIEW explode(
          from_json(
            details:flow_progress.data_quality.expectations,
            'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
          )
        ) t AS exp
        WHERE details:flow_progress.data_quality.expectations IS NOT NULL
        ORDER BY timestamp DESC
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC Si `filas_ko` es 0 en todas, tu generador no metió filas malas. Con los datos
# MAGIC por defecto deberías ver:
# MAGIC
# MAGIC - `pais_conocido` → 1 fila fallada por lote (la de `XX`), **pero presente en silver**
# MAGIC - `cliente_presente` → 1 fila fallada por lote, **descartada**
# MAGIC - `importe_positivo` → 0 falladas (si hubiera una, el pipeline habría muerto)
