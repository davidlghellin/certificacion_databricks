# Databricks notebook source
# MAGIC %md
# MAGIC # Generador de datos de aterrizaje
# MAGIC
# MAGIC Los pipelines de este demo ingieren **ficheros JSON de un Volume** con Auto
# MAGIC Loader. Este notebook los fabrica.
# MAGIC
# MAGIC Escribe **un fichero por lote**, así que ejecutándolo varias veces con `lote`
# MAGIC distinto puedes ver el procesamiento **incremental**: el pipeline solo lee los
# MAGIC ficheros nuevos, nunca reprocesa los anteriores.
# MAGIC
# MAGIC Genera dos conjuntos:
# MAGIC
# MAGIC | Carpeta | Para qué |
# MAGIC |---|---|
# MAGIC | `pedidos/` | bronze → silver (expectations) → gold |
# MAGIC | `clientes_cdc/` | eventos CDC para `AUTO CDC INTO` (SCD 1 y 2) |

# COMMAND ----------

import json

dbutils.widgets.text("catalogo", "main")
dbutils.widgets.text("esquema_landing", "demo_pipeline")
dbutils.widgets.text("lote", "1")
dbutils.widgets.dropdown("filas_toxicas", "no", ["no", "si"])
dbutils.widgets.dropdown("reiniciar", "no", ["no", "si"])

CAT = dbutils.widgets.get("catalogo")
ESQ = dbutils.widgets.get("esquema_landing")
LOTE = int(dbutils.widgets.get("lote"))
TOXICAS = dbutils.widgets.get("filas_toxicas") == "si"
REINICIAR = dbutils.widgets.get("reiniciar") == "si"

BASE = f"/Volumes/{CAT}/{ESQ}/landing"
print(f"lote={LOTE}  filas_toxicas={TOXICAS}  reiniciar={REINICIAR}")
print(f"destino: {BASE}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{ESQ}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.{ESQ}.landing")

if REINICIAR:
    # Borra los ficheros para empezar de cero. Ojo: si reinicias la landing,
    # tienes que hacer un `full refresh` del pipeline, porque su checkpoint
    # sigue recordando los ficheros que ya leyó.
    try:
        dbutils.fs.rm(BASE, True)
        print("landing borrada")
    except Exception as e:
        print("nada que borrar:", str(e)[:100])

dbutils.fs.mkdirs(f"{BASE}/pedidos")
dbutils.fs.mkdirs(f"{BASE}/clientes_cdc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pedidos, con problemas de calidad a propósito
# MAGIC
# MAGIC Cada lote trae filas buenas y filas que violan alguna expectation:
# MAGIC
# MAGIC | Problema | Expectation que lo caza | Acción |
# MAGIC |---|---|---|
# MAGIC | `cliente_id` a NULL | `cliente_presente` | **DROP ROW** |
# MAGIC | `pais` fuera de ES/PT/FR | `pais_conocido` | solo avisa |
# MAGIC | `importe` <= 0 | `importe_positivo` | **FAIL UPDATE** |
# MAGIC
# MAGIC Las dos primeras salen siempre, para que veas métricas. La tercera solo si
# MAGIC pones `filas_toxicas = si`: entonces el pipeline **falla a propósito**.

# COMMAND ----------

base_id = LOTE * 100

pedidos = [
    # --- filas correctas ---
    {"pedido_id": base_id + 1, "cliente_id": 1, "pais": "ES", "importe": 120.50,
     "ts": f"2026-09-{LOTE:02d} 10:00:00"},
    {"pedido_id": base_id + 2, "cliente_id": 2, "pais": "PT", "importe": 75.00,
     "ts": f"2026-09-{LOTE:02d} 10:05:00"},
    {"pedido_id": base_id + 3, "cliente_id": 3, "pais": "FR", "importe": 240.00,
     "ts": f"2026-09-{LOTE:02d} 10:10:00"},
    {"pedido_id": base_id + 4, "cliente_id": 1, "pais": "ES", "importe": 33.30,
     "ts": f"2026-09-{LOTE:02d} 11:00:00"},

    # --- viola `cliente_presente` -> se DESCARTA la fila ---
    {"pedido_id": base_id + 5, "cliente_id": None, "pais": "ES", "importe": 99.00,
     "ts": f"2026-09-{LOTE:02d} 11:30:00"},

    # --- viola `pais_conocido` -> solo AVISA, la fila pasa igual ---
    {"pedido_id": base_id + 6, "cliente_id": 2, "pais": "XX", "importe": 15.00,
     "ts": f"2026-09-{LOTE:02d} 12:00:00"},
]

if TOXICAS:
    # --- viola `importe_positivo` -> el pipeline FALLA ---
    pedidos.append(
        {"pedido_id": base_id + 7, "cliente_id": 3, "pais": "ES", "importe": -50.00,
         "ts": f"2026-09-{LOTE:02d} 12:30:00"}
    )

ruta_pedidos = f"{BASE}/pedidos/pedidos_lote_{LOTE:03d}.json"
dbutils.fs.put(ruta_pedidos, "\n".join(json.dumps(p) for p in pedidos), overwrite=True)

print(f"{len(pedidos)} pedidos -> {ruta_pedidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Eventos CDC de clientes
# MAGIC
# MAGIC Aquí está la gracia del `AUTO CDC INTO`. El lote incluye a propósito:
# MAGIC
# MAGIC - **Dos eventos de la misma clave** (`id=1`). Un `MERGE` normal fallaría con
# MAGIC   *"multiple source rows matched"*; `AUTO CDC` los ordena por `SEQUENCE BY` y
# MAGIC   aplica el último.
# MAGIC - Un evento **fuera de orden**: llega después pero su `ts` es anterior, así que
# MAGIC   debe ser ignorado.
# MAGIC - Un **DELETE**, que `APPLY AS DELETE WHEN` traduce a borrado real.

# COMMAND ----------

if LOTE == 1:
    cdc = [
        {"id": 1, "nombre": "Ana",   "ciudad": "Madrid",  "operacion": "INSERT",
         "ts": "2026-09-01 09:00:00"},
        {"id": 2, "nombre": "Bruno", "ciudad": "Sevilla", "operacion": "INSERT",
         "ts": "2026-09-01 09:00:00"},
        {"id": 3, "nombre": "Carla", "ciudad": "Bilbao",  "operacion": "INSERT",
         "ts": "2026-09-01 09:00:00"},
    ]
else:
    cdc = [
        # Dos eventos del mismo cliente en el mismo lote: gana el ts más alto
        {"id": 1, "nombre": "Ana", "ciudad": "Valencia",  "operacion": "UPDATE",
         "ts": f"2026-09-{LOTE:02d} 08:00:00"},
        {"id": 1, "nombre": "Ana", "ciudad": "Barcelona", "operacion": "UPDATE",
         "ts": f"2026-09-{LOTE:02d} 20:00:00"},   # <- este gana

        # Fuera de orden: su ts es ANTERIOR al estado actual, debe ignorarse
        {"id": 2, "nombre": "Bruno", "ciudad": "Cadiz", "operacion": "UPDATE",
         "ts": "2026-08-01 00:00:00"},

        # Borrado
        {"id": 3, "nombre": None, "ciudad": None, "operacion": "DELETE",
         "ts": f"2026-09-{LOTE:02d} 21:00:00"},

        # Alta nueva
        {"id": 4, "nombre": "Diego", "ciudad": "Malaga", "operacion": "INSERT",
         "ts": f"2026-09-{LOTE:02d} 22:00:00"},
    ]

ruta_cdc = f"{BASE}/clientes_cdc/clientes_lote_{LOTE:03d}.json"
dbutils.fs.put(ruta_cdc, "\n".join(json.dumps(c) for c in cdc), overwrite=True)

print(f"{len(cdc)} eventos CDC -> {ruta_cdc}")

# COMMAND ----------

display(dbutils.fs.ls(f"{BASE}/pedidos"))

# COMMAND ----------

display(dbutils.fs.ls(f"{BASE}/clientes_cdc"))

# COMMAND ----------

# Lo que va a leer el pipeline
print("--- pedidos ---")
print(dbutils.fs.head(ruta_pedidos))
print("--- cdc ---")
print(dbutils.fs.head(ruta_cdc))
