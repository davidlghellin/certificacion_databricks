# Databricks notebook source
# Prepara el esquema destino y publica lo que necesita el resto del grafo.
#
# Fijate en la diferencia:
#   ${var.esquema}           -> lo resuelve el BUNDLE al desplegar (queda fijo)
#   {{job.parameters.esquema}} -> lo resuelve el JOB al ejecutar (se puede cambiar)
dbutils.widgets.text("catalogo", "main")
dbutils.widgets.text("esquema", "demo_yml")
dbutils.widgets.text("items", "alfa,beta,gamma")
dbutils.widgets.text("target", "dev")
dbutils.widgets.text("run_id", "")

g = dbutils.widgets.get
cat, esq = g("catalogo"), g("esquema")
tabla = f"{cat}.{esq}.eventos"

print(f"target del bundle = {g('target')}   run_id = {g('run_id')}")
print(f"tabla destino     = {tabla}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{esq}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tabla} (
        item   STRING,
        n      INT,
        ts     TIMESTAMP
    )
""")

lista = [x.strip() for x in g("items").split(",") if x.strip()]

dbutils.jobs.taskValues.set(key="tabla", value=tabla)
dbutils.jobs.taskValues.set(key="lista", value=lista)
dbutils.jobs.taskValues.set(key="n_items", value=len(lista))

print(f"lista = {lista}  ({len(lista)} items)")
