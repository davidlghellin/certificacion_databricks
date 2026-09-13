# Databricks notebook source
# 00 - Punto de entrada.
#
# Demuestra las DOS formas de que entre informacion en una task:
#   1. Parametros del job  -> {{job.parameters.x}}  -> widget
#   2. Valores dinamicos   -> {{job.run_id}} y cia  -> widget
#
# Y la forma de que salga hacia otras tasks: dbutils.jobs.taskValues.set
import random

# --- 1. Parametros del job (job.json -> "parameters") ----------------
dbutils.widgets.text("entorno", "dev")
dbutils.widgets.text("umbral", "50")
dbutils.widgets.text("items", "alfa,beta,gamma")
dbutils.widgets.dropdown("modo", "rapido", ["rapido", "lento"])

# --- 2. Valores dinamicos (los rellena Databricks al lanzar) ---------
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("task_name", "")
dbutils.widgets.text("fecha_run", "")
dbutils.widgets.text("disparador", "")
dbutils.widgets.text("workspace_url", "")

g = dbutils.widgets.get

print("=== Parametros del job ===")
for k in ["entorno", "umbral", "items", "modo"]:
    print(f"  {k:15} = {g(k)}")

print("=== Valores dinamicos ===")
for k in ["job_id", "run_id", "task_name", "fecha_run", "disparador", "workspace_url"]:
    print(f"  {k:15} = {g(k)}")

# --- 3. Salida hacia las siguientes tasks ----------------------------
# Un taskValue tiene que ser serializable a JSON: str, int, float, bool,
# listas y diccionarios de esos. Un DataFrame NO.
numero = random.randint(0, 100)
lista = [x.strip() for x in g("items").split(",") if x.strip()]

dbutils.jobs.taskValues.set(key="numero", value=numero)
dbutils.jobs.taskValues.set(key="lista", value=lista)          # -> alimenta el for_each
dbutils.jobs.taskValues.set(key="entorno", value=g("entorno"))

print(f"\nnumero sorteado = {numero}  (umbral = {g('umbral')})")
print(f"lista para el bucle = {lista}")

# El valor de exit sale en la UI del run, pero NO lo pueden leer las
# tasks siguientes: para eso estan los taskValues.
dbutils.notebook.exit(f"numero={numero}")
