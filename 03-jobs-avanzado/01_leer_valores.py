# Databricks notebook source
# La segunda mitad del job minimo: consume lo que publico 00_parametros.
#
# Las dos vias por las que entra informacion aqui:
#   - base_parameters del job.json  -> widgets
#   - taskValues de la task anterior -> dbutils.jobs.taskValues.get
dbutils.widgets.text("entorno", "dev")
dbutils.widgets.text("run_id", "")

# taskKey es el "task_key" del job.json, NO el nombre del notebook.
numero = dbutils.jobs.taskValues.get(
    taskKey="parametros", key="numero", default=-1, debugValue=42)
lista = dbutils.jobs.taskValues.get(
    taskKey="parametros", key="lista", default=[], debugValue=["a", "b"])

print(f"entorno = {dbutils.widgets.get('entorno')}")
print(f"run_id  = {dbutils.widgets.get('run_id')}")
print(f"numero recibido de 'parametros' = {numero}")
print(f"lista  recibida de 'parametros' = {lista}")

dbutils.notebook.exit(f"leidos {len(lista)} items, numero={numero}")
