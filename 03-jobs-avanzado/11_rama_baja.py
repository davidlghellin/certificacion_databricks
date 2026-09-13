# Databricks notebook source
# Rama FALSE del condition_task: se ejecuta si numero <= umbral.
dbutils.widgets.text("rama", "baja")

numero = dbutils.jobs.taskValues.get(
    taskKey="parametros", key="numero", default=-1, debugValue=1)

print(f"Rama {dbutils.widgets.get('rama').upper()} — numero = {numero}")
dbutils.jobs.taskValues.set(key="rama_ejecutada", value="baja")
dbutils.jobs.taskValues.set(key="numero_visto", value=numero)
