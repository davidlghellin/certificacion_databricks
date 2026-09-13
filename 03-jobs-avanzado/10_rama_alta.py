# Databricks notebook source
# Rama TRUE del condition_task: se ejecuta si numero > umbral.
# La otra rama queda en estado SKIPPED (no FAILED): eso importa para el run_if
# de las tasks que vienen despues.
dbutils.widgets.text("rama", "alta")

numero = dbutils.jobs.taskValues.get(
    taskKey="parametros", key="numero", default=-1, debugValue=99)

print(f"Rama {dbutils.widgets.get('rama').upper()} — numero = {numero}")
dbutils.jobs.taskValues.set(key="rama_ejecutada", value="alta")
dbutils.jobs.taskValues.set(key="numero_visto", value=numero)
