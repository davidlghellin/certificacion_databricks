# Databricks notebook source
# Cuerpo del for_each_task.
#
# El job.json pasa "item": "{{input}}". Databricks sustituye {{input}} por el
# elemento de la iteracion. Se lanza una task-run por elemento, hasta
# `concurrency` a la vez.
#
# Ojo: cada iteracion es una task-run distinta, asi que los taskValues que
# escriba aqui NO son legibles desde fuera del bucle de forma fiable.
dbutils.widgets.text("item", "?")
dbutils.widgets.text("entorno", "dev")

item = dbutils.widgets.get("item")
entorno = dbutils.widgets.get("entorno")

print(f"[{entorno}] procesando item = {item!r}  (len={len(item)})")

if item == "explota":
    raise ValueError("item invalido: 'explota'")

dbutils.notebook.exit(f"ok:{item}")
