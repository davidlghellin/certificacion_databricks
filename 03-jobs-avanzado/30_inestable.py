# Databricks notebook source
# Task que falla a proposito, con reintentos configurados en el job.json
# (max_retries / min_retry_interval_millis).
#
# Sirve para ver en la UI la diferencia entre:
#   - un intento fallido  -> se reintenta
#   - agotar reintentos   -> la task queda FAILED y dispara los run_if de abajo
import random

dbutils.widgets.text("prob_fallo", "0.4")
dbutils.widgets.text("intento", "0")   # {{job.repair_count}}

prob = float(dbutils.widgets.get("prob_fallo"))
reparaciones = dbutils.widgets.get("intento")

print(f"prob_fallo = {prob}   repair_count = {reparaciones}")

if random.random() < prob:
    # Cualquier excepcion no capturada marca la task como FAILED.
    raise RuntimeError(f"Fallo simulado (probabilidad {prob})")

print("Esta vez ha ido bien")
dbutils.jobs.taskValues.set(key="resultado", value="ok")
