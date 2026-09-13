# Databricks notebook source
# Notebook reutilizado por DOS tasks distintas, cada una con su run_if y su
# base_parameters. La misma pieza de codigo, dos sitios del grafo.
#
#   alerta_fallo -> run_if = AT_LEAST_ONE_FAILED
#   todo_fallo   -> run_if = ALL_FAILED
#
# Con una sola dependencia los dos run_if son equivalentes; se separan cuando
# hay varias: AT_LEAST_ONE_FAILED basta con una, ALL_FAILED las quiere todas.
dbutils.widgets.text("motivo", "sin motivo")
dbutils.widgets.text("run_url", "")

motivo = dbutils.widgets.get("motivo")
url = dbutils.widgets.get("run_url")

print(f"ALERTA: {motivo}")
if url:
    print(f"Run: {url}")

dbutils.notebook.exit(f"alerta:{motivo}")
