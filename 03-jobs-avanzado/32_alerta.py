# Databricks notebook source
# Notebook reutilizado por DOS tasks distintas, cada una con su run_if y su
# base_parameters. La misma pieza de codigo, dos sitios del grafo.
#
#   alerta_fallo -> run_if = AT_LEAST_ONE_FAILED
#   todo_fallo   -> run_if = ALL_FAILED
#
# Las dos dependen de [inestable, parametros], y `parametros` siempre sale bien.
# Eso es lo que hace visible la diferencia: cuando `inestable` falla hay UNA
# fallida y UNA correcta, así que
#   AT_LEAST_ONE_FAILED -> basta con una  -> se EJECUTA
#   ALL_FAILED          -> las quiere todas -> queda EXCLUDED
# Con una sola dependencia serían indistinguibles.
dbutils.widgets.text("motivo", "sin motivo")
dbutils.widgets.text("run_url", "")

motivo = dbutils.widgets.get("motivo")
url = dbutils.widgets.get("run_url")

print(f"ALERTA: {motivo}")
if url:
    print(f"Run: {url}")

dbutils.notebook.exit(f"alerta:{motivo}")
