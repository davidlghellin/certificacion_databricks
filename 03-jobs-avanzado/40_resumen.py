# Databricks notebook source
# run_if = ALL_DONE, y depende de casi todo el grafo.
#
# Recoge los taskValues de las tasks anteriores. Como algunas pueden estar
# saltadas o falladas, cada lectura va con `default=`.
import json


def leer(task, clave, por_defecto=None):
    try:
        return dbutils.jobs.taskValues.get(
            taskKey=task, key=clave, default=por_defecto, debugValue=por_defecto)
    except Exception as e:
        print(f"  ({task}.{clave}: {type(e).__name__})")
        return por_defecto


dbutils.widgets.text("run_id", "")
dbutils.widgets.text("entorno", "dev")

resumen = {
    "run_id": dbutils.widgets.get("run_id"),
    "entorno": dbutils.widgets.get("entorno"),
    "numero": leer("parametros", "numero", -1),
    "lista": leer("parametros", "lista", []),
    "rama": leer("recoger", "rama", "?"),
    "inestable": leer("inestable", "resultado", "fallo_o_saltada"),
    "limpieza": leer("limpieza", "limpieza", "no_ejecutada"),
}

print(json.dumps(resumen, indent=2, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(resumen, ensure_ascii=False))
