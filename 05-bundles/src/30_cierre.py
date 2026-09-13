# Databricks notebook source
# run_if: ALL_DONE. Depende de las dos ramas, de las que una siempre esta
# SKIPPED, asi que las lecturas van con `default=`.
import json

dbutils.widgets.text("tabla", "main.demo_yml.eventos")
dbutils.widgets.text("run_id", "")


def leer(task, clave, por_defecto=None):
    try:
        return dbutils.jobs.taskValues.get(
            taskKey=task, key=clave, default=por_defecto, debugValue=por_defecto)
    except Exception:
        return por_defecto


tabla = dbutils.widgets.get("tabla")
veredicto = leer("muchos", "veredicto") or leer("pocos", "veredicto") or "?"

resumen = {
    "run_id": dbutils.widgets.get("run_id"),
    "tabla": tabla,
    "veredicto": veredicto,
    "filas": spark.table(tabla).count(),
}

print(json.dumps(resumen, indent=2, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(resumen, ensure_ascii=False))
