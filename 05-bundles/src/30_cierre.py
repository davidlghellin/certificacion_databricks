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

# Esta task corre con `run_if: ALL_DONE`, o sea también cuando `preparar` falló y
# la tabla ni existe. Si el conteo reventara, taparía el error original.
try:
    filas = spark.table(tabla).count()
except Exception as e:
    filas = f"no disponible ({type(e).__name__})"

resumen = {
    "run_id": dbutils.widgets.get("run_id"),
    "tabla": tabla,
    "veredicto": veredicto,
    "filas": filas,
}

print(json.dumps(resumen, indent=2, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(resumen, ensure_ascii=False))
