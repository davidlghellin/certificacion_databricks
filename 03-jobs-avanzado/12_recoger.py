# Databricks notebook source
# run_if = AT_LEAST_ONE_SUCCESS
#
# Depende de las DOS ramas, pero solo una llega a ejecutarse; la otra queda
# SKIPPED. Con el run_if por defecto (ALL_SUCCESS) esta task tambien se
# saltaria, que es el error clasico al ramificar.
#
# Leer un taskValue de una task saltada lanza excepcion, por eso `default=`.
def leer(task, clave, por_defecto):
    try:
        return dbutils.jobs.taskValues.get(
            taskKey=task, key=clave, default=por_defecto, debugValue=por_defecto)
    except Exception as e:
        print(f"  ({task}.{clave} no disponible: {type(e).__name__})")
        return por_defecto

alta = leer("rama_alta", "rama_ejecutada", None)
baja = leer("rama_baja", "rama_ejecutada", None)

rama = alta or baja or "ninguna"
print(f"La rama que corrio fue: {rama}")

dbutils.jobs.taskValues.set(key="rama", value=rama)
