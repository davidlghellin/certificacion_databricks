# Databricks notebook source
# run_if = AT_LEAST_ONE_SUCCESS
#
# Depende de las DOS ramas, pero solo una llega a ejecutarse; la otra queda
# EXCLUDED. Con el run_if por defecto (ALL_SUCCESS) esta task quedaría también
# EXCLUDED y no se ejecutaría: EXCLUDED no cuenta como éxito. Verificado: es el
# error clásico al ramificar.
#
# Si la rama que SÍ se ejecuta falla, esta task queda UPSTREAM_FAILED (no
# EXCLUDED), y el fallo se propaga aguas abajo.
#
# Leer un taskValue de una task excluida lanza excepción, por eso `default=`.
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
