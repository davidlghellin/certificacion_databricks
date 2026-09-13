# Databricks notebook source
# run_if = ALL_DONE
#
# Se ejecuta pase lo que pase con la dependencia: exito, fallo o salto.
# Es el patron "finally" de un job: borrar temporales, soltar locks, avisar.
resultado = "desconocido"
try:
    resultado = dbutils.jobs.taskValues.get(
        taskKey="inestable", key="resultado", default="no_publicado",
        debugValue="debug")
except Exception as e:
    print(f"(no hay taskValue: {type(e).__name__})")

print(f"Limpieza ejecutada. Estado de 'inestable' segun su taskValue: {resultado}")
print("Si pone 'no_publicado', la task de arriba fallo antes de publicarlo.")

dbutils.jobs.taskValues.set(key="limpieza", value="hecha")
