# Databricks notebook source
import transformaciones as T

tabla = dbutils.jobs.taskValues.get(
    taskKey="crear_datos", key="tabla", debugValue="main.demo_job.datos_demo")
esperadas = dbutils.jobs.taskValues.get(
    taskKey="crear_datos", key="filas_esperadas", debugValue=-1)

df = spark.table(tabla)               # ← el DataFrame se RECONSTRUYE aquí
reales = T.contar_filas(df)           # ← misma función, otro contexto

print(f"tabla={tabla}  esperadas={esperadas}  reales={reales}")

if reales != esperadas:
    raise Exception(f"Esperaba {esperadas} filas y hay {reales}")

print("Validación OK")