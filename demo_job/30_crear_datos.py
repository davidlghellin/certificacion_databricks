# Databricks notebook source
import transformaciones as T          # ← el .py hermano, mismo directorio

dbutils.widgets.text("catalogo", "main")
dbutils.widgets.text("esquema", "demo_job")

cat = dbutils.widgets.get("catalogo")
esq = dbutils.widgets.get("esquema")
tabla = f"{cat}.{esq}.datos_demo"

# 2 columnas, 2 filas
df = spark.createDataFrame([("ana", 10), ("bruno", 20)], "nombre STRING, valor INT")

df_norm = T.normalizar(df)            # ← le paso el DataFrame a la función del otro fichero
n = T.contar_filas(df_norm)

df_norm.show()
print(f"filas = {n}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{esq}")
df_norm.write.mode("overwrite").saveAsTable(tabla)

# Lo que cruza a la siguiente task NO es el DataFrame: es el nombre y el conteo
dbutils.jobs.taskValues.set(key="tabla", value=tabla)
dbutils.jobs.taskValues.set(key="filas_esperadas", value=n)