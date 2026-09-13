# Databricks notebook source
# Notebook compartido por las dos ramas del condition_task: el `veredicto`
# llega por base_parameters, asi que no hace falta duplicar codigo.
dbutils.widgets.text("veredicto", "?")
dbutils.widgets.text("tabla", "main.demo_yml.eventos")

veredicto = dbutils.widgets.get("veredicto")
tabla = dbutils.widgets.get("tabla")

print(f"Veredicto: {veredicto}")
spark.table(tabla).orderBy("item").show(truncate=False)

dbutils.jobs.taskValues.set(key="veredicto", value=veredicto)
