# Databricks notebook source
hora = dbutils.jobs.taskValues.get(
    taskKey="obtener_hora", key="hora", debugValue="00:00")
usuario = dbutils.jobs.taskValues.get(
    taskKey="obtener_usuario", key="usuario", debugValue="nadie")
usuario = dbutils.jobs.taskValues.get(
    taskKey="obtener_fecha", key="fecha", debugValue="01/01/2000")

mensaje = f"Hola {usuario}, son las {hora} de "
print(mensaje)
dbutils.notebook.exit(mensaje)