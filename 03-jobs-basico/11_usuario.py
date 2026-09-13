# Databricks notebook source
email = spark.sql("SELECT current_user()").first()[0]
nombre = email.split("@")[0]
print(f"Usuario: {email}")
dbutils.jobs.taskValues.set(key="usuario", value=nombre)