# Databricks notebook source
from datetime import datetime
print("Arrancando el job")
dbutils.jobs.taskValues.set(key="arranque", value=datetime.now().isoformat())
