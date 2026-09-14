# Databricks notebook source
from datetime import datetime
from zoneinfo import ZoneInfo

fecha = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%d/%m/%y")
print(f"Fecha: {fecha}")
dbutils.jobs.taskValues.set(key="fecha", value=fecha)