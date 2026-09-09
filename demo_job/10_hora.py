# Databricks notebook source
from datetime import datetime
from zoneinfo import ZoneInfo

hora = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
print(f"Hora: {hora}")
dbutils.jobs.taskValues.set(key="hora", value=hora)