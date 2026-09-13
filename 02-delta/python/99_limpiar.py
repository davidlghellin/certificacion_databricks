# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Limpiar (Python)
# MAGIC
# MAGIC Borra el esquema de la demo. Las tablas son **managed**, así que el `DROP SCHEMA
# MAGIC ... CASCADE` se lleva también los datos.
# MAGIC
# MAGIC Si alguna fuese **external**, el `DROP` borraría solo los metadatos y los ficheros
# MAGIC seguirían en su `LOCATION`.

# COMMAND ----------

CATALOGO, ESQUEMA = "main", "demo_delta_py"

tablas = spark.sql(f"SHOW TABLES IN {CATALOGO}.{ESQUEMA}").collect()
print(f"{len(tablas)} tablas a borrar:")
for t in tablas:
    print(" -", t["tableName"])

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {CATALOGO}.{ESQUEMA} CASCADE")
print("Esquema borrado")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOGO} LIKE 'demo_delta*'"))
