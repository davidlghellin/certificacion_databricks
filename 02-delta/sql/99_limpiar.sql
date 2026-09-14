-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 99 · Limpiar
-- MAGIC
-- MAGIC Borra el esquema de la demo. Como las tablas son **managed**, el `DROP SCHEMA
-- MAGIC ... CASCADE` se lleva también los datos.

-- COMMAND ----------

USE CATALOG main;

DROP SCHEMA IF EXISTS demo_delta CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Si alguna tabla fuese **external**, el `DROP` habría borrado solo los metadatos
-- MAGIC y los ficheros seguirían en su `LOCATION`. Hay que ir a borrarlos aparte.
