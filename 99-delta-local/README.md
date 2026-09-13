# 99-delta-local — Delta Lake fuera de Databricks

Un script mínimo para ver Delta Lake OSS en tu máquina: escribe una tabla Delta
en `/tmp/clientes` y la lee de vuelta.

## Requisitos

El entorno de `flake.nix` del repo **solo trae el CLI de Databricks**, no Spark.
Para este script hacen falta Java y dos paquetes de Python:

```sh
python3 -m venv .venv
.venv/bin/pip install pyspark delta-spark
.venv/bin/python demo_delta_local.py
```

`delta-spark` tiene que ser compatible con tu versión de `pyspark`. Si falla al
arrancar con un error de clases de Scala, fija versiones emparejadas según la
[tabla de compatibilidad de Delta](https://docs.delta.io/latest/releases.html).

## Por qué está en el repo

Para ver qué es de **Delta** y qué es de **Databricks**: el formato, el
`_delta_log` y el time travel funcionan aquí igual; Unity Catalog, serverless,
Predictive Optimization y los pipelines declarativos, no.
