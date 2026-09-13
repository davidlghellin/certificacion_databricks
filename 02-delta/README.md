# demo_delta — Delta Lake a fondo

Delta es el bloque más gordo del examen Professional junto con streaming. Este
demo lo recorre entero, **dos veces**: una en SQL y otra en Python, con el mismo
temario y la misma numeración, para que puedas comparar lado a lado.

```
demo_delta/
├── sql/     00…08 + 99      ← esquema demo_delta
├── python/  00…08 + 99      ← esquema demo_delta_py
├── job_sql.json             ← job que ejecuta los 10 notebooks SQL
└── job_python.json          ← job que ejecuta los 10 notebooks Python
```

Los dos jobs son **independientes** y escriben en esquemas distintos, así que
los datos no se pisan. Dentro de cada job las tasks sí corren en paralelo
(verificado). Lanzar **los dos jobs a la vez** no está probado: si ves
`RESOURCE_EXHAUSTED`, espera a que acabe uno.

## El temario

| # | Notebook | Qué cubre |
|---|---|---|
| **00** | `crear_tablas` | Managed vs external y qué se lleva un `DROP`. CTAS. `DESCRIBE TABLE/EXTENDED/DETAIL`. **El `_delta_log` por dentro**: acciones `add`/`remove`/`metaData`, checkpoints cada 10 commits. Cuándo NO particionar |
| **01** | `dml_merge` | `UPDATE`/`DELETE` y por qué reescriben ficheros enteros. `MERGE` con los **tres universos** (incluido `NOT MATCHED BY SOURCE`). SCD 1 y 2, deduplicación, idempotencia |
| **02** | `historial_time_travel` | `DESCRIBE HISTORY`. `VERSION AS OF` vs `TIMESTAMP AS OF`. Diffs entre versiones. `RESTORE` (no borra historia). Las **dos retenciones** que limitan el time travel |
| **03** | `esquema` | Schema enforcement. `mergeSchema` vs `overwriteSchema`. `ALTER TABLE` y qué se puede y qué no. **Column mapping** como requisito de `RENAME`/`DROP COLUMN`. Cambiar tipos |
| **04** | `constraints_columnas` | `NOT NULL` y `CHECK` (**se aplican**) vs `PRIMARY KEY`/`FOREIGN KEY` (**no**). Columnas generadas. `IDENTITY` `ALWAYS` vs `BY DEFAULT` |
| **05** | `optimize_vacuum` | Small files. `OPTIMIZE`, data skipping, `ZORDER`. **Liquid clustering** (lo que se usa hoy). Deletion vectors. `VACUUM` y cómo destruye el time travel. Predictive optimization |
| **06** | `change_data_feed` | `_change_type` con sus cuatro valores, el doble evento de un `UPDATE`. Propagar a silver con `MERGE`. Qué **no** genera feed |
| **07** | `clones` | `SHALLOW` vs `DEEP`, el peligro del shallow ante un `VACUUM`, el deep clone **incremental** como backup |
| **08** | `propiedades_concurrencia` | `TBLPROPERTIES` que hay que conocer. `appendOnly`. Versiones de protocolo. **Concurrencia optimista**: las cinco excepciones y cómo se curan. Niveles de aislamiento |
| **99** | `limpiar` | `DROP SCHEMA CASCADE` |

## Las diferencias SQL / Python que importan

| | SQL | Python |
|---|---|---|
| DML | `UPDATE` / `DELETE` / `MERGE INTO` | `DeltaTable.update()` / `.delete()` / `.merge()` |
| Historial | `DESCRIBE HISTORY t` | `dt.history()` → DataFrame filtrable |
| Time travel | `VERSION AS OF 2` | `.option("versionAsOf", 2)` |
| Evolución | `SET spark.databricks.delta.schema.autoMerge.enabled` | `.option("mergeSchema", "true")` |
| CDF | `table_changes('t', 1)` | `.option("readChangeFeed", "true")` |
| OPTIMIZE | `OPTIMIZE t ZORDER BY (c)` | `dt.optimize().executeZOrderBy("c")` |
| Constraints, clones, `ALTER` | nativo | **no hay API**: `spark.sql(...)` |

Esa última fila es la que más sorprende: constraints, columnas generadas,
identity y clones **solo existen en DDL**. Desde Python se hacen con
`spark.sql()`, no hay equivalente en la API de DataFrames.

## Desplegar y ejecutar

Los notebooks SQL llevan alguna celda `%python` (para mirar el `_delta_log`), así
que van como **notebook_task**, no como `sql_task`. Los dos jobs corren en
serverless, sin cluster declarado.

```sh
databricks auth login --host https://<tu-workspace>.cloud.databricks.com

databricks workspace mkdirs /demo_delta/sql
databricks workspace mkdirs /demo_delta/python

for f in sql/*.sql; do
  n=$(basename "$f" .sql)
  databricks workspace import "/demo_delta/sql/$n" \
    --file "$f" --language SQL --format SOURCE --overwrite
done

for f in python/*.py; do
  n=$(basename "$f" .py)
  databricks workspace import "/demo_delta/python/$n" \
    --file "$f" --language PYTHON --format SOURCE --overwrite
done

databricks jobs create --json @job_sql.json
databricks jobs create --json @job_python.json
```

Lanzar:

```sh
SQL_ID=$(databricks jobs list -o json \
  | jq -r '.[] | select(.settings.name=="demo-delta-sql") | .job_id')
PY_ID=$(databricks jobs list -o json \
  | jq -r '.[] | select(.settings.name=="demo-delta-python") | .job_id')

databricks jobs run-now $SQL_ID
databricks jobs run-now $PY_ID
```

Los dos jobs dejan las tablas creadas para que puedas curiosear. Si quieres que
limpien al terminar:

```sh
# Con --json, el job_id va DENTRO del JSON: no se puede pasar además como argumento
databricks jobs run-now --json "{\"job_id\": $SQL_ID, \"job_parameters\": {\"limpiar_al_final\": \"si\"}}"
```

La tarea `limpiar` cuelga de un `condition_task` con `run_if: ALL_SUCCESS`, así
que **solo borra si todo ha ido bien**: si algún notebook falla, las tablas se
quedan para que puedas investigar.

## Antes de lanzarlo

Los notebooks asumen el catálogo `main`. En Free Edition suele llamarse
`workspace`. Cámbialo en la primera celda de cada uno, o de una pasada:

```sh
# -i.bak funciona con el sed de macOS y con el de GNU/Linux
sed -i.bak 's/"main"/"workspace"/'    python/*.py && rm python/*.bak
sed -i.bak 's/USE CATALOG main/USE CATALOG workspace/' sql/*.sql && rm sql/*.bak
```

## Lo que NO está aquí

- **Ingesta** (`COPY INTO`, Auto Loader, `read_files`) → [01-ingesta](../01-ingesta/README.md)
- **Structured Streaming** sobre Delta → apéndice B de los apuntes
- **`AUTO CDC INTO`** y Declarative Pipelines → apéndices J y K
- **Unity Catalog** (permisos, volumes, views) → apéndice G
