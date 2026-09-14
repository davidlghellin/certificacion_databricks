# 04-pipelines — Declarative Pipelines con expectations y AUTO CDC

Dos pipelines con el **mismo temario**, uno en SQL y otro en Python, más un job que
los orquesta. Todo desplegado como **bundle**.

```
04-pipelines/
├── databricks.yml              ← bundle: variables y targets dev/pro
├── resources/
│   ├── pipelines.yml           ← los dos pipelines
│   └── job.yml                 ← generar datos → pipeline SQL → pipeline Python → validar
└── src/
    ├── generar_datos.py        ← escribe JSON en un Volume, con filas malas a propósito
    ├── validar_resultado.py    ← mira las tablas y lee el event log
    ├── sql/                    ← 00_bronze · 01_silver_expectations · 02_gold · 03_auto_cdc
    └── python/                 ← los mismos cuatro, con la API `pyspark.pipelines`
```

## Qué demuestra

| Fichero | Concepto |
|---|---|
| `00_bronze` | Auto Loader (`read_files` / `cloudFiles`), columna `_metadata` |
| `01_silver_expectations` | las tres acciones: `EXPECT` (avisa), `DROP ROW`, `FAIL UPDATE`; y la **cuarentena** como complemento exacto |
| `02_gold` | materialized views, y expectations sobre agregados |
| `03_auto_cdc` | `AUTO CDC INTO` con SCD 1 y SCD 2 sobre los mismos eventos |

Los datos de entrada están preparados para que se vea cada comportamiento: una
fila con cliente nulo (se descarta), una con país desconocido (**pasa a silver**,
porque `EXPECT` sin `ON VIOLATION` no filtra), dos eventos CDC de la misma clave en
el mismo lote, uno fuera de orden y un borrado.

## Desplegar y ejecutar

El `host` no va en el YAML: se toma del perfil del CLI.

```sh
databricks auth login --host https://<tu-workspace>.cloud.databricks.com

cd 04-pipelines
databricks bundle validate
databricks bundle deploy -t dev
```

Todo de una vez, con el job orquestador:

```sh
databricks bundle run demo_pipeline_job -t dev
```

**La primera vez, lanza el job orquestador**: es lo único que genera los ficheros
de entrada (`generar_datos` es una task del job, no un recurso suelto). Si lanzas
un pipeline antes de que exista ningún fichero, bronze no tiene nada que leer y
`gold_control_calidad` falla su expectation `total_pedidos > 0`, que es de tipo
`FAIL UPDATE`.

Una vez hay datos en la landing, puedes lanzarlos pieza a pieza, que es más cómodo
para depurar:

```sh
databricks bundle run demo_pipeline_sql -t dev
databricks bundle run demo_pipeline_python -t dev
```

El segundo lote es el que trae los casos interesantes del CDC:

```sh
databricks bundle run demo_pipeline_job -t dev --params lote=2
```

Para ver fallar un pipeline a propósito por `ON VIOLATION FAIL UPDATE`:

```sh
databricks bundle run demo_pipeline_job -t dev --params filas_toxicas=si
```

## Empezar de cero

Un *full refresh* vacía las tablas del pipeline y **reprocesa todos los ficheros que
haya en la landing**, olvidando lo que el checkpoint recordaba. Los ficheros no se
borran, así que no hace falta regenerarlos. Si la landing estuviera vacía, lanza
antes el job orquestador para crearlos:

```sh
databricks bundle run demo_pipeline_sql    -t dev --full-refresh-all
databricks bundle run demo_pipeline_python -t dev --full-refresh-all
```

## Dos límites que condicionan el diseño

- **Los pipelines van en serie**, no en paralelo, aunque no dependan entre sí. Cada
  pipeline pide compute serverless propio, y dos a la vez dieron
  `RESOURCE_EXHAUSTED`. Por eso `job.yml` los encadena.
- **El target `dev` no deja el compute encendido** entre updates
  (`presets.pipelines_development: false`). El modo desarrollo lo retiene para
  iterar rápido, pero así bloquea al siguiente pipeline.

## Comprobaciones hechas contra el workspace

- Con `@dp.table` y `spark.read.table(...)`, las tablas quedan en Unity Catalog
  como **`MATERIALIZED_VIEW`**, igual que las de SQL. Con `readStream`, como
  `STREAMING_TABLE`.
- El orden de los ficheros en `libraries` **no** es el orden de ejecución: el motor
  infiere el grafo de las dependencias entre tablas.

Limpiar:

```sh
databricks bundle destroy -t dev
```
