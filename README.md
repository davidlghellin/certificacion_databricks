# Certificación Databricks Data Engineer Professional

Repo de estudio **práctico**: cada carpeta es un demo desplegable y ejecutable en
un workspace real, no fragmentos de código. La numeración **es el orden de
estudio**, ordenada por peso en el examen.

Los apuntes teóricos están en [`apuntes-databricks/`](apuntes-databricks/)
(168 pág. + 175 preguntas). Este repo es la parte de manos.

## Los 10 dominios oficiales y dónde se practican

Pesos de `databricks.com/learn/certification/data-engineer-professional`
(59 preguntas, 120 min).

| Dominio | Peso | Demo | Estado |
|---|---|---|---|
| 1 · Developing Code (Python y SQL) | **22%** | [06-streaming](06-streaming/), [11-tests](11-tests/), [04-pipelines](04-pipelines/) | ⬜ streaming, ⬜ tests |
| 2 · Data Ingestion & Acquisition | 7% | [01-ingesta](01-ingesta/) | ⬜ multi-formato |
| 3 · Transformation, Cleansing & Quality | 10% | [04-pipelines](04-pipelines/), [02-delta](02-delta/) | ✅ |
| 4 · Data Sharing & Federation | 5% | [09-sharing-federation](09-sharing-federation/) | ⬜ |
| 5 · Monitoring & Alerting | 10% | [10-monitorizacion](10-monitorizacion/) | ⬜ |
| 6 · Cost & Performance | **13%** | [07-performance](07-performance/), [02-delta](02-delta/) | ⬜ shuffle/skew |
| 7 · Security & Compliance | 10% | [08-seguridad](08-seguridad/) | ⬜ |
| 8 · Data Governance | 7% | [08-seguridad](08-seguridad/) | ⬜ |
| 9 · Debugging & Deploying | 10% | [05-bundles](05-bundles/), [03-jobs-avanzado](03-jobs-avanzado/) | 🔶 falta CI/CD y repair |
| 10 · Data Modelling | 6% | [02-delta](02-delta/) | 🔶 falta modelo dimensional |

## Las carpetas

| Carpeta | Qué contiene | Cómo se despliega |
|---|---|---|
| [01-ingesta](01-ingesta/) | CTAS vs `COPY INTO` vs Auto Loader | teoría + notebooks |
| [02-delta](02-delta/) | **Delta a fondo**: 20 notebooks, SQL y Python en paralelo | 2 jobs JSON |
| [03-jobs-basico](03-jobs-basico/) | Jobs: taskValues, widgets, diamante de dependencias | job JSON |
| [03-jobs-sql](03-jobs-sql/) | `sql_task` sobre warehouse, `MERGE` con `IDENTIFIER(:param)` | job JSON |
| [03-jobs-avanzado](03-jobs-avanzado/) | `condition_task`, `for_each_task`, los 6 `run_if`, reintentos | 2 jobs JSON |
| [04-pipelines](04-pipelines/) | Declarative Pipelines: expectations, `AUTO CDC` SCD 1/2 | **bundle** |
| [05-bundles](05-bundles/) | DABs: variables, targets dev/pro, sustituciones | **bundle** |
| [06-streaming](06-streaming/) | Structured Streaming | — |
| [07-performance](07-performance/) | Shuffle, skew, spill, query profile | — |
| [08-seguridad](08-seguridad/) | Row filters, column masks, PII, GRANT | — |
| [09-sharing-federation](09-sharing-federation/) | Delta Sharing D2D/D2O, Federation | — |
| [10-monitorizacion](10-monitorizacion/) | System tables, SQL Alerts, event log | — |
| [11-tests](11-tests/) | `assertDataFrameEqual`, unitarios e integración | — |
| [99-delta-local](99-delta-local/) | Delta OSS en local, fuera de Databricks | `python` |

## Requisitos del workspace

Comprobado sobre un workspace real. Dos límites que condicionan todos los demos:

- **Serverless obligatorio.** Declarar `clusters:` con compute clásico lo rechaza
  el deploy: `INVALID_PARAMETER_VALUE: You must use serverless compute`.
- **Cuota de un solo workload serverless.** Dos cosas a la vez dan
  `RESOURCE_EXHAUSTED`. Por eso los jobs encadenan tasks **en serie**.
- **Serverless bloquea configs de Spark** (`CONFIG_NOT_AVAILABLE.SERVERLESS_*`):
  nada de `spark.conf.set` para comportamiento de Delta. Todo por `TBLPROPERTIES`
  o por opción de escritura.

## Arrancar

```sh
databricks auth login --host https://<tu-workspace>.cloud.databricks.com
```

Los demos con `job*.json` se despliegan con `workspace import` + `jobs create`
(ver el README de cada uno). Los que son bundle, con `databricks bundle deploy`.
