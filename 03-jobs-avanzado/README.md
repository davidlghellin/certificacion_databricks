# demo-task-advanced — parámetros, ramas, bucles y `run_if`

Job "kitchen sink" para tocar de una vez casi todo lo que la doc de Jobs deja
configurar. Todas las tasks son notebooks sin cluster declarado, así que corren
en **serverless** (vale en Free Edition).

Hay **dos jobs** sobre los mismos notebooks:

| Fichero | Tasks | Para qué |
|---|---|---|
| [job_basic.json](job_basic.json) | 2 | Lo mínimo que funciona. Empieza por aquí y ve añadiendo |
| [job.json](job.json) | 12 | El grafo completo, con todo lo de abajo |

## Empezar por lo básico

`job_basic.json` es solo `00_parametros` → `01_leer_valores`, y ya enseña las
tres piezas que sostienen todo lo demás: parámetros de job, widgets y
taskValues.

```sh
databricks jobs create --json @job_basic.json
```

Y a partir de ahí se amplía por capas, copiando bloques de `job.json`:

1. **Ramificar** — añade `condicion` (`condition_task`) + `rama_alta` / `rama_baja`,
   y `recoger` con `run_if: AT_LEAST_ONE_SUCCESS`.
2. **Iterar** — añade `bucle` (`for_each_task`) sobre `{{tasks.parametros.values.lista}}`.
3. **Fallar bien** — añade `inestable` con `max_retries`, y cuélgale `limpieza`
   (`ALL_DONE`), `alerta_fallo` (`AT_LEAST_ONE_FAILED`) y `todo_fallo` (`ALL_FAILED`).
4. **Cerrar** — añade `sin_fallos` (`NONE_FAILED`) y `resumen` (`ALL_DONE`).
5. **Operar** — `schedule`, `health`, `tags`, `timeout_seconds`, notificaciones.

Cada capa se aplica con `databricks jobs reset --job-id $JOB_ID --json @job_basic.json`
(sustituye la definición entera) sin tener que borrar y recrear el job.

## El grafo (job.json)

```
parametros ─┬─ condicion (IF/ELSE) ─┬─[true]── rama_alta ─┐
            │                       └─[false]─ rama_baja ─┼─ recoger    (AT_LEAST_ONE_SUCCESS)
            │                                             │
            ├─ bucle (for_each) ──────────────────────────┴─ sin_fallos (NONE_FAILED)
            │                                                ↑ ve las dos ramas directamente
            │
            ├─ inestable (falla random) ─┬─ limpieza (ALL_DONE) ── resumen (ALL_DONE)
            │                            │
            └────────────────────────────┴─ alerta_fallo (AT_LEAST_ONE_FAILED)
                                         └─ todo_fallo   (ALL_FAILED)
                                            ↑ las dos dependen de [inestable, parametros]
```

`resumen` depende también de `recoger` y `bucle`.

## Qué demuestra cada cosa

| Concepto | Dónde mirarlo |
|---|---|
| Parámetros de job | `job.json` → `parameters`, se leen como `{{job.parameters.x}}` |
| Parámetros de task → widgets | `base_parameters` → `dbutils.widgets.get()` en [00_parametros.py](00_parametros.py) |
| Valores dinámicos | `{{job.id}}`, `{{job.run_id}}`, `{{task.name}}`, `{{job.start_time.iso_date}}`, `{{job.trigger.type}}`, `{{workspace.url}}`, `{{job.repair_count}}` |
| taskValues (salida) | `dbutils.jobs.taskValues.set` en [00_parametros.py](00_parametros.py) |
| taskValues (entrada Python) | `dbutils.jobs.taskValues.get(..., default=)` en [40_resumen.py](40_resumen.py) |
| taskValues (entrada JSON) | `{{tasks.parametros.values.numero}}` en el `condition_task` |
| `condition_task` | task `condicion`, con `depends_on.outcome: "true"/"false"` |
| `for_each_task` | task `bucle`: `inputs` + `concurrency`, y `{{input}}` en [20_procesa_item.py](20_procesa_item.py) |
| Reintentos | `max_retries`, `min_retry_interval_millis`, `retry_on_timeout` en `inestable` |
| Timeouts | `timeout_seconds` a nivel de job y de task |
| Los 6 `run_if` | ver tabla de abajo |
| Schedule cron (en pausa) | `schedule` con `pause_status: PAUSED` |
| Health rule | `health` → aviso si el run pasa de 900 s |
| Tags, queue, concurrencia | `tags`, `queue`, `max_concurrent_runs` |
| Mismo notebook en dos tasks | `32_alerta` lo usan `alerta_fallo`, `todo_fallo` y `sin_fallos` |

## Los seis `run_if`

| Valor | Se ejecuta si… | Task de ejemplo |
|---|---|---|
| `ALL_SUCCESS` (por defecto) | todas las dependencias OK | `condicion`, `bucle` |
| `AT_LEAST_ONE_SUCCESS` | al menos una OK | `recoger` |
| `NONE_FAILED` | ninguna falló (tolera EXCLUDED si otra fue bien) | `sin_fallos` |
| `ALL_DONE` | todas terminaron, sea como sea | `limpieza`, `resumen` |
| `AT_LEAST_ONE_FAILED` | al menos una falló | `alerta_fallo` |
| `ALL_FAILED` | todas fallaron | `todo_fallo` |

La trampa clásica: al ramificar con `condition_task`, la rama no tomada queda
**EXCLUDED**, no FAILED. Y EXCLUDED **no cuenta como éxito**: si la task que junta
las ramas se deja con el `ALL_SUCCESS` por defecto, queda también EXCLUDED y no se
ejecuta. Por eso `recoger` usa `AT_LEAST_ONE_SUCCESS`.

### Cómo se comportan los estados, verificado en un workspace real

| Dependencias | `run_if` de la hija | Resultado de la hija |
|---|---|---|
| SUCCESS + EXCLUDED | `ALL_SUCCESS` | **EXCLUDED**, no se ejecuta |
| SUCCESS + EXCLUDED | `AT_LEAST_ONE_SUCCESS` | se ejecuta |
| SUCCESS + EXCLUDED | `NONE_FAILED` | se ejecuta |
| solo EXCLUDED | `ALL_SUCCESS` o `NONE_FAILED` | **EXCLUDED**: la exclusión se propaga |
| FAILED + EXCLUDED | `AT_LEAST_ONE_SUCCESS` | **UPSTREAM_FAILED** |
| UPSTREAM_FAILED | `NONE_FAILED` | UPSTREAM_FAILED |
| FAILED + SUCCESS | `AT_LEAST_ONE_FAILED` | se ejecuta |
| FAILED + SUCCESS | `ALL_FAILED` | **EXCLUDED** |

EXCLUDED no es ni éxito ni fallo: `ALL_SUCCESS` no lo acepta, y `NONE_FAILED` y
`AT_LEAST_ONE_SUCCESS` lo toleran solo si **otra** dependencia salió bien.

### Dos detalles de diseño que hacen visible cada `run_if`

**`AT_LEAST_ONE_FAILED` vs `ALL_FAILED`.** Con una sola dependencia son
indistinguibles: las dos se ejecutan exactamente cuando esa falla. Por eso
`alerta_fallo` y `todo_fallo` dependen de **`[inestable, parametros]`**, y
`parametros` siempre sale bien. Cuando `inestable` falla hay una fallida y una
correcta, y se ven lado a lado: `alerta_fallo` **se ejecuta** y `todo_fallo`
queda **EXCLUDED**.

**`NONE_FAILED` depende de las ramas para que se vea qué tolera.** `sin_fallos`
depende de `rama_alta`, `rama_baja` y `bucle`. En la ejecución normal una rama sale
bien y la otra queda EXCLUDED: `NONE_FAILED` se ejecuta igual, que es justo lo que
`ALL_SUCCESS` no haría. Si la rama tomada falla, `sin_fallos` queda
UPSTREAM_FAILED y no se ejecuta.

## Desplegar

```sh
databricks auth login --host https://<tu-workspace>.cloud.databricks.com

BASE=/demo_task_advanced
databricks workspace mkdirs "$BASE"

for f in *.py; do
  databricks workspace import "$BASE/${f%.py}" \
    --file "$f" --language PYTHON --format SOURCE --overwrite
done

databricks jobs create --json @job_basic.json    # o @job.json para el completo
```

Lanzar con los valores por defecto:

```sh
# El nombre depende del JSON que creaste:
#   job_basic.json -> demo-task-basic      job.json -> demo-task-advanced
NOMBRE=demo-task-advanced
JOB_ID=$(databricks jobs list -o json \
  | jq -r --arg n "$NOMBRE" '.[] | select(.settings.name==$n) | .job_id')

databricks jobs run-now $JOB_ID
```

Lanzar sobreescribiendo parámetros (aquí forzando la rama baja, más items y
que `inestable` no falle nunca):

```sh
# Con --json el job_id va DENTRO del JSON: pasarlo además como argumento da
#   "when --json flag is specified, no positional arguments are allowed"
databricks jobs run-now --json "{
  \"job_id\": $JOB_ID,
  \"job_parameters\": {
    \"umbral\": \"100\",
    \"items\": \"uno,dos,tres,cuatro\",
    \"prob_fallo\": \"0\",
    \"entorno\": \"pro\"
  }
}"
```

Para ver el camino de fallo entero, pon `"prob_fallo": "1"`: `inestable` gasta
sus 2 reintentos y se marca FAILED. Se ejecutan `limpieza` y `alerta_fallo`, y
`todo_fallo` queda **EXCLUDED a propósito**: depende también de `parametros`, que
siempre sale bien, así que `ALL_FAILED` no se cumple nunca. Es un ejemplo de
exclusión, para ver el contraste con `AT_LEAST_ONE_FAILED`; `ALL_FAILED` solo se
ejecutaría si fallaran **todas** sus dependencias. Verificado: el job termina en
`SUCCESS_WITH_FAILURES` con exactamente ese reparto.

Borrar:

```sh
databricks jobs delete $JOB_ID
```

## Cosas que no están y se añaden en dos líneas

**Notificaciones por email** (a nivel de job o de task):

```json
"email_notifications": {
  "on_start": [],
  "on_success": ["tu-email@ejemplo.com"],
  "on_failure": ["tu-email@ejemplo.com"],
  "on_duration_warning_threshold_exceeded": ["tu-email@ejemplo.com"]
},
"notification_settings": {
  "no_alert_for_skipped_runs": true,
  "no_alert_for_canceled_runs": true
}
```

**Llamar a otro job como task** (`run_job_task`), p. ej. al de `03-jobs-basico/`:

```json
{
  "task_key": "llamar_otro_job",
  "depends_on": [{ "task_key": "resumen" }],
  "run_job_task": {
    "job_id": 123456789,
    "job_parameters": { "entorno": "dev" }
  }
}
```

**Trigger por llegada de fichero** en vez de cron:

```json
"trigger": {
  "pause_status": "UNPAUSED",
  "file_arrival": {
    "url": "/Volumes/main/demo/landing/",
    "min_time_between_triggers_seconds": 60,
    "wait_after_last_change_seconds": 30
  }
}
```

**Task SQL** sobre un warehouse: ver [03-jobs-sql](../03-jobs-sql/).
