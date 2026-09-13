# 05-bundles — el mismo job, pero como Asset Bundle (DAB)

Igual que [03-jobs-avanzado](../03-jobs-avanzado/) pero en YAML dentro de un
**Databricks Asset Bundle**. Es la forma que pide el examen en la parte de
DevOps/CI-CD: el job, los notebooks y la config por entorno viven en el repo y
se despliegan con un comando.

```
05-bundles/
├── databricks.yml                  ← raiz del bundle: nombre, variables, targets
├── resources/
│   └── demo_yml.job.yml            ← el job
└── src/
    ├── 00_preparar.py
    ├── 10_cargar_item.py
    ├── 20_reporte.py
    └── 30_cierre.py
```

## JSON suelto vs bundle

| | `databricks jobs create --json` | Asset Bundle |
|---|---|---|
| Notebooks | los subes tú a mano (`workspace import`) | los sube `bundle deploy` |
| Rutas | absolutas del workspace (`/demo_x/00_a`) | relativas del repo (`../src/00_a.py`) |
| Entornos | copias el JSON y editas | `targets:` dev / pro en el mismo fichero |
| Actualizar | `jobs reset` / `jobs update` | `bundle deploy` (idempotente) |
| Borrar | `jobs delete` | `bundle destroy` |

## Los dos tipos de sustitución (esto es lo que suele caer)

```yaml
esquema: ${var.esquema}                    # BUNDLE, se resuelve al DESPLEGAR
default: "{{job.parameters.esquema}}"      # JOB,    se resuelve al EJECUTAR
```

- `${...}` lo resuelve la CLI en el `deploy`. Queda **congelado** en el job.
  Fuentes: `${var.x}`, `${bundle.target}`, `${bundle.name}`,
  `${workspace.current_user.userName}`, `${resources.jobs.otro.id}`.
- `{{...}}` lo resuelve Databricks en cada **run**. Son los parámetros del job
  y los valores dinámicos (`{{job.run_id}}`, `{{tasks.x.values.y}}`, `{{input}}`).

Por eso los `parameters:` del job cogen su `default` de una variable de bundle:
el valor por defecto lo fija el entorno, pero se puede sobreescribir en cada
ejecución sin redesplegar.

## `mode: development` vs `mode: production`

`dev` usa `mode: development`, que automáticamente:

- prefija el nombre del job → `[dev david] demo-yml-dev`
- pone schedules y triggers en **PAUSED** (aunque el YAML diga `UNPAUSED`)
- despliega en tu carpeta personal `/Workspace/Users/<tú>/.bundle/...`
- fuerza `max_concurrent_runs`

`pro` usa `mode: production`: sin prefijo, schedule activo (vía
`presets.trigger_pause_status: UNPAUSED`) y `root_path` compartido.

## Usarlo

El `host` no va en el YAML: se toma de tu perfil del CLI.

```sh
databricks auth login --host https://<tu-workspace>.cloud.databricks.com

cd 05-bundles

databricks bundle validate                 # comprueba el YAML y resuelve ${...}
databricks bundle validate -t pro          # lo mismo para el otro target
databricks bundle deploy -t dev            # sube notebooks + crea/actualiza el job
databricks bundle run demo_yml_job         # lo lanza y sigue los logs
```

Sobreescribir variables del bundle (afecta al despliegue):

```sh
databricks bundle deploy -t dev --var="items=uno,dos,tres,cuatro,cinco"
```

Sobreescribir parámetros del job (afecta solo a esa ejecución):

```sh
databricks bundle run demo_yml_job --params umbral=2,esquema=pruebas
```

(`--params` separa por comas, así que un valor que lleve comas —como `items`—
hay que pasarlo por variable de bundle en el `deploy`, no por aquí.)

Ver qué quedó realmente desplegado y limpiar:

```sh
databricks bundle summary -t dev
databricks bundle destroy -t dev
```

## Empezar un bundle desde cero

```sh
databricks bundle init            # plantillas oficiales: default-python, dbt, etc.
```
