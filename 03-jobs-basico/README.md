# Databricks en NIX

Logearse en la CLI

```
databricks auth login --host https://<tu-workspace>.cloud.databricks.com
```

Supongamos que ponemos en base es la raiz

```sh
databricks workspace list /Workspace
```

```sh
for f in $(ls *.py | sed 's/.[^.]*$//'); do   databricks workspace import "$BASE/$f" --file "$f.py" --language PYTHON --format SOURCE --overwrite; done
```

Creamos job

```sh
databricks jobs create --json @job.json
```

obtener el id

```sh
JOB_ID=$(databricks jobs list -o json \
  | jq -r '.[] | select(.settings.name=="demo-diamante-saludo") | .job_id')
```

Lanzar

```sh
databricks jobs run-now $JOB_ID
```

borrar job

```sh
databricks jobs delete $JOB_ID
```
