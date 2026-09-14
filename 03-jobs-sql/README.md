# Databricks en NIX 

Logearse en la CLI
```
databricks auth login --host https://<tu-workspace>.cloud.databricks.com
```


Supongamos que ponemos en base es la raiz

```
databricks workspace list /Workspace
```

```
databricks workspace import "/00_merge.sql" --file 00_merge.sql --format AUTO --overwrite
databricks workspace import "/01_merge_into.sql" --file 01_merge_into.sql --format AUTO --overwrite
```

sacar el id del warehouse
```
databricks warehouses list -o json | jq -r '.[] | "\(.id)\t\(.name)\t\(.warehouse_type)\t\(.state)"
```

WH_ID=$(databricks warehouses list -o json   | jq -r '.[] | "\(.id)"')


Creamos job
```
databricks jobs create --json @job_sql.json
```

obtener el id 
```
JOB_ID=$(databricks jobs list -o json \
  | jq -r '.[] | select(.settings.name=="demo-sql-merge") | .job_id')
```

Lanzar
```
databricks jobs run-now $JOB_ID
```

borrar job
```
databricks jobs delete $JOB_ID
```