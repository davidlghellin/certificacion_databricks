# Certificacion Databricks

## Lecture - Data Ingestion from Cloud Storage

Demonstrate how to ingest data from cloud object storage into Delta tables using CREATE TABLE AS, COPY INTO, and Auto Loader, including capturing input file metadata in Bronze layer tables

```sql
CREATE TABLE new_table AS
          SELECT *
          FROM read_files(
            <path_to_file(s)>,
            format => '<file_type>',
            <other_format_specific_options>
          );
```

CREATE TABLE AS (CTAS)
Supports reading file formats like:
| JSON | CSV | XML | TEXT | BINARYFILE | PARQUET | AVRO | ORC
Can detect the file format automatically and infer a unified schema across all files.
Specify specific file format options to read in the data based on the source file format.
Can be used in streaming tables to incrementally ingest files into Delta Lake using Auto Loader.


```sql
-- Sin columnas a propósito: es la tabla vacía de destino que documenta Databricks
-- para COPY INTO (verificado: SUCCEEDED en un SQL warehouse). El esquema lo pone
-- el primer COPY INTO, y para eso necesita 'mergeSchema' = 'true'.
CREATE TABLE new_table;

COPY INTO new_table
FROM '<dir_path>'
FILEFORMAT = <file_type>
FORMAT_OPTIONS (<options>)
COPY_OPTIONS ('mergeSchema' = 'true')
```

COPY INTO
Use the COPY INTO statement to copy files from cloud storage into the Delta table. This command performs a bulk load from files in cloud object storage into the table, and in this example, it will load files into the empty table new_table. The FROM clause specifies the location of the CSV files.

COPY INTO is ideal for situations where the cloud storage location is continuously adding files, since it is a retriable and idempotent operation designed for incremental batch ingestion.
Key aspects of COPY INTO:

Idempotent: Will skip any files that have already been loaded into the table; only new files will be ingested
File format support: Parquet, JSON, XML, and others
FROM clause: Specifies the path of the cloud storage location where new files are being continuously added
FORMAT_OPTIONS(): Controls how the source files are parsed and interpreted (options depend on file format)
COPY_OPTIONS(): Controls the behavior of the COPY INTO operation itself, such as:
Schema evolution using (mergeSchema)
Idempotency override using (force): by default COPY INTO is idempotent and skips
files already loaded; `'force' = 'true'` DISABLES that and reloads them.

Verified on a real SQL warehouse, loading the same folder of JSON files four times:

| Run | Result |
|---|---|
| Empty table, no `mergeSchema` | fails: `COPY_INTO_SCHEMA_MISMATCH_WITH_TARGET_TABLE` |
| With `'mergeSchema' = 'true'` | 12 rows loaded |
| Same command again | **0 rows loaded**, still 12: idempotent |
| With `'force' = 'true'` | 12 rows loaded again, **24 in total: duplicates** |

```python
        (spark
  .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "<checkpoint_path>")
    .load("/Volumes/catalog/schema/files")
  .writeStream
    .option("checkpointLocation", "<checkpoint_path>")
    .trigger(processingTime="5 seconds")
    .toTable("catalog.database.table")
)
```

Auto Loader with SQL (Declarative Pipelines)

```sql
CREATE OR REFRESH STREAMING TABLE
  catalog.schema.table
SCHEDULE EVERY 1 HOUR
AS
SELECT *
FROM STREAM read_files(
  '<dir_path>',
  format => '<file_type>'
)
```

AUTO LOADER
Incremental batch or streaming ingestion using Auto Loader.
Process new data files incrementally as they arrive in cloud storage (batch or streaming)
Ingest data without extra setup or complex configuration
Automatically detect and load new files into Delta tables
Simplify handling of incremental and streaming data
Use with both Python and SQL (via Declarative Pipelines)
Scale to process billions of files
Rely on Spark Structured Streaming for efficient and reliable ingestion

Auto Loader in Python to read streaming data from cloud storage:
We start with .readStream and set the format to "cloudFiles", which enables Auto Loader.
Then, we specify the file format as JSON, and define the schema location using cloudFiles.schemaLocation, which is used to track schema inference and evolution.
Next, we use .load() to point to the location of the files, in this case a path under /Volumes referencing Unity Catalog.
On the write side, we configure .writeStream with a checkpoint location to maintain state and progress, and set a trigger interval of every 5 seconds.
Finally, we use .toTable() to write the data into a Delta table specified by catalog, database, and table name.

Auto Loader with Databricks SQL
Databricks recommends using streaming tables to ingest data with Databricks SQL (instead of COPY INTO). A streaming table is a table registered to Unity Catalog that includes additional support for streaming or incremental data processing.
When you create a streaming table, a pipeline is automatically generated for it.
Streaming tables can be used for incremental data loading from both Kafka and cloud object storage.
To create a streaming table from files in a volume, you use Auto Loader. Databricks recommends using Auto Loader with Apache Spark™ Declarative Pipelines for most data ingestion tasks from cloud object storage. Together, Auto Loader and Declarative Pipelines are designed to incrementally and idempotently load continuously growing datasets as they arrive.
Streaming tables in Databricks SQL are backed by serverless Spark Declarative Pipelines. Your workspace must support serverless pipelines to use this functionality. Alternatively, you can build your own Spark Declarative Pipelines for incremental processing, optimization, and monitoring. Declarative Pipelines offer a range of additional features, which you can learn more about here.
To use Auto Loader in Databricks SQL, use the read_files function with the STREAM keyword in the FROM clause.


https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/
https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader

| FEATURE | CREATE TABLE AS (CTAS) + `spark.read` | COPY INTO | Auto Loader |
|----------|----------|----------|----------|
| **Ingestion Type** | Batch | Incremental Batch | Incremental (Batch or Streaming) |
| **Use Cases** | Best for smaller datasets | Ideal for thousands of files | Scale to millions+ of files per hour, backfills with billions of files |
| **Syntax / Interface** | Python (`spark.read`)<br>SQL (CTAS) | SQL | Python (`spark.readStream`)<br>SQL with Declarative Pipelines (`CREATE OR REFRESH STREAMING TABLE`)<br>Streaming Tables in Databricks SQL |
| **Idempotency** | No | Yes | Yes |
| **Schema Evolution** | Manual or inferred during read | Supported with options | Automatically detects and evolves schemas. Handles new columns as they appear. |
| **Latency** | High | Moderate (scheduled) | Low or high depending on configuration |
| **Ease of Use** | Simple | Simple and SQL-based | Intermediate to advanced depending on the implementation |
| **Summary** | Best for one-time, ad hoc ingestion. Can be scheduled to always read and process all data. | Simple and repeatable for incremental file ingestion. Great for scheduled jobs or pipelines. | Best for near real-time streaming or incremental ingestion, with high automation and scalability. |


D. Conclusion
In this lecture, you learned the three primary methods for ingesting data from cloud object storage into Delta tables:

**CREATE TABLE AS (CTAS)**: Batch ingestion using read_files() that creates Delta tables from raw files. Best for smaller, ad hoc datasets.

**COPY INTO**: Incremental batch ingestion that is idempotent and retriable. Skips already-loaded files and supports format and copy options for fine-grained control.

**AUTO LOADER**: The most scalable method, built on Spark Structured Streaming. Supports both Python and SQL (via Declarative Pipelines), processes billions of files, and automatically handles schema evolution.
