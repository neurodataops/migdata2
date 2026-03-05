# Databricks notebook source
# MAGIC %md
# MAGIC # Redshift → Databricks Ingestion Notebook
# MAGIC
# MAGIC Ingests Parquet files exported from Redshift (via `export_to_s3.py`) into
# MAGIC Delta tables on Databricks using `COPY INTO`.
# MAGIC
# MAGIC **Pipeline per table:**
# MAGIC 1. Create staging Delta table `stg.<schema>_<table>`
# MAGIC 2. `COPY INTO` staging from S3 Parquet path
# MAGIC 3. Deduplicate staging data
# MAGIC 4. Apply partitioning heuristic (date columns → partition-by)
# MAGIC 5. Create final Delta table `prod.<schema>_<table>`
# MAGIC 6. Record metrics in `ingestion_job_report.json`
# MAGIC
# MAGIC **Parameters (widgets):**
# MAGIC - `s3_bucket` — S3 bucket name
# MAGIC - `s3_prefix` — key prefix (default: `redshift_export`)
# MAGIC - `manifest_path` — path to `export_manifest.json` (dbfs or local)
# MAGIC - `catalog_path` — path to `source_catalog.json` (for column-type mapping)
# MAGIC - `aws_iam_role` — IAM role ARN for S3 access from Databricks
# MAGIC - `stg_database` — staging database (default: `stg`)
# MAGIC - `prod_database` — production database (default: `prod`)

# COMMAND ----------

import json
import time
from datetime import datetime, timezone

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("s3_bucket", "", "S3 Bucket")
dbutils.widgets.text("s3_prefix", "redshift_export", "S3 Prefix")
dbutils.widgets.text("manifest_path", "/dbfs/mnt/migration/artifacts/export_manifest.json", "Manifest Path")
dbutils.widgets.text("catalog_path", "/dbfs/mnt/migration/artifacts/source_catalog.json", "Catalog Path")
dbutils.widgets.text("aws_iam_role", "", "AWS IAM Role ARN")
dbutils.widgets.text("stg_database", "stg", "Staging Database")
dbutils.widgets.text("prod_database", "prod", "Production Database")

s3_bucket = dbutils.widgets.get("s3_bucket")
s3_prefix = dbutils.widgets.get("s3_prefix")
manifest_path = dbutils.widgets.get("manifest_path")
catalog_path = dbutils.widgets.get("catalog_path")
aws_iam_role = dbutils.widgets.get("aws_iam_role")
stg_database = dbutils.widgets.get("stg_database")
prod_database = dbutils.widgets.get("prod_database")

assert s3_bucket, "s3_bucket widget is required"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Manifest & Catalog

# COMMAND ----------

def load_json_file(path: str) -> dict:
    """Load a JSON file from DBFS or local path."""
    with open(path, "r") as f:
        return json.load(f)


manifest = load_json_file(manifest_path)
tables_to_ingest = manifest.get("tables", [])
print(f"Manifest loaded: {len(tables_to_ingest)} tables to ingest")

# Load catalog for column metadata (optional — graceful if missing)
catalog_columns = {}
try:
    catalog = load_json_file(catalog_path)
    for col in catalog.get("columns", []):
        schema = col.get("table_schema", "").lower()
        table = col.get("table_name", "").lower()
        key = f"{schema}.{table}"
        catalog_columns.setdefault(key, []).append(col)
    print(f"Catalog loaded: column metadata for {len(catalog_columns)} tables")
except Exception as e:
    print(f"Warning: Could not load catalog — proceeding without column metadata: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Redshift → Databricks Type Map

# COMMAND ----------

REDSHIFT_TO_SPARK_TYPE = {
    "boolean":                      "BOOLEAN",
    "bool":                         "BOOLEAN",
    "smallint":                     "SMALLINT",
    "int2":                         "SMALLINT",
    "integer":                      "INT",
    "int":                          "INT",
    "int4":                         "INT",
    "bigint":                       "BIGINT",
    "int8":                         "BIGINT",
    "real":                         "FLOAT",
    "float4":                       "FLOAT",
    "float":                        "DOUBLE",
    "float8":                       "DOUBLE",
    "double precision":             "DOUBLE",
    "numeric":                      "DECIMAL(38,10)",
    "decimal":                      "DECIMAL(38,10)",
    "character":                    "STRING",
    "char":                         "STRING",
    "nchar":                        "STRING",
    "bpchar":                       "STRING",
    "character varying":            "STRING",
    "varchar":                      "STRING",
    "nvarchar":                     "STRING",
    "text":                         "STRING",
    "date":                         "DATE",
    "timestamp":                    "TIMESTAMP",
    "timestamp without time zone":  "TIMESTAMP",
    "timestamp with time zone":     "TIMESTAMP",
    "timestamptz":                  "TIMESTAMP",
    "time":                         "STRING",
    "time without time zone":       "STRING",
    "timetz":                       "STRING",
    "super":                        "STRING",
    "varbyte":                      "BINARY",
    "bytea":                        "BINARY",
    "geometry":                     "STRING",
    "hllsketch":                    "BINARY",
}


def map_redshift_type(rs_type: str) -> str:
    """Map a Redshift data type to Spark SQL type."""
    rs_lower = rs_type.lower().strip()
    # Handle parameterised types: numeric(p,s) / decimal(p,s)
    if rs_lower.startswith(("numeric(", "decimal(")):
        return rs_lower.upper().replace("NUMERIC", "DECIMAL")
    # Handle varchar(n), char(n)
    for prefix in ("varchar", "char", "nvarchar", "nchar", "character varying",
                    "character", "bpchar"):
        if rs_lower.startswith(prefix):
            return "STRING"
    return REDSHIFT_TO_SPARK_TYPE.get(rs_lower, "STRING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Partitioning Heuristic

# COMMAND ----------

import re

DATE_COLUMN_PATTERNS = [
    re.compile(r".*_dt$", re.IGNORECASE),
    re.compile(r".*_date$", re.IGNORECASE),
    re.compile(r"^created_at$", re.IGNORECASE),
    re.compile(r"^updated_at$", re.IGNORECASE),
    re.compile(r"^event_date$", re.IGNORECASE),
    re.compile(r"^event_time$", re.IGNORECASE),
    re.compile(r"^load_date$", re.IGNORECASE),
    re.compile(r"^insert_date$", re.IGNORECASE),
    re.compile(r"^transaction_date$", re.IGNORECASE),
    re.compile(r"^order_date$", re.IGNORECASE),
]


def detect_partition_column(columns: list[dict]) -> str | None:
    """
    Detect the best partition column from column metadata.
    Returns the column name or None.
    """
    date_candidates = []
    for col in columns:
        col_name = col.get("column_name", "").lower()
        col_type = col.get("data_type", "").lower()

        # Only consider date/timestamp types
        if col_type not in ("date", "timestamp", "timestamp without time zone",
                            "timestamp with time zone", "timestamptz"):
            continue

        for pattern in DATE_COLUMN_PATTERNS:
            if pattern.match(col_name):
                date_candidates.append(col_name)
                break

    if date_candidates:
        # Prefer *_dt, then created_at, then first match
        for preferred in ("created_at", "event_date", "load_date"):
            if preferred in date_candidates:
                return preferred
        return date_candidates[0]

    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingestion Pipeline

# COMMAND ----------

def sanitise_table_name(schema: str, table: str) -> str:
    """Produce a safe Delta table name: schema_table."""
    s = re.sub(r"[^a-z0-9_]", "_", schema.lower())
    t = re.sub(r"[^a-z0-9_]", "_", table.lower())
    return f"{s}_{t}"


def build_column_spec(columns: list[dict]) -> str:
    """Build a column spec string for CREATE TABLE."""
    parts = []
    for col in sorted(columns, key=lambda c: int(c.get("ordinal_position", 0))):
        col_name = col["column_name"]
        spark_type = map_redshift_type(col.get("data_type", "STRING"))
        nullable = "" if col.get("is_nullable", "YES") == "YES" else " NOT NULL"
        parts.append(f"  `{col_name}` {spark_type}{nullable}")
    return ",\n".join(parts)


def ingest_table(
    schema: str,
    table: str,
    s3_path: str,
    columns: list[dict],
) -> dict:
    """
    Full ingestion pipeline for a single table.
    Returns a result dict with metrics.
    """
    table_name = sanitise_table_name(schema, table)
    stg_fqn = f"{stg_database}.{table_name}"
    prod_fqn = f"{prod_database}.{table_name}"

    result = {
        "source_schema": schema,
        "source_table": table,
        "stg_table": stg_fqn,
        "prod_table": prod_fqn,
        "s3_path": s3_path,
        "status": "pending",
        "rows_loaded": 0,
        "file_count": 0,
        "runtime_seconds": 0,
        "partition_column": None,
        "schema_mismatches": [],
        "error": None,
    }

    start = time.time()

    try:
        # ------------------------------------------------------------------
        # Step 1: Create staging database if needed
        # ------------------------------------------------------------------
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {stg_database}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {prod_database}")

        # ------------------------------------------------------------------
        # Step 2: Create staging table
        # ------------------------------------------------------------------
        if columns:
            col_spec = build_column_spec(columns)
            create_stg_sql = f"""
                CREATE TABLE IF NOT EXISTS {stg_fqn} (
                {col_spec}
                )
                USING DELTA
            """
        else:
            # No column metadata — let COPY INTO infer schema
            create_stg_sql = f"""
                CREATE TABLE IF NOT EXISTS {stg_fqn}
                USING DELTA
                LOCATION 'dbfs:/mnt/migration/staging/{table_name}'
            """

        spark.sql(create_stg_sql)

        # ------------------------------------------------------------------
        # Step 3: COPY INTO staging from S3 Parquet
        # ------------------------------------------------------------------
        credential_clause = ""
        if aws_iam_role:
            credential_clause = f"CREDENTIAL (AWS_IAM_ROLE = '{aws_iam_role}')"

        copy_sql = f"""
            COPY INTO {stg_fqn}
            FROM '{s3_path}'
            FILEFORMAT = PARQUET
            {credential_clause}
            FORMAT_OPTIONS ('mergeSchema' = 'true')
            COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'false')
        """

        copy_result = spark.sql(copy_sql)
        copy_metrics = copy_result.collect()

        # Parse COPY INTO metrics
        total_rows = 0
        total_files = 0
        for row in copy_metrics:
            total_rows += int(row["num_affected_rows"]) if "num_affected_rows" in row.asDict() else 0
            total_files += 1

        result["file_count"] = total_files

        # ------------------------------------------------------------------
        # Step 4: Deduplicate staging
        # ------------------------------------------------------------------
        stg_count_before = spark.sql(f"SELECT COUNT(*) AS cnt FROM {stg_fqn}").collect()[0]["cnt"]

        # Deduplicate by removing exact duplicate rows
        spark.sql(f"""
            CREATE OR REPLACE TABLE {stg_fqn} AS
            SELECT DISTINCT * FROM {stg_fqn}
        """)

        stg_count_after = spark.sql(f"SELECT COUNT(*) AS cnt FROM {stg_fqn}").collect()[0]["cnt"]
        dupes_removed = stg_count_before - stg_count_after
        if dupes_removed > 0:
            print(f"  Removed {dupes_removed} duplicate rows from {stg_fqn}")

        result["rows_loaded"] = stg_count_after

        # ------------------------------------------------------------------
        # Step 5: Detect partition column
        # ------------------------------------------------------------------
        partition_col = detect_partition_column(columns) if columns else None
        result["partition_column"] = partition_col

        # ------------------------------------------------------------------
        # Step 6: Create final production table
        # ------------------------------------------------------------------
        if partition_col:
            spark.sql(f"""
                CREATE OR REPLACE TABLE {prod_fqn}
                USING DELTA
                PARTITIONED BY (`{partition_col}`)
                AS SELECT * FROM {stg_fqn}
            """)
        else:
            spark.sql(f"""
                CREATE OR REPLACE TABLE {prod_fqn}
                USING DELTA
                AS SELECT * FROM {stg_fqn}
            """)

        # ------------------------------------------------------------------
        # Step 7: Schema mismatch detection
        # ------------------------------------------------------------------
        if columns:
            actual_schema = {
                f.name.lower(): str(f.dataType)
                for f in spark.table(prod_fqn).schema.fields
            }
            for col in columns:
                col_name = col["column_name"].lower()
                expected_type = map_redshift_type(col.get("data_type", "STRING"))
                actual_type = actual_schema.get(col_name)
                if actual_type is None:
                    result["schema_mismatches"].append({
                        "column": col_name,
                        "issue": "missing_in_target",
                        "expected": expected_type,
                        "actual": None,
                    })
                # Note: exact type comparison is complex due to Spark type
                # representations — log for manual review if needed

        result["status"] = "success"

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        print(f"  ERROR ingesting {schema}.{table}: {e}")

    result["runtime_seconds"] = round(time.time() - start, 2)
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Run Ingestion

# COMMAND ----------

print(f"Starting ingestion of {len(tables_to_ingest)} tables")
print(f"S3 source: s3://{s3_bucket}/{s3_prefix}/")
print(f"Staging DB: {stg_database}, Production DB: {prod_database}")
print("=" * 60)

ingestion_results = []
pipeline_start = time.time()

for entry in tables_to_ingest:
    schema = entry.get("schema", "public")
    table = entry.get("table", "unknown")
    s3_path = entry.get("s3_path", f"s3://{s3_bucket}/{s3_prefix}/{schema}/{table}/")

    print(f"\nIngesting {schema}.{table} from {s3_path} ...")

    # Get column metadata from catalog
    catalog_key = f"{schema.lower()}.{table.lower()}"
    columns = catalog_columns.get(catalog_key, [])

    result = ingest_table(schema, table, s3_path, columns)
    ingestion_results.append(result)

    print(
        f"  -> {result['status']} | "
        f"rows={result['rows_loaded']} | "
        f"files={result['file_count']} | "
        f"time={result['runtime_seconds']}s | "
        f"partition={result['partition_column'] or 'none'}"
    )

pipeline_runtime = round(time.time() - pipeline_start, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write Ingestion Report

# COMMAND ----------

succeeded = sum(1 for r in ingestion_results if r["status"] == "success")
failed = sum(1 for r in ingestion_results if r["status"] == "failed")
total_rows = sum(r["rows_loaded"] for r in ingestion_results)
total_files = sum(r["file_count"] for r in ingestion_results)
mismatch_count = sum(len(r["schema_mismatches"]) for r in ingestion_results)

report = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "pipeline_runtime_seconds": pipeline_runtime,
    "summary": {
        "total_tables": len(ingestion_results),
        "succeeded": succeeded,
        "failed": failed,
        "total_rows_loaded": total_rows,
        "total_files_processed": total_files,
        "schema_mismatches_detected": mismatch_count,
    },
    "tables": ingestion_results,
}

# Write to DBFS
report_path = "/dbfs/mnt/migration/artifacts/ingestion_job_report.json"
try:
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nIngestion report written to {report_path}")
except Exception:
    # Fallback: write via dbutils
    report_json = json.dumps(report, indent=2, default=str)
    dbutils.fs.put(
        "dbfs:/mnt/migration/artifacts/ingestion_job_report.json",
        report_json,
        overwrite=True,
    )
    print("\nIngestion report written to dbfs:/mnt/migration/artifacts/ingestion_job_report.json")

# Also write locally in artifacts/ if running outside Databricks
try:
    local_report_path = "/Workspace/migration/artifacts/ingestion_job_report.json"
    with open(local_report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary

# COMMAND ----------

print("=" * 60)
print("INGESTION COMPLETE")
print("=" * 60)
print(f"  Total tables       : {len(ingestion_results)}")
print(f"  Succeeded          : {succeeded}")
print(f"  Failed             : {failed}")
print(f"  Total rows loaded  : {total_rows:,}")
print(f"  Total files        : {total_files}")
print(f"  Schema mismatches  : {mismatch_count}")
print(f"  Pipeline runtime   : {pipeline_runtime}s")
print("=" * 60)

if failed > 0:
    print("\nFAILED TABLES:")
    for r in ingestion_results:
        if r["status"] == "failed":
            print(f"  - {r['source_schema']}.{r['source_table']}: {r['error']}")

if mismatch_count > 0:
    print("\nSCHEMA MISMATCHES:")
    for r in ingestion_results:
        for m in r.get("schema_mismatches", []):
            print(
                f"  - {r['source_schema']}.{r['source_table']}.{m['column']}: "
                f"expected={m['expected']}, actual={m['actual']}, issue={m['issue']}"
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix: Programmatic Execution via Databricks REST API
# MAGIC
# MAGIC ### Step-by-step shell commands:
# MAGIC
# MAGIC ```bash
# MAGIC # 1. Upload the notebook to Databricks workspace
# MAGIC databricks workspace import \
# MAGIC   --language PYTHON \
# MAGIC   --format SOURCE \
# MAGIC   --overwrite \
# MAGIC   databricks_ingest.py \
# MAGIC   /Workspace/migration/databricks_ingest
# MAGIC
# MAGIC # 2. Upload artifacts to DBFS
# MAGIC databricks fs cp artifacts/export_manifest.json dbfs:/mnt/migration/artifacts/export_manifest.json
# MAGIC databricks fs cp artifacts/source_catalog.json  dbfs:/mnt/migration/artifacts/source_catalog.json
# MAGIC
# MAGIC # 3. Run the notebook via Databricks Jobs API (one-time run)
# MAGIC curl -X POST "https://<databricks-host>/api/2.1/jobs/runs/submit" \
# MAGIC   -H "Authorization: Bearer <your-token>" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "run_name": "redshift-migration-ingest",
# MAGIC     "existing_cluster_id": "<cluster-id>",
# MAGIC     "notebook_task": {
# MAGIC       "notebook_path": "/Workspace/migration/databricks_ingest",
# MAGIC       "base_parameters": {
# MAGIC         "s3_bucket": "my-migration-bucket",
# MAGIC         "s3_prefix": "redshift_export",
# MAGIC         "aws_iam_role": "arn:aws:iam::123456789012:role/DatabricksS3Access",
# MAGIC         "stg_database": "stg",
# MAGIC         "prod_database": "prod"
# MAGIC       }
# MAGIC     }
# MAGIC   }'
# MAGIC
# MAGIC # 4. Check run status
# MAGIC curl -X GET "https://<databricks-host>/api/2.1/jobs/runs/get?run_id=<run_id>" \
# MAGIC   -H "Authorization: Bearer <your-token>"
# MAGIC
# MAGIC # 5. Download the ingestion report
# MAGIC databricks fs cp dbfs:/mnt/migration/artifacts/ingestion_job_report.json ./artifacts/
# MAGIC ```
# MAGIC
# MAGIC ### Python SDK alternative:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC run = w.jobs.submit_and_wait(
# MAGIC     run_name="redshift-migration-ingest",
# MAGIC     tasks=[{
# MAGIC         "task_key": "ingest",
# MAGIC         "existing_cluster_id": "<cluster-id>",
# MAGIC         "notebook_task": {
# MAGIC             "notebook_path": "/Workspace/migration/databricks_ingest",
# MAGIC             "base_parameters": {
# MAGIC                 "s3_bucket": "my-migration-bucket",
# MAGIC                 "s3_prefix": "redshift_export",
# MAGIC                 "aws_iam_role": "arn:aws:iam::123456789012:role/DatabricksS3Access",
# MAGIC                 "stg_database": "stg",
# MAGIC                 "prod_database": "prod",
# MAGIC             },
# MAGIC         },
# MAGIC     }],
# MAGIC )
# MAGIC print(f"Run completed: {run.state.result_state}")
# MAGIC ```
