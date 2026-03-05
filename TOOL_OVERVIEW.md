# MigData — Any Source to Any Target — Overview

## What This Toolkit Does

This toolkit automates the end-to-end migration of a data warehouse from any supported source to any supported target. It currently supports **AWS Redshift** and **Snowflake** as source platforms and **Databricks** as a target platform (more targets coming). It discovers everything in your source warehouse, converts SQL and schemas to target-compatible formats, moves the data, validates that nothing was lost, and gives you a live dashboard to track the whole process.

Both source and target platforms are configurable. The source is selected via `source.adapter` and the target via `target.platform` in `config.yaml`. These can also be switched from the dashboard sidebar without editing config files. The mock simulator includes adapters for both source platforms (`mock_redshift` and `mock_snowflake`), each with platform-specific catalog metadata, SQL dialects, and conversion rules. The pipeline, test runner, and dashboard all adapt automatically — every UI label reflects the selected source and target dynamically.

The toolkit is organized into three phases that run sequentially, producing artifacts that feed into the next phase.

---

## Phase 1 — Discovery

### 1a. Metadata Extractor — Redshift (`metadata_extractor.py`)

Connects to your Redshift cluster and builds a complete inventory of everything that lives there — every table (with row counts and disk sizes), every column and its data type, all constraints (primary keys, foreign keys, unique), stored procedures, user-defined functions, standard views, and materialized views.

This inventory becomes the foundation that every other tool in the toolkit reads from.

**Produces:** A full source catalog (JSON) and a one-page summary (CSV).

### 1b. Mock Source Adapter — Redshift (`src/mock_redshift.py`)

Generates a synthetic Redshift-style catalog without requiring a real cluster. Produces 5 schemas, 25 tables (~213 columns), 32 constraints, 3 PL/pgSQL stored procedures, 2 UDFs, 3 views, and ~682 query log entries spanning 90 days. Table metadata includes Redshift-specific properties: `diststyle`, `sortkey1`, `encoded`, `pct_used`. Column types use Redshift conventions (`VARCHAR`, `INTEGER`, `TIMESTAMP`, `BOOLEAN`).

**Produces:** `mock_data/source_catalog.json` and `mock_data/query_logs.json`.

### 1c. Mock Source Adapter — Snowflake (`src/mock_snowflake.py`)

Generates a synthetic Snowflake-style catalog without requiring a real account. Produces the same 5 schemas and 25 tables but with Snowflake-specific metadata: `cluster_by`, `auto_clustering`, `retention_time`, `transient`. Column types use Snowflake conventions (`NUMBER(38,0)`, `VARIANT`, `TIMESTAMP_NTZ`, `ARRAY`, `OBJECT`, `GEOGRAPHY`). Stored procedures use Snowflake Scripting (SQL) and JavaScript. Query templates use `IFF`, `LATERAL FLATTEN`, `QUALIFY`, `::` casting, and `ARRAY_AGG ... WITHIN GROUP`.

**Produces:** `mock_data/source_catalog.json` and `mock_data/query_logs.json`.

---

### 2. Workload Analyzer (`workload_analyzer.py`)

Reads Redshift's query history logs to understand how the warehouse is actually being used. It answers questions like: How many queries run per day? Which queries take the longest? Which SQL patterns are repeated most often? How complex is each query pattern?

Queries are "fingerprinted" — two queries that are logically identical but have different parameter values (e.g., different dates or IDs) are grouped together under one fingerprint. Each fingerprint gets a complexity score based on how many JOINs, window functions, subqueries, CTEs, and UDF references it uses.

The analyzer also maps each query pattern to its likely owner (the user or team that runs it most), flagging cases where ownership is ambiguous and needs human review.

**Produces:** Workload summary (JSON), top queries (CSV), interactive query volume chart (HTML), owner mapping (JSON), and an uncertain-ownership report (CSV) for manual triage.

---

### 3. Relationship Profiler (`relationship_profiler.py`)

Many Redshift warehouses have implicit foreign key relationships — columns like `customer_id` that reference another table's `id` column, but without a declared FK constraint. This tool finds them.

It scans all columns for naming patterns that suggest relationships (`*_id`, `*_key`, `*_code`, etc.), matches them to candidate parent tables, then runs profiling queries against Redshift to measure how well the relationship holds: What percentage of child values exist in the parent? Is the parent column unique? How many orphan rows are there?

Each candidate is classified as highly likely, likely, possible, or unlikely. All candidates — even highly likely ones — are flagged for human confirmation before being created in Databricks.

**Produces:** Ranked candidate list (CSV + JSON) and ready-to-run verification SQL for each candidate.

---

## Phase 2 — Conversion & Data Movement

### 4a. SQL Transpiler — Redshift (`transpile.py` + `sql_transpiler_service.py` + `src/mock_converter.py`)

Translates Redshift SQL and DDL into Databricks-compatible SQL. The transpiler applies 30 deterministic rewrite rules that handle the most common differences between the two platforms — removing Redshift-specific clauses (DISTKEY, SORTKEY, ENCODE), converting data types (VARCHAR → STRING), translating functions (NVL → COALESCE, GETDATE → CURRENT_TIMESTAMP), and restructuring syntax (IDENTITY columns, LISTAGG, CONVERT_TIMEZONE).

Every object is classified into one of three categories:
- **Auto-convert** — fully handled by rules, ready to deploy
- **Convert with warnings** — rule-based but contains uncertain transforms that should be reviewed
- **Manual rewrite required** — stored procedures, PL/pgSQL, cursor logic, and other constructs that have no direct Databricks equivalent

Each object also gets a difficulty score (1–10) to help teams prioritize their manual work.

A placeholder integration point is provided for connecting an LLM to assist with complex translations that rules alone cannot handle.

**Produces:** Converted SQL files with header annotations, and a per-object conversion report (JSON) with diffs, applied rules, warnings, and difficulty scores.

### 4b. SQL Transpiler — Snowflake (`src/mock_snowflake_converter.py`)

Translates Snowflake SQL and DDL into Databricks-compatible SQL using 30 Snowflake-specific rewrite rules. Handles Snowflake-to-Spark differences including: removing `CLUSTER BY`, `TRANSIENT`, and `DATA_RETENTION_TIME_IN_DAYS`; converting types (`NUMBER(38,0)` → `BIGINT`, `VARIANT` → `STRING`, `TIMESTAMP_NTZ` → `TIMESTAMP`); translating functions (`IFF` → `IF`, `NVL` → `COALESCE`, `PARSE_JSON` → `FROM_JSON`, `ARRAY_AGG` → `COLLECT_LIST`, `LISTAGG` → `CONCAT_WS`); and restructuring syntax (`::` casts → `CAST()`, `LATERAL FLATTEN` → `LATERAL VIEW EXPLODE`).

Manual rewrite patterns include: JavaScript and Python stored procedures, `CREATE STREAM`, `CREATE TASK`, `CREATE STAGE`, `CREATE FILE FORMAT`, `COPY INTO`, `QUALIFY`, `MATCH_RECOGNIZE`, and time travel queries (`AT`/`BEFORE`).

**Produces:** Same output format as the Redshift transpiler — converted SQL files and a per-object conversion report (JSON).

---

### 5. Bulk Data Export (`export_to_s3.py`)

Generates and optionally executes Redshift UNLOAD commands to export every table as Parquet files to S3. Supports dry-run mode (generates the scripts for review without executing), IAM role authentication, configurable file sizes, and retry with backoff for transient failures.

**Produces:** Export manifest (JSON) tracking each table's S3 path and status, and a consolidated UNLOAD script for review.

---

### 6. Databricks Ingestion Notebook (`databricks_ingest.py`)

A Databricks notebook (PySpark) that loads the exported Parquet files from S3 into Delta Lake tables. For each table it: loads into a staging table using COPY INTO (idempotent — skips already-loaded files), deduplicates, auto-detects a date partition column by naming convention, creates the production Delta table with partitioning, and checks for schema mismatches between expected and actual column definitions.

**Produces:** Per-table ingestion report (JSON) with rows loaded, file counts, runtime, partition column used, and any schema mismatches.

---

### 7. DDL Deployer (`ddl_deployer.py` + `deploy.sh`)

Reads the transpiled SQL, source catalog constraints, and inferred FK candidates to produce deployment-ready DDL for Databricks. It generates an ordered set of SQL files: databases first, then tables, views, and constraints.

Constraint handling follows Databricks/Delta Lake conventions:
- Primary keys and unique constraints are created as NOT ENFORCED (informational metadata in Unity Catalog)
- Declared foreign keys are preserved as metadata with async validation
- Inferred foreign keys are included but marked for manual confirmation

A shell script (`deploy.sh`) can execute the DDL against a Databricks SQL warehouse via the REST API, or run in dry-run mode to print everything for review.

A comprehensive manual tasks checklist is generated listing everything that cannot be auto-deployed: stored procedures, complex UDFs, PL/pgSQL functions, external schemas, COPY/UNLOAD commands, and library installations.

**Produces:** Ordered DDL files, deployment manifest (JSON), manual tasks checklist (Markdown), and an async FK validation job.

---

## Phase 3 — Verification & Dashboard

### 8. Validation Generator (`validation_generator.py` + `run_reconciliation.py`)

Generates data parity checks for every migrated table in three formats:
- **Great Expectations** — JSON expectation suites ready to run in Databricks or local Spark
- **PyDeequ** — Python scripts using the Deequ library for metric computation and verification
- **Reconciliation SQL** — paired source and target queries (adapts to the configured platforms)

For each table, checks include: row count comparison, hash-based checksum, per-column null and distinct counts, numeric min/max/sum, date range boundaries, and primary key uniqueness on the target.

The reconciliation runner connects to both Redshift and Databricks simultaneously, executes every paired query, compares the results field-by-field, and records pass/fail for each check.

**Produces:** Great Expectations suites (JSON per table), PyDeequ scripts (Python per table), paired reconciliation queries (JSON), and validation results (JSON) with per-table per-check pass/fail and an overall summary.

---

### 9. Confidence Calculator (`confidence_calculator.py`)

Computes a single confidence score (0 to 1) for every migrated object by combining three signals:
- **Validation pass rate** — what fraction of data parity checks passed
- **Conversion difficulty** — how hard was the SQL translation (inverted, so easier = higher confidence)
- **Coverage** — what fraction of columns have known type mappings and no schema mismatches

Objects below a configurable threshold are flagged for manual review. For each low-confidence object, specific guidance is generated: investigate failed checks, review the transpiled SQL, check column type mappings, or resolve schema mismatches.

The report includes a full audit trail — every raw input value, the weights used, and the formula itself — so auditors can reproduce and adjust scores without re-running the tool.

**Produces:** Confidence summary (CSV), detailed confidence report (JSON) with audit trail, and appended entries to the manual tasks checklist.

---

### 10. Migration Dashboard (`app.py`)

A Streamlit web application that ties all artifacts together into a single interactive cockpit. It adapts to whatever artifacts are present — you can launch it at any point during the migration and it will show what's available.

**Six tabs:**

| Tab | What It Shows |
|---|---|
| **Overview** | KPI cards (total objects, auto-converted %, manual rewrite count, overall confidence, validation pass rate), query volume timeline, classification breakdown pie chart, confidence score distribution |
| **Objects** | Searchable, pageable table of every object with schema, type, classification, difficulty, and confidence. Click any row to see source DDL, transpiled SQL, diff, and validation results |
| **Conversion Diff** | Side-by-side view of source vs. transpiled SQL with syntax-highlighted unified diff, applied rules, and warnings |
| **Validation** | Per-table drill-down into every check (row count, hash, column stats, PK uniqueness) with pass/fail and source/target values. Re-run validation for a single table on demand |
| **Manual Tasks** | Rendered checklist from `manual_tasks.md` with progress tracking. Export a ZIP review pack for offline review by data owners |
| **Runbook** | Execution status showing which pipeline steps have been completed (based on artifact timestamps) and guidance on remaining manual work |

**Sidebar controls:** Source platform selector (Redshift / Snowflake), target platform selector (Databricks), schema filter, confidence threshold slider, date range filter, and a button to re-run full validation. Switching source or target rewrites `config.yaml` and re-runs the pipeline automatically. The dashboard enforces source ≠ target — selecting the same platform for both shows an error and blocks the app.

---

## How It All Fits Together

```
Source (Redshift / Snowflake)              Target (Databricks / ...)
      │                                          ▲
      ▼                                          │
 ┌─────────────────────────────────────┐         │
 │  1. Metadata (platform-specific)    │──→ source_catalog.json
 │  2. Workload                        │──→ workload_summary.json, owner_mapping.json
 │  3. Relationships                   │──→ fk_candidates.csv
 └─────────────────────────────────────┘         │
                    │                             │
                    ▼                             │
 ┌─────────────────────────────────────┐         │
 │  4. Transpiler (platform-specific)  │──→ transpiled/*.sql, convert_report.json
 │  5. Export to S3                    │──→ Parquet on S3, export_manifest.json
 │  6. Ingest to Delta                 │──→ Delta tables, ingestion_job_report.json
 │  7. DDL Deployer                    │──→ deploy/*.sql, manual_tasks.md
 └─────────────────────────────────────┘         │
                    │                             │
                    ▼                             │
 ┌─────────────────────────────────────┐         │
 │  8. Validation                      │──→ GE suites, Deequ scripts, validation_results.json
 │  9. Confidence                      │──→ confidence_report.json, confidence_summary.csv
 │ 10. Dashboard (source+target aware) │──→ Live web UI at localhost:8501
 └─────────────────────────────────────┘
```

Each script reads artifacts produced by earlier scripts and writes its own. The dashboard reads all of them. The pipeline is designed to be re-runnable — re-run any step after changes and downstream tools pick up the updated artifacts.

---

## What Requires Manual Work

The toolkit automates as much as possible but certain items always need human involvement:

### Common to both platforms
- **Inferred FK constraints** — statistical suggestions that need business owner confirmation
- **Low-confidence objects** — investigate and resolve the specific issues flagged by the confidence calculator
- **Complex Python UDFs** — rewrite as Databricks Python UDFs
- **Scheduled jobs** — recreate as Databricks Workflows

### Redshift-specific
- **PL/pgSQL stored procedures** — no Spark SQL equivalent; rewrite as Databricks notebooks or workflows
- **Procedural functions** — IF/THEN, LOOP, CURSOR, RAISE, dynamic SQL require manual conversion
- **External schemas / Spectrum tables** — map to Unity Catalog external locations
- **Library installations** — replace CREATE LIBRARY with cluster libraries or `%pip install`

### Snowflake-specific
- **JavaScript stored procedures** — `LANGUAGE JAVASCRIPT` procs need rewrite as Databricks notebooks
- **Snowflake Streams and Tasks** — `CREATE STREAM` / `CREATE TASK` have no Databricks equivalent; redesign as Delta Live Tables or Workflows
- **Stages and File Formats** — `CREATE STAGE` / `CREATE FILE FORMAT` / `COPY INTO` need redesign for cloud storage + Auto Loader
- **QUALIFY clause** — rewrite using subqueries or window function filters
- **MATCH_RECOGNIZE** — pattern matching needs manual redesign
- **Time Travel queries** — `AT(TIMESTAMP => ...)` / `BEFORE(STATEMENT => ...)` need redesign for Delta time travel syntax

All manual items are tracked in `artifacts/manual_tasks.md` and surfaced in the dashboard's Manual Tasks tab.
