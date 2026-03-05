# User Guide — MigData (Any Source to Any Target)

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Installation](#2-installation)
3. [Quick Start](#3-quick-start)
4. [Logging In & Connecting](#4-logging-in--connecting)
5. [Choosing Source and Target Platform](#5-choosing-source-and-target-platform)
6. [Running the Full Pipeline](#6-running-the-full-pipeline)
7. [Running Individual Steps](#7-running-individual-steps)
8. [Launching the Dashboard](#8-launching-the-dashboard)
9. [Dashboard Tabs Reference](#9-dashboard-tabs-reference)
10. [Standalone Tools](#10-standalone-tools)
11. [Configuration](#11-configuration)
12. [Output Artifacts](#12-output-artifacts)
13. [Connecting Real Systems](#13-connecting-real-systems)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Prerequisites

- **Python 3.10+** (required for type hint syntax used in the codebase)
- **pip** (Python package manager)
- **Git** (optional, for version control)

Verify your Python version:

```bash
python --version
```

---

## 2. Installation

### Step 1: Navigate to the project directory

```bash
cd C:\dev\data-migration
```

### Step 2: (Recommended) Create a virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

If `requirements.txt` is missing packages, install the full set manually:

```bash
pip install faker pyarrow pandas streamlit plotly pyyaml networkx psycopg2-binary tqdm
```

### Step 4: Verify installation

```bash
python -c "import streamlit, plotly, pandas, faker, pyarrow, networkx; print('All dependencies OK')"
```

---

## 3. Quick Start

Run the entire pipeline and launch the dashboard with a single command:

```bash
python -m src.run_demo
```

This will:
1. Generate a synthetic source catalog (5 schemas, 25 tables, ~213 columns)
2. Transpile all SQL from source dialect to Spark SQL (33 objects)
3. Generate Parquet files with realistic synthetic data (~47K rows)
4. Run 100 validation checks across 25 tables
5. Execute automated tests (schema, SQL translation, data parity, edge cases, manual flags)
6. Launch the Streamlit dashboard at **http://localhost:8501**

To run without launching the UI:

```bash
python -m src.run_demo --no-ui
```

---

## 4. Logging In & Connecting

MigData includes a login page and a connection page that appear before the dashboard.

### Step 1: Launch

```bash
streamlit run app.py
```

### Step 2: Login

- Default credentials: **admin** / **admin@123**
- Enter your username and password, then click **Log In**

### Step 3: Self-registration

- Click **"Don't have an account? Register"** on the login page
- Fill in username, password (min 6 characters), and confirm password
- Click **Register**, then log in with your new credentials
- User accounts are stored in `config/users.json` and persist across sessions

### Step 4: Connection page

After logging in you are taken to the connection page:

1. **Mock mode** — Check "Use Mock Data" to skip real connections. Click "Test Source Connection" to simulate a 2-second connection, then "Proceed to Dashboard"
2. **Real connections** — Select your source platform (Redshift / Snowflake), fill in connection fields, and click "Test Source Connection". A green success message confirms the connection. Optionally test the target (Databricks) or skip it
3. Click **"Proceed to Dashboard →"** to enter the main dashboard. The pipeline runs automatically on first entry

### Logout

In the dashboard sidebar, scroll to the bottom and click **Logout** to return to the login page.

---

## 5. Choosing Source and Target Platform

The simulator supports multiple source and target platforms. You can switch platforms in two ways:

### Option A — Dashboard sidebar (recommended)

The sidebar has **Source platform** and **Target platform** radio buttons. Selecting a different value automatically updates `config.yaml` and re-runs the pipeline — no restart needed.

### Option B — Edit `config.yaml`

#### Source platforms

**Redshift (default):**

```yaml
source:
  adapter: "mock_redshift"    # or "mock" (legacy alias)
```

Uses `mock_redshift.py` (Redshift-style catalog with DISTKEY, SORTKEY, ENCODE, PL/pgSQL stored procs) and `mock_converter.py` (30 Redshift-to-Spark rewrite rules).

**Snowflake:**

```yaml
source:
  adapter: "mock_snowflake"
```

Uses `mock_snowflake.py` (Snowflake-style catalog with CLUSTER BY, VARIANT, TIMESTAMP_NTZ, JavaScript stored procs) and `mock_snowflake_converter.py` (30 Snowflake-to-Spark rewrite rules).

#### Target platforms

**Databricks (default):**

```yaml
target:
  platform: "databricks"     # "databricks" (more targets coming)
```

After changing the source or target, re-run the pipeline to regenerate all artifacts:

```bash
python -m src.run_demo --no-ui
```

The dashboard, test runner, and pipeline all adapt automatically to the configured platforms. All UI labels update dynamically (e.g. "Redshift → Databricks Demo" or "Snowflake → Databricks Demo").

### Source ≠ target validation

The dashboard enforces that source and target cannot be the same platform. If a future platform appears in both lists, selecting the same for source and target shows an error and blocks the app.

---

## 6. Running the Full Pipeline

### From the command line

```bash
cd C:\dev\data-migration
python -m src.run_demo
```

Expected output (Redshift example):

```
============================================================
  Redshift -> Databricks MigData
============================================================
  Config   : config.yaml
  Seed     : 42
  Platform : Redshift
  Adapters : source=mock_redshift, conversion=mock, loader=mock, validation=mock
============================================================

STEP 1 -- Generate Source Metadata
  Tables: 25, Columns: 213, Queries: 682

STEP 2 -- Convert SQL (Redshift -> Databricks)
  AUTO: 30, WARNINGS: 0, MANUAL: 3

STEP 3 -- Load Data (Parquet)
  Tables: 25, Rows: 46,833, Mismatches: 3

STEP 4 -- Run Validation Checks
  Checks: 100, Passed: 93, Failed: 7, Rate: 93.0%

STEP 5 -- Execute Test Suite
  Total: 252, Passed: 252, Failed: 0, Pass Rate: 100.0%

============================================================
  Pipeline complete in 23.07s
============================================================
```

### From the dashboard

Click the **Run Full Demo Pipeline** button in the sidebar. The dashboard runs all 5 steps inline and reloads automatically.

---

## 7. Running Individual Steps

Each pipeline step can run independently. Run them in order if starting from scratch.

**Redshift path:**

| Step | Command | What It Does |
|------|---------|-------------|
| 1 | `python -m src.mock_redshift` | Generate synthetic Redshift catalog and query logs |
| 2 | `python -m src.mock_converter` | Transpile Redshift SQL to Spark SQL |

**Snowflake path:**

| Step | Command | What It Does |
|------|---------|-------------|
| 1 | `python -m src.mock_snowflake` | Generate synthetic Snowflake catalog and query logs |
| 2 | `python -m src.mock_snowflake_converter` | Transpile Snowflake SQL to Spark SQL |

**Shared steps (after Step 1 and 2):**

| Step | Command | What It Does |
|------|---------|-------------|
| 3 | `python -m src.mock_loader` | Generate Parquet files with synthetic data |
| 4 | `python -m src.mock_validator` | Run validation checks (row count, checksum, etc.) |
| 5 | `python -m src.test_runner` | Execute the full test suite |

**Step 1** must run first. Steps 2-5 depend on the artifacts from Step 1. Steps 3-5 depend on Step 2's output.

### Optional: Run with a custom seed

```bash
python -m src.mock_redshift --seed 123
python -m src.mock_snowflake --seed 123
```

---

## 8. Launching the Dashboard

After running the pipeline (or at least Step 1), launch the Streamlit dashboard:

```bash
streamlit run app.py
```

The dashboard opens at **http://localhost:8501** in your default browser.

### Sidebar Controls

- **Source platform** — Radio button to select the source warehouse (Redshift / Snowflake). Switching rewrites `config.yaml` and re-runs the pipeline
- **Target platform** — Radio button to select the target warehouse (Databricks). Switching rewrites `config.yaml` and re-runs the pipeline. Source and target cannot be the same
- **Schema filter** — Filter all tabs by one or more source schemas (public, analytics, staging, finance, marketing)
- **Confidence threshold** — Adjust the threshold slider (default 0.6) to flag objects for manual review
- **Run Full Demo Pipeline** — One-click button to regenerate all data
- **Pipeline Status** — Checklist showing which artifacts have been generated

---

## 9. Dashboard Tabs Reference

The dashboard has **9 tabs** organized by workflow:

### Tab 1: Overview

Top-level migration summary with:
- KPI cards: total tables, auto-converted %, manual rewrite count, avg confidence, validation pass rate
- Query volume timeline (90-day bar chart with 7-day rolling average)
- Object classification pie chart (AUTO / WARNINGS / MANUAL)
- Confidence score distribution histogram
- Table size distribution (top 15 by MB)

### Tab 2: Objects

Searchable, filterable table of all migration objects:
- Filter by name, classification, or object type
- Columns: object name, schema, type, classification, difficulty score, rules applied, warnings, manual flags
- CSV export button

### Tab 3: Schema Explorer

Detailed schema and column browser:
- **Schema summary** — Table count, total rows, total size per schema
- **Table selector** — Pick any table to inspect
- **Table properties** — Row count, size, and platform-specific properties (Redshift: dist style, encoding, % used; Snowflake: cluster by, auto clustering, retention time)
- **Column details** — Every column with its source type and auto-mapped target equivalent
- **Constraints** — Primary keys, foreign keys, unique constraints per table
- **Target DDL preview** — Auto-generated `CREATE TABLE ... USING DELTA` statement
- CSV export for column mappings

### Tab 4: Relationships

Declared and inferred foreign key relationships:
- **Summary metrics** — Declared FKs, inferred candidates, highly likely, likely counts
- **Three views** (toggle with radio buttons):
  - **Graph** — Interactive network graph. Solid lines = declared FKs, dashed orange = inferred FKs. Node color = schema, node size = connection count
  - **Declared FKs** — Table of all FK constraints from the catalog
  - **Inferred Candidates** — Table of FK candidates from the relationship profiler, filterable by classification (highly_likely, likely, possible, unlikely). Shows overlap ratio and parent uniqueness

> To populate inferred candidates, run `relationship_profiler.py` (requires Redshift connection) or provide an `artifacts/fk_candidates.json` file.

### Tab 5: Metadata

Browse stored procedures, UDFs, views, and materialized views:
- **Summary metrics** — Counts per object type
- **Four sub-views** (toggle with radio buttons):
  - **Stored Procedures** — Expandable cards showing source code, language, args, return type, and referenced tables. Flagged as requiring manual rewrite
  - **UDFs** — Source code with automation assessment (SQL UDFs = auto-convertible to target SQL UDF, Python UDFs = manual)
  - **Views** — View definitions with dependency analysis (which tables each view reads from) and conversion status
  - **Materialized Views** — Definitions if available
- **Automation Assessment** — Summary table showing which metadata objects can be auto-migrated vs. need manual work, with a progress bar

### Tab 6: Lineage

End-to-end data lineage built from FK relationships, view definitions, proc source code, and query logs:
- **Three views** (toggle with radio buttons):
  - **Full Lineage Graph** — Network graph with different node shapes (circle=table, diamond=view, square=proc) and edge styles (solid=FK, dashed=view dependency, dotted=proc dependency)
  - **Query Access Heatmap** — Bar chart of most-accessed tables from query logs, plus a co-occurrence matrix showing which tables are frequently queried together
  - **Table Dependencies** — Searchable dependency matrix listing every object and what it depends on (views read from tables, procs read/write tables, FKs reference tables). CSV export

### Tab 7: SQL Comparison

Side-by-side source vs. target SQL (labels adapt to the configured platforms):
- Object selector dropdown
- Info bar: classification, difficulty, rules applied, warnings
- Source and target SQL panels with syntax highlighting
- Unified diff viewer
- Applied rules list, warnings, and manual rewrite flags

### Tab 8: Validation

Per-table validation scorecards:
- Summary: tables validated, total checks, passed, failed, pass rate
- Table selector with per-table drill-down:
  - Confidence score, checks passed count, difficulty
  - Expandable check details (row_count_match, checksum, null_variance, schema_drift)
- Confidence heatmap — Horizontal bar chart of all tables colored by confidence score
- JSON export

### Tab 9: Manual Work

Categorized list of items requiring human attention:
- Stored procedures (need rewrite as target-platform notebooks/workflows)
- Manual rewrite objects (flagged by transpiler)
- Flagged items from conversion
- Low-confidence tables (below threshold)
- **Export Review Pack (ZIP)** — Bundles all artifacts for offline review
- Test report (embedded HTML)

---

## 10. Standalone Tools

These scripts complement the pipeline and can be run independently. Scripts marked **(Redshift only)** currently target Redshift and would need adaptation for Snowflake sources.

| Script | Purpose | Platform | Command |
|--------|---------|----------|---------|
| `metadata_extractor.py` | Extract metadata from a real Redshift cluster | Redshift only | `python metadata_extractor.py` |
| `relationship_profiler.py` | Profile columns to infer FK relationships | Redshift only | `python relationship_profiler.py` |
| `workload_analyzer.py` | Analyze query workload patterns | Redshift only | `python workload_analyzer.py` |
| `sql_transpiler_service.py` | Advanced SQL transpilation service | Redshift only | `python sql_transpiler_service.py` |
| `transpile.py` | CLI wrapper for SQL transpilation | Redshift only | `python transpile.py` |
| `validation_generator.py` | Generate validation rules (Great Expectations, PyDeequ) | Generic | `python validation_generator.py` |
| `confidence_calculator.py` | Compute per-object confidence scores | Generic | `python confidence_calculator.py` |
| `export_to_s3.py` | Export Parquet to S3 (UNLOAD simulation) | Redshift only | `python export_to_s3.py` |
| `databricks_ingest.py` | Ingest data into Databricks Delta tables | Generic | `python databricks_ingest.py` |
| `ddl_deployer.py` | Generate deployment-ready DDL for Databricks | Generic | `python ddl_deployer.py` |
| `deploy.sh` | Execute DDL against a Databricks SQL warehouse | Generic | `bash deploy.sh --dry-run` |
| `capture_demo.py` | Capture dashboard screenshots (requires Playwright) | Generic | `python capture_demo.py` |

### Relationship Profiler

Generates `artifacts/fk_candidates.json` which the dashboard Relationships tab reads:

```bash
# Requires Redshift connection (env vars: REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD)
python relationship_profiler.py

# Or use --live to fetch metadata directly instead of reading source_catalog.json
python relationship_profiler.py --live
```

### Workload Analyzer

Generates `artifacts/workload_summary.json`:

```bash
python workload_analyzer.py --lookback-days 90 --top-n 50
```

### Confidence Calculator

Generates `artifacts/confidence_summary.csv` and `artifacts/confidence_report.json`:

```bash
python confidence_calculator.py --threshold 0.6
```

---

## 11. Configuration

All settings live in `config.yaml` at the project root.

### Key sections

```yaml
project:
  name: "MigData"
  seed: 42                          # Random seed for reproducible mock data

paths:
  mock_data: "mock_data"            # Source metadata output
  artifacts: "artifacts"            # Migration artifacts
  test_results: "test_results"      # Validation & test outputs
  transpiled: "artifacts/transpiled_sql"
  target_tables: "artifacts/target_tables"
  logs: "artifacts/logs"

source:
  adapter: "mock_redshift"          # "mock_redshift" | "mock_snowflake" | "mock" (legacy) | "redshift" | "snowflake"

target:
  platform: "databricks"           # "databricks" (more targets coming)

conversion:
  engine: "mock"                    # "mock" or "llm"

loader:
  engine: "mock"                    # "mock" or "databricks"
  max_rows: 2000

validation:
  engine: "mock"                    # "mock" or "live"
  weights:                          # Confidence score weights
    row_count: 0.30
    checksum: 0.25
    null_variance: 0.20
    schema_drift: 0.25
  confidence_threshold: 0.6

logging:
  level: "INFO"
  json_logs: true
```

### Environment variable interpolation

Use `${VAR}` syntax for secrets:

```yaml
source:
  adapter: "redshift"
  redshift:
    host: "${REDSHIFT_HOST}"
    password: "${REDSHIFT_PASSWORD}"
```

Or for Snowflake:

```yaml
source:
  adapter: "snowflake"
  snowflake:
    account: "${SNOWFLAKE_ACCOUNT}"
    warehouse: "${SNOWFLAKE_WAREHOUSE}"
    database: "${SNOWFLAKE_DB}"
    user: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"
    role: "${SNOWFLAKE_ROLE}"
```

---

## 12. Output Artifacts

After a full pipeline run, the following directories and files are produced:

```
C:\dev\data-migration\
|
+-- mock_data\
|   +-- source_catalog.json        # Full catalog: tables, columns, constraints, procs, UDFs, views
|   +-- query_logs.json            # 682 synthetic query log entries (90 days)
|
+-- artifacts\
|   +-- transpiled_sql\            # 28 SQL files (25 tables + 3 views)
|   |   +-- public_customers.sql
|   |   +-- public_orders.sql
|   |   +-- ...
|   +-- target_tables\             # 25 Parquet files with synthetic data
|   |   +-- public_customers.parquet
|   |   +-- ...
|   +-- conversion_report.json     # Per-object classification, difficulty, diffs, rules
|   +-- load_summary.json          # Per-table load metrics (rows, mismatches, runtime)
|   +-- pipeline_summary.json      # Full pipeline run summary
|   +-- fk_candidates.json         # Inferred FK relationships (from relationship_profiler)
|   +-- fk_candidates.csv          # Same in CSV format
|   +-- workload_summary.json      # Query workload analysis (from workload_analyzer)
|   +-- confidence_summary.csv     # Per-object confidence scores (from confidence_calculator)
|   +-- confidence_report.json     # Detailed confidence breakdown
|   +-- logs\                      # Structured JSON logs
|       +-- run_demo.log
|       +-- mock_redshift.log
|       +-- mock_converter.log
|       +-- mock_loader.log
|       +-- mock_validator.log
|
+-- test_results\
    +-- validation_results.json    # Per-table validation checks with pass/fail
    +-- confidence_scores.csv      # Confidence score per table
    +-- test_report.xml            # JUnit XML format (252 tests)
    +-- test_summary.html          # Styled HTML test report
```

---

## 13. Connecting Real Systems

MigData supports connecting to real source and target systems. You can provide credentials either via the **connection page** in the UI or via **environment variables**.

### Real Redshift — Step by Step

**Prerequisites:** `psycopg2-binary` (already in `requirements.txt`)

**Gather:** Cluster endpoint, port (default 5439), database name, IAM user, password.

**Option A — Connection page (recommended):**

1. On the connection page, select **Redshift** as source platform
2. Fill in Host, Port, Database, User, Password
3. Click **Test Source Connection** — wait for green success
4. Optionally check **"Save connection details for next time"**
5. Click **"Proceed to Dashboard →"**

**Option B — Environment variables:**

```bash
set REDSHIFT_HOST=my-cluster.abc123.us-east-1.redshift.amazonaws.com
set REDSHIFT_PORT=5439
set REDSHIFT_DB=analytics_dw
set REDSHIFT_USER=migration_user
set REDSHIFT_PASSWORD=your_password
```

Then update `config.yaml`:

```yaml
source:
  adapter: "redshift"
  redshift:
    host: "${REDSHIFT_HOST}"
    port: 5439
    database: "${REDSHIFT_DB}"
    user: "${REDSHIFT_USER}"
    password: "${REDSHIFT_PASSWORD}"
```

**Troubleshooting:**

| Error symptom | Likely cause | Fix |
|---|---|---|
| Timeout / could not connect | Wrong host or port, or security group blocks access | Check Host and Port, ensure your IP is whitelisted |
| Password authentication failed | Wrong user or password | Check User and Password |
| Database does not exist | Wrong database name | Check Database name |

### Real Snowflake — Step by Step

**Prerequisites:** `pip install snowflake-connector-python>=3.6`

**Gather:** Account identifier, warehouse, database, role, user, password.

**Account identifier format:**
- New URL format: `org-account` (e.g. `myorg-myaccount`)
- Legacy format: `account.region.cloud` (e.g. `xy12345.us-east-1.aws`)

**Option A — Connection page (recommended):**

1. On the connection page, select **Snowflake** as source platform
2. Fill in Account, Warehouse, Database, Role, User, Password
3. Click **Test Source Connection** — wait for green success
4. Optionally check **"Save connection details for next time"**
5. Click **"Proceed to Dashboard →"**

**Option B — Environment variables:**

```bash
set SNOWFLAKE_ACCOUNT=myorg-myaccount
set SNOWFLAKE_WAREHOUSE=COMPUTE_WH
set SNOWFLAKE_DB=ANALYTICS_DW
set SNOWFLAKE_USER=migration_user
set SNOWFLAKE_PASSWORD=your_password
set SNOWFLAKE_ROLE=SYSADMIN
```

Then update `config.yaml`:

```yaml
source:
  adapter: "snowflake"
  snowflake:
    account: "${SNOWFLAKE_ACCOUNT}"
    warehouse: "${SNOWFLAKE_WAREHOUSE}"
    database: "${SNOWFLAKE_DB}"
    user: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"
    role: "${SNOWFLAKE_ROLE}"
```

**Troubleshooting:**

| Error symptom | Likely cause | Fix |
|---|---|---|
| Account not found | Wrong account identifier | Check Account identifier (format: org-account) |
| Incorrect username or password | Wrong credentials | Check User and Password |
| Warehouse does not exist / suspended | Wrong warehouse or it needs resuming | Check Warehouse name, ensure it is running |

**Note on query logs:** Query log extraction requires `ACCOUNTADMIN` or `MONITOR` privilege to access `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`. Without it, catalog extraction still works but query logs will be empty.

The real adapter (`src/snowflake_adapter.py`) implements `SourceAdapter` — queries `INFORMATION_SCHEMA` for metadata. No manual setup beyond installing the connector and providing credentials.

### Connect to Databricks (Target)

**Option A — Connection page:**

1. On the connection page, uncheck **"Skip target connection"**
2. Fill in Host, Access Token, HTTP Path
3. Click **Test Target Connection**

**Option B — Environment variables:**

```bash
set DATABRICKS_HOST=your-workspace.cloud.databricks.com
set DATABRICKS_TOKEN=your_token
set DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
```

Then update `config.yaml`:

```yaml
loader:
  engine: "databricks"
  databricks:
    host: "${DATABRICKS_HOST}"
    token: "${DATABRICKS_TOKEN}"
    http_path: "${DATABRICKS_HTTP_PATH}"
    s3_bucket: "your-migration-bucket"
    iam_role: "arn:aws:iam::123456789012:role/DatabricksS3Access"
```

---

## 14. Troubleshooting

### "No data yet" on the dashboard

Run the pipeline first:

```bash
python -m src.run_demo
```

Or click **Run Full Demo Pipeline** in the sidebar.

### ModuleNotFoundError

Install missing dependencies:

```bash
pip install faker pyarrow pandas streamlit plotly pyyaml networkx
```

### "Install networkx for the lineage graph"

```bash
pip install networkx
```

### Streamlit won't start

Check if port 8501 is already in use:

```bash
streamlit run app.py --server.port 8502
```

### Relationship or Lineage tabs show "No data"

These tabs need the pipeline artifacts to exist. Ensure you have:
- `mock_data/source_catalog.json` (from Step 1)
- `artifacts/conversion_report.json` (from Step 2)
- `mock_data/query_logs.json` (from Step 1, for lineage query heatmap)

For inferred FK candidates, you need either a source database connection to run `relationship_profiler.py` (currently Redshift only), or manually place an `artifacts/fk_candidates.json` file.

### Pipeline step fails with timeout

Increase the timeout in `app.py` if running from the dashboard (default 120s), or run the step from the command line where there is no timeout.

### Confidence scores not appearing

Ensure `test_results/confidence_scores.csv` exists. This is generated by Step 4 (mock_validator). Re-run:

```bash
python -m src.mock_validator
```

### Clearing all generated data

To start fresh, delete the output directories and re-run:

```bash
rmdir /s /q mock_data artifacts test_results
python -m src.run_demo
```
