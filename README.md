# MigData — Any Source to Any Target

Mock migration simulator that demonstrates the full source-to-target migration lifecycle
without requiring real cloud connections. Supports **Redshift** and **Snowflake**
as source platforms and **Databricks** as a target platform (more targets coming).
Both source and target are selectable from the dashboard sidebar or `config.yaml`.
Every adapter is pluggable — swap mock adapters for real connectors when ready.

## Architecture

```
                        config.yaml
                            |
                    +-------+-------+
                    |  run_demo.py  |   <-- python -m src.run_demo
                    +---+---+---+---+
                        |   |   |   |
           +------------+   |   |   +-------------+
           |                |   |                  |
    SourceAdapter   ConversionEngine  DataLoader  ValidationEngine
    (interface)      (interface)     (interface)   (interface)
           |                |            |              |
   Redshift path:    Redshift path:
  MockSourceAdapter  MockConversion
  (mock_redshift.py) (mock_converter)
           |                |
   Snowflake path:   Snowflake path:
  MockSourceAdapter  MockConversion
  (mock_snowflake.py)(mock_snowflake_converter)
                                         |              |
                                   MockDataLoader  MockValidation
                                   (mock_loader)   (mock_validator)
           |                |            |              |
           v                v            v              v
    mock_data/       artifacts/     artifacts/     test_results/
    source_catalog   transpiled_sql target_tables  validation_results
    query_logs       conversion_rpt load_summary   confidence_scores
                                                        |
                                                   test_runner.py
                                                        |
                                                   test_results/
                                                   test_report.xml
                                                   test_summary.html
                                                        |
                                                     app.py
                                                   (Streamlit UI)
```

## Quick Start

```bash
# Install dependencies
pip install faker pyarrow pandas streamlit plotly pyyaml networkx

# Run everything with one command (generates data + launches dashboard)
python -m src.run_demo

# Or run without launching the UI
python -m src.run_demo --no-ui
```

By default the pipeline runs **Redshift → Databricks**. You can change both source
and target in two ways:

**Option A — Dashboard sidebar** (no restart needed):
The sidebar has **Source platform** and **Target platform** radio buttons. Selecting a
different value writes to `config.yaml` and re-runs the pipeline automatically.

**Option B — Edit `config.yaml`:**

```yaml
source:
  adapter: "mock_snowflake"   # was "mock_redshift"

target:
  platform: "databricks"      # "databricks" (more targets coming)
```

Then re-run `python -m src.run_demo`.

## Switching Between Platforms

### Source platforms

| Config value | Source platform | Source adapter | Conversion engine |
|---|---|---|---|
| `"mock"` or `"mock_redshift"` | Redshift | `mock_redshift.py` | `mock_converter.py` |
| `"mock_snowflake"` | Snowflake | `mock_snowflake.py` | `mock_snowflake_converter.py` |

### Target platforms

| Config value | Target platform |
|---|---|
| `"databricks"` | Databricks |

The pipeline, test runner, and dashboard all adapt automatically based on the configured
source adapter and target platform. All UI labels update dynamically — e.g. the sidebar
caption shows "Redshift → Databricks Demo" or "Snowflake → Databricks Demo".

The dashboard enforces **source ≠ target** — selecting the same platform for both shows
an error and blocks the app.

## Project Structure

```
data-migration/
+-- config.yaml                        # All paths, adapters, weights -- single source of truth
+-- app.py                             # Streamlit dashboard (10 tabs, platform-aware)
+-- capture_demo.py                    # Playwright screenshot automation (optional)
+-- src/
|   +-- __init__.py
|   +-- __main__.py                    # Entry: python -m src
|   +-- interfaces.py                  # ABCs: SourceAdapter, ConversionEngine, DataLoader, ValidationEngine
|   +-- config.py                      # YAML config loader + get_source_platform() + get_target_platform()
|   +-- logger.py                      # Structured JSON logging
|   +-- run_demo.py                    # Pipeline orchestrator (factory + 5 steps + UI launch)
|   +-- mock_redshift.py               # MockSourceAdapter -- Redshift catalog + query logs
|   +-- mock_converter.py              # MockConversionEngine -- Redshift rewrite rules
|   +-- mock_snowflake.py              # MockSourceAdapter -- Snowflake catalog + query logs
|   +-- mock_snowflake_converter.py    # MockConversionEngine -- Snowflake rewrite rules
|   +-- mock_loader.py                 # MockDataLoader -- local Parquet via pandas/pyarrow
|   +-- mock_validator.py              # MockValidationEngine -- 4 checks per table + confidence
|   +-- test_runner.py                 # 5 test suites, platform-conditional, JUnit XML + HTML
+-- mock_data/                         # Generated source metadata
|   +-- source_catalog.json            # 25 tables, 213 columns, 32 constraints, 3 procs, 2 UDFs
|   +-- query_logs.json                # ~682 query log entries spanning 90 days
+-- artifacts/                         # Generated migration artifacts
|   +-- transpiled_sql/                # 28 .sql files (25 tables + 3 views)
|   +-- conversion_report.json         # Per-object classification, difficulty, diffs
|   +-- target_tables/                 # 25 Parquet files with synthetic data
|   +-- load_summary.json              # Per-table load metrics
|   +-- pipeline_summary.json          # Full pipeline run summary
|   +-- logs/                          # Structured JSON logs per module
+-- test_results/                      # Test + validation outputs
|   +-- validation_results.json        # Per-table per-check pass/fail + confidence
|   +-- confidence_scores.csv          # One row per table
|   +-- test_report.xml                # JUnit XML
|   +-- test_summary.html              # Styled HTML report
+-- screenshots/                       # Demo screenshots (from capture_demo.py)
```

## How Mock Replaces Real Systems

| Real System | Mock Replacement | Interface |
|---|---|---|
| Redshift cluster | `MockSourceAdapter` (mock_redshift.py) -- Faker generates Redshift-style catalog with DISTKEY, SORTKEY, ENCODE, PL/pgSQL procs | `SourceAdapter` |
| Snowflake account | `MockSourceAdapter` (mock_snowflake.py) -- Faker generates Snowflake-style catalog with CLUSTER BY, VARIANT, TIMESTAMP_NTZ, JavaScript procs | `SourceAdapter` |
| Redshift SQL transpiler | `MockConversionEngine` (mock_converter.py) -- 30 Redshift-to-Spark regex rules | `ConversionEngine` |
| Snowflake SQL transpiler | `MockConversionEngine` (mock_snowflake_converter.py) -- 30 Snowflake-to-Spark regex rules | `ConversionEngine` |
| S3 UNLOAD + Databricks COPY INTO | `MockDataLoader` -- generates typed DataFrames with Faker, writes local Parquet | `DataLoader` |
| Source + Databricks dual queries | `MockValidationEngine` -- compares catalog metadata against Parquet files | `ValidationEngine` |

## Where to Plug Real Connectors

Each adapter is resolved by a factory function in `run_demo.py`. To swap in real systems:

### 1. Source: Real Redshift

```python
# src/redshift_adapter.py
from src.interfaces import SourceAdapter

class RedshiftSourceAdapter(SourceAdapter):
    def __init__(self, config):
        self.conn = psycopg2.connect(...)

    def extract_catalog(self) -> dict: ...
    def extract_query_logs(self, catalog) -> list: ...
```

### 2. Source: Real Snowflake

```python
# src/snowflake_adapter.py
from src.interfaces import SourceAdapter

class SnowflakeSourceAdapter(SourceAdapter):
    def __init__(self, config):
        self.conn = snowflake.connector.connect(
            account=config["source"]["snowflake"]["account"],
            warehouse=config["source"]["snowflake"]["warehouse"],
            ...
        )

    def extract_catalog(self) -> dict:
        # Query INFORMATION_SCHEMA, ACCOUNT_USAGE views
        ...

    def extract_query_logs(self, catalog) -> list:
        # Query SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        ...
```

Then in `config.yaml`:
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

## Configuration (config.yaml)

All runtime settings in one file -- no hardcoded paths:

```yaml
project:
  name: "MigData"
  seed: 42

source:
  adapter: "mock_redshift"   # "mock_redshift" | "mock_snowflake" | "mock" (legacy)

target:
  platform: "databricks"    # "databricks" (more targets coming)

conversion:
  engine: "mock"             # "mock" | "llm"

loader:
  engine: "mock"             # "mock" | "databricks"
  max_rows: 2000

validation:
  engine: "mock"             # "mock" | "live"
  weights:
    row_count: 0.30
    checksum: 0.25
    null_variance: 0.20
    schema_drift: 0.25
  confidence_threshold: 0.6

logging:
  level: "INFO"
  json_logs: true
```

Environment variables are interpolated via `${VAR}` syntax.

## Running Individual Steps

Each module works standalone:

```bash
# Redshift path
python -m src.mock_redshift          # Step 1: generate Redshift catalog
python -m src.mock_converter         # Step 2: transpile Redshift SQL

# Snowflake path
python -m src.mock_snowflake         # Step 1: generate Snowflake catalog
python -m src.mock_snowflake_converter  # Step 2: transpile Snowflake SQL

# Shared steps
python -m src.mock_loader            # Step 3: generate Parquet files
python -m src.mock_validator         # Step 4: run validation checks
python -m src.test_runner            # Step 5: execute test suite
streamlit run app.py                 # Step 6: launch dashboard
```

## Adding a New Target Platform

Adding a new target (e.g. Synapse) requires three steps:

1. **`app.py`** — append `"Synapse"` to the `_target_options` list in the sidebar
2. **Type mapping + converter** — add a new type mapping dict and conversion module
3. **`config.yaml`** — set `target.platform: "synapse"`

The `TARGET_LABEL` constant uses `.title()` so any platform name becomes a proper
UI label automatically. Source ≠ target validation works for any combination.

## Dependencies

```
faker>=40.0
pyarrow>=15.0
pandas>=2.0
streamlit>=1.30
plotly>=5.18
pyyaml>=6.0
networkx>=3.0
```

Optional for screenshots:
```
playwright>=1.40
```
