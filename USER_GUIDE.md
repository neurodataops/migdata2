# User Guide — MigData (Any Source to Any Target)

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Installation](#2-installation)
3. [Quick Start](#3-quick-start)
4. [Running with Mock Data](#4-running-with-mock-data)
5. [Logging In & Connecting](#5-logging-in--connecting)
6. [Choosing Source and Target Platform](#6-choosing-source-and-target-platform)
7. [Running the Full Pipeline](#7-running-the-full-pipeline)
8. [Dashboard Tabs Reference](#8-dashboard-tabs-reference)
9. [Configuration](#9-configuration)
10. [Connecting Real Systems](#10-connecting-real-systems)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Prerequisites

### Python Backend
- **Python 3.10+** (required for type hint syntax used in the codebase)
- **pip** (Python package manager)

### React Frontend
- **Node.js 18+** (LTS version recommended)
- **npm 9+** (comes with Node.js)

### Optional
- **Git** (for version control)

Verify your installations:

```bash
python --version
node --version
npm --version
```

---

## 2. Installation

### Step 1: Navigate to the project directory

```bash
cd C:\dev\data-migration
```

### Step 2: (Recommended) Create a Python virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate
```

### Step 3: Install Python dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Install Node.js dependencies

```bash
cd web
npm install
cd ..
```

### Step 5: Verify installation

**Python:**
```bash
python -c "import fastapi, pandas, pyarrow; print('Python dependencies OK')"
```

**Node.js:**
```bash
cd web && npm list react vite
```

---

## 3. Quick Start

The fastest way to see the application running:

### Terminal 1: Generate Mock Data
```bash
cd C:\dev\data-migration
python -m src.run_demo
```

This generates:
- 5 schemas, 25 tables, ~213 columns
- Transpiled SQL (33 objects)
- Parquet files with synthetic data (~47K rows)
- 100 validation checks
- Automated test results

### Terminal 2: Start the Backend API
```bash
cd C:\dev\data-migration
python -m uvicorn api.main:app --reload --port 8000
```

The FastAPI backend will be available at **http://localhost:8000**

### Terminal 3: Start the Frontend
```bash
cd C:\dev\data-migration\web
npm run dev
```

The React frontend will be available at **http://localhost:5173**

### Step 4: Open Your Browser

Navigate to **http://localhost:5173** and you'll see the login page.

Default credentials: **admin** / **admin@123**

---

## 4. Running with Mock Data

### Step-by-Step Process

#### 4.1 Generate Mock Data

The first step is to generate synthetic data for the demo:

```bash
python -m src.run_demo
```

**What this does:**
1. Generates a synthetic source catalog (Redshift or Snowflake-style)
2. Transpiles all SQL from source dialect to Spark SQL
3. Generates Parquet files with realistic synthetic data
4. Runs validation checks
5. Executes automated tests

**Expected output:**
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

To view results:
  1. Start the FastAPI backend: python -m uvicorn api.main:app --reload
  2. Start the React frontend: cd web && npm run dev
  3. Open http://localhost:5173 in your browser
```

#### 4.2 Start the Backend

**In a new terminal (Terminal 2):**

```bash
cd C:\dev\data-migration
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

**Options:**
- `--reload`: Auto-reload on code changes (for development)
- `--host 0.0.0.0`: Listen on all network interfaces
- `--port 8000`: Specify port (default: 8000)

**Expected output:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [12345] using WatchFiles
INFO:     Started server process [12346]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

**API Documentation:**
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

#### 4.3 Start the Frontend

**In a new terminal (Terminal 3):**

```bash
cd C:\dev\data-migration\web
npm run dev
```

**Expected output:**
```
  VITE v7.3.1  ready in 432 ms

  ➜  Local:   http://localhost:5173/
  ➜  Network: http://192.168.1.100:5173/
  ➜  press h + enter to show help
```

#### 4.4 Access the Application

Open your browser and navigate to **http://localhost:5173**

**Login Flow:**
1. You'll see the login page
2. Enter credentials: **admin** / **admin@123**
3. Click "Log In"
4. You'll be taken to the Connection page
5. Check "Use Mock Data" and click "Test Source Connection"
6. Click "Proceed to Dashboard →"

---

## 5. Logging In & Connecting

### 5.1 Login Page

The application requires authentication. Default credentials are stored in `config/users.json`.

**Default users:**
- Username: `admin`, Password: `admin@123`

### 5.2 Self-Registration

To create a new account:

1. On the login page, click **"Don't have an account? Register"**
2. Fill in:
   - Username (unique)
   - Password (minimum 6 characters)
   - Confirm Password
3. Click **Register**
4. You'll be automatically logged in

User accounts are persisted in `config/users.json`.

### 5.3 Connection Page

After logging in, you'll see the connection page with two modes:

#### Mode 1: Mock Data (No Real Connections)

1. Check **"Use Mock Data"**
2. Click **"Test Source Connection"**
3. Wait for the success message
4. Click **"Proceed to Dashboard →"**

#### Mode 2: Real Database Connections

See [Section 10: Connecting Real Systems](#10-connecting-real-systems) for detailed instructions.

### 5.4 Logout

In the dashboard sidebar, scroll to the bottom and click **Logout** to return to the login page.

---

## 6. Choosing Source and Target Platform

The application supports multiple source and target platforms. You can switch platforms in two ways:

### Option A — Dashboard Sidebar (Recommended)

The sidebar has **Source platform** and **Target platform** radio buttons. Selecting a different value automatically:
1. Updates `config.yaml`
2. Re-runs the pipeline
3. Reloads the dashboard with new data

### Option B — Edit `config.yaml`

#### Source Platforms

**Redshift (default):**
```yaml
source:
  adapter: "mock_redshift"
```

**Snowflake:**
```yaml
source:
  adapter: "mock_snowflake"
```

#### Target Platforms

**Databricks (default):**
```yaml
target:
  platform: "databricks"
```

After changing the configuration, run:

```bash
python -m src.run_demo
```

The dashboard will automatically adapt to the configured platforms. All UI labels update dynamically.

---

## 7. Running the Full Pipeline

### From the Command Line

```bash
cd C:\dev\data-migration
python -m src.run_demo
```

### From the Dashboard

Click the **Run Full Demo Pipeline** button in the sidebar. The backend will:
1. Execute all 5 pipeline steps
2. Send real-time progress via WebSocket
3. Automatically reload the dashboard when complete

### Individual Steps

You can run each pipeline step independently:

**Redshift Path:**
```bash
python -m src.mock_redshift          # Step 1: Generate Redshift catalog
python -m src.mock_converter         # Step 2: Transpile SQL
```

**Snowflake Path:**
```bash
python -m src.mock_snowflake         # Step 1: Generate Snowflake catalog
python -m src.mock_snowflake_converter  # Step 2: Transpile SQL
```

**Shared Steps:**
```bash
python -m src.mock_loader            # Step 3: Generate Parquet files
python -m src.mock_validator         # Step 4: Run validation checks
python -m src.test_runner            # Step 5: Execute test suite
```

---

## 8. Dashboard Tabs Reference

The dashboard has **10 tabs** organized by workflow:

### Tab 1: Executive Summary
High-level migration overview with key metrics and charts.

### Tab 2: Overview
Detailed migration summary with:
- KPI cards: total tables, auto-converted %, manual rewrite count, avg confidence, validation pass rate
- Query volume timeline (90-day bar chart with 7-day rolling average)
- Object classification pie chart (AUTO / WARNINGS / MANUAL)
- Confidence score distribution histogram
- Table size distribution (top 15 by MB)

### Tab 3: Objects
Searchable, filterable table of all migration objects:
- Filter by name, classification, or object type
- Columns: object name, schema, type, classification, difficulty score, rules applied, warnings, manual flags
- CSV export button

### Tab 4: Schema Explorer
Detailed schema and column browser:
- **Schema summary** — Table count, total rows, total size per schema
- **Table selector** — Pick any table to inspect
- **Table properties** — Row count, size, and platform-specific properties
- **Column details** — Every column with its source type and auto-mapped target equivalent
- **Constraints** — Primary keys, foreign keys, unique constraints per table
- **Target DDL preview** — Auto-generated `CREATE TABLE ... USING DELTA` statement
- CSV export for column mappings

### Tab 5: Metadata
Browse stored procedures, UDFs, views, and materialized views:
- **Summary metrics** — Counts per object type
- **Four sub-views**:
  - **Stored Procedures** — Source code, language, args, return type, referenced tables
  - **UDFs** — Source code with automation assessment
  - **Views** — View definitions with dependency analysis
  - **Materialized Views** — Definitions if available
- **Automation Assessment** — Summary of which metadata objects can be auto-migrated

### Tab 6: Lineage
End-to-end data lineage with:
- **Full Lineage Graph** — Network graph with different node shapes and edge styles
- **Query Access Heatmap** — Most-accessed tables from query logs
- **Table Dependencies** — Searchable dependency matrix

### Tab 7: Relationships
Declared and inferred foreign key relationships:
- **Graph** — Interactive network graph
- **Declared FKs** — Table of all FK constraints
- **Inferred Candidates** — FK candidates from relationship profiler

### Tab 8: SQL Comparison
Side-by-side source vs. target SQL:
- Object selector dropdown
- Info bar: classification, difficulty, rules applied, warnings
- Source and target SQL panels with syntax highlighting
- Unified diff viewer
- Applied rules list

### Tab 9: Validation
Per-table validation scorecards:
- Summary: tables validated, total checks, passed, failed, pass rate
- Table selector with per-table drill-down
- Confidence heatmap
- JSON export

### Tab 10: Manual Work
Categorized list of items requiring human attention:
- Stored procedures (need rewrite)
- Manual rewrite objects
- Flagged items from conversion
- Low-confidence tables
- **Export Review Pack (ZIP)** — Bundles all artifacts
- Test report (embedded HTML)

---

## 9. Configuration

All settings live in `config.yaml` at the project root.

### Key Sections

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
  adapter: "mock_redshift"          # "mock_redshift" | "mock_snowflake" | "redshift" | "snowflake"

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

### Environment Variables

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

## 10. Connecting Real Systems

MigData supports connecting to real source and target systems. You can provide credentials either via the **connection page** in the UI or via **environment variables**.

### 10.1 Real Redshift Connection

#### Prerequisites
- `psycopg2-binary` (already in `requirements.txt`)
- Redshift cluster endpoint
- Database credentials with read access

#### Gather Connection Details
- **Host**: Cluster endpoint (e.g., `my-cluster.abc123.us-east-1.redshift.amazonaws.com`)
- **Port**: Default 5439
- **Database**: Database name (e.g., `analytics_dw`)
- **User**: IAM user or database user
- **Password**: User password

#### Option A — Connection Page (Recommended)

1. On the connection page, select **Redshift** as source platform
2. Fill in the connection details:
   - Host: `my-cluster.abc123.us-east-1.redshift.amazonaws.com`
   - Port: `5439`
   - Database: `analytics_dw`
   - User: `migration_user`
   - Password: `your_password`
3. Click **Test Source Connection**
4. Wait for the green success message
5. Optionally check **"Save connection details for next time"**
6. Click **"Proceed to Dashboard →"**

#### Option B — Environment Variables

**Windows:**
```bash
set REDSHIFT_HOST=my-cluster.abc123.us-east-1.redshift.amazonaws.com
set REDSHIFT_PORT=5439
set REDSHIFT_DB=analytics_dw
set REDSHIFT_USER=migration_user
set REDSHIFT_PASSWORD=your_password
```

**Linux/Mac:**
```bash
export REDSHIFT_HOST=my-cluster.abc123.us-east-1.redshift.amazonaws.com
export REDSHIFT_PORT=5439
export REDSHIFT_DB=analytics_dw
export REDSHIFT_USER=migration_user
export REDSHIFT_PASSWORD=your_password
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

#### Troubleshooting Redshift Connection

| Error Symptom | Likely Cause | Fix |
|---------------|--------------|-----|
| Timeout / could not connect | Wrong host/port or security group blocks access | Check Host and Port, ensure your IP is whitelisted |
| Password authentication failed | Wrong user or password | Check User and Password |
| Database does not exist | Wrong database name | Check Database name |
| SSL/TLS error | SSL certificate validation issue | Add `sslmode: 'require'` to config |

---

### 10.2 Real Snowflake Connection

#### Prerequisites
- `snowflake-connector-python>=3.6` (already in `requirements.txt`)
- Snowflake account identifier
- Database credentials with read access
- Warehouse with sufficient capacity

#### Gather Connection Details
- **Account**: Account identifier (e.g., `myorg-myaccount` or `xy12345.us-east-1.aws`)
- **Warehouse**: Warehouse name (e.g., `COMPUTE_WH`)
- **Database**: Database name (e.g., `ANALYTICS_DW`)
- **Role**: Role name (e.g., `SYSADMIN`)
- **User**: Snowflake user
- **Password**: User password

#### Account Identifier Formats

**New URL format** (recommended):
```
myorg-myaccount
```

**Legacy format**:
```
account.region.cloud
# Example: xy12345.us-east-1.aws
```

#### Option A — Connection Page (Recommended)

1. On the connection page, select **Snowflake** as source platform
2. Fill in the connection details:
   - Account: `myorg-myaccount`
   - Warehouse: `COMPUTE_WH`
   - Database: `ANALYTICS_DW`
   - Role: `SYSADMIN`
   - User: `migration_user`
   - Password: `your_password`
3. Click **Test Source Connection**
4. Wait for the green success message
5. Optionally check **"Save connection details for next time"**
6. Click **"Proceed to Dashboard →"**

#### Option B — Environment Variables

**Windows:**
```bash
set SNOWFLAKE_ACCOUNT=myorg-myaccount
set SNOWFLAKE_WAREHOUSE=COMPUTE_WH
set SNOWFLAKE_DB=ANALYTICS_DW
set SNOWFLAKE_USER=migration_user
set SNOWFLAKE_PASSWORD=your_password
set SNOWFLAKE_ROLE=SYSADMIN
```

**Linux/Mac:**
```bash
export SNOWFLAKE_ACCOUNT=myorg-myaccount
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DB=ANALYTICS_DW
export SNOWFLAKE_USER=migration_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=SYSADMIN
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

#### Troubleshooting Snowflake Connection

| Error Symptom | Likely Cause | Fix |
|---------------|--------------|-----|
| Account not found | Wrong account identifier | Check Account identifier (format: org-account) |
| Incorrect username or password | Wrong credentials | Check User and Password |
| Warehouse does not exist / suspended | Wrong warehouse or it needs resuming | Check Warehouse name, ensure it is running |
| Database does not exist | Wrong database name | Check Database name |
| Role not authorized | User doesn't have the role | Check Role assignment or use default role |
| Network timeout | Firewall or network issue | Check network connectivity, try from different location |

#### Query Log Access

Query log extraction requires `ACCOUNTADMIN` or `MONITOR` privilege to access `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`. Without it, catalog extraction still works but query logs will be empty.

To grant privileges:

```sql
GRANT ROLE ACCOUNTADMIN TO USER migration_user;
-- OR
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SYSADMIN;
```

#### Performance Tips

1. **Use a dedicated warehouse**: Create a small warehouse for migration tasks
2. **Resume warehouse**: Ensure the warehouse is running before connecting
3. **Optimize queries**: The adapter queries `INFORMATION_SCHEMA` which can be slow for large databases

---

### 10.3 Connect to Databricks (Target)

#### Prerequisites
- Databricks workspace
- Personal access token
- SQL warehouse HTTP path

#### Gather Connection Details
- **Host**: Workspace URL (e.g., `your-workspace.cloud.databricks.com`)
- **Access Token**: Personal access token (starts with `dapi...`)
- **HTTP Path**: SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/abc123def456`)

#### Option A — Connection Page

1. On the connection page, uncheck **"Skip target connection"**
2. Fill in:
   - Host: `your-workspace.cloud.databricks.com`
   - Access Token: `dapi...`
   - HTTP Path: `/sql/1.0/warehouses/abc123def456`
3. Click **Test Target Connection**
4. Wait for the success message

#### Option B — Environment Variables

**Windows:**
```bash
set DATABRICKS_HOST=your-workspace.cloud.databricks.com
set DATABRICKS_TOKEN=your_token
set DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
```

**Linux/Mac:**
```bash
export DATABRICKS_HOST=your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your_token
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
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

## 11. Troubleshooting

### 11.1 Backend Issues

#### "No module named 'fastapi'"
**Solution:** Install Python dependencies
```bash
pip install -r requirements.txt
```

#### "Address already in use" (Port 8000)
**Solution:** Kill the process or use a different port
```bash
# Windows
netstat -ano | findstr :8000
taskkill /PID <process_id> /F

# Use different port
python -m uvicorn api.main:app --reload --port 8001
```

#### "No data yet" in dashboard
**Solution:** Run the pipeline first
```bash
python -m src.run_demo
```

### 11.2 Frontend Issues

#### "npm: command not found"
**Solution:** Install Node.js from https://nodejs.org/

#### "Cannot find module 'react'"
**Solution:** Install Node dependencies
```bash
cd web
npm install
```

#### "Failed to fetch" or "Network Error"
**Cause:** Backend not running or CORS issue

**Solution:** Ensure backend is running on port 8000
```bash
python -m uvicorn api.main:app --reload
```

#### Frontend won't start (Port 5173 in use)
**Solution:** Kill the process or use a different port
```bash
# Windows
netstat -ano | findstr :5173
taskkill /PID <process_id> /F

# Or edit vite.config.ts to change port
```

### 11.3 Database Connection Issues

#### Redshift: "Connection refused"
- Check security group allows inbound traffic on port 5439
- Verify VPC and subnet settings
- Check if cluster is publicly accessible

#### Snowflake: "Authentication failed"
- Verify account identifier format
- Check username and password
- Ensure user has necessary privileges
- Check if MFA is required (may need OAuth flow)

#### Databricks: "Invalid token"
- Generate a new personal access token
- Check token hasn't expired
- Verify workspace URL format (no https://)

### 11.4 Pipeline Errors

#### "FileNotFoundError: source_catalog.json"
**Solution:** Run Step 1 first
```bash
python -m src.mock_redshift  # or mock_snowflake
```

#### "ModuleNotFoundError: No module named 'src'"
**Solution:** Run from project root, not from src/
```bash
cd C:\dev\data-migration
python -m src.run_demo
```

#### Pipeline step fails with timeout
**Solution:** Increase timeout in pipeline runner or run step individually

### 11.5 Clearing Generated Data

To start fresh, delete output directories:

**Windows:**
```bash
rmdir /s /q mock_data artifacts test_results
python -m src.run_demo
```

**Linux/Mac:**
```bash
rm -rf mock_data artifacts test_results
python -m src.run_demo
```

---

## Need More Help?

- Check the API documentation: http://localhost:8000/docs
- Review logs in `artifacts/logs/`
- Check `config.yaml` for configuration issues
- Report issues at the project repository

---

**End of User Guide**
