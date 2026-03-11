# MigData — Real Snowflake End-to-End Guide

This guide walks you through connecting MigData to a **real Snowflake instance** so the dashboard displays your actual schemas, tables, columns, constraints, procedures, views, and query history instead of mock data.

---

## Prerequisites

1. **Python 3.10+** with the project virtual environment activated
2. **Snowflake account** with network access from your machine
3. **`snowflake-connector-python`** installed:
   ```bash
   pip install snowflake-connector-python
   ```
4. A Snowflake **role** with read access to:
   - `INFORMATION_SCHEMA` (tables, columns, constraints, procedures, functions, views)
   - `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` (optional — for query log extraction)

---

## Step 1: Gather Snowflake Connection Details

You will need:

| Field       | Env Variable           | Example                           | Description                                      |
|-------------|------------------------|-----------------------------------|--------------------------------------------------|
| Account     | `SNOWFLAKE_ACCOUNT`    | `HX52632.AWS_AP_SOUTHEAST_1`     | Org-account identifier (with region if needed)    |
| Warehouse   | `SNOWFLAKE_WAREHOUSE`  | `COMPUTE_WH`                      | Virtual warehouse for running queries             |
| Database    | `SNOWFLAKE_DB`         | `SNOWFLAKE_SAMPLE_DATA`           | Database to extract metadata from                 |
| User        | `SNOWFLAKE_USER`       | `rnanavaty`                       | Snowflake login username                          |
| Password    | `SNOWFLAKE_PASSWORD`   | `********`                        | Snowflake login password                          |
| Role        | `SNOWFLAKE_ROLE`       | `ACCOUNTADMIN`                    | Role with INFORMATION_SCHEMA access               |

> **Tip:** For the `account` field, use the format `<orgname>.<account_name>` or `<account_locator>.<region>.<cloud>` depending on your Snowflake edition.

---

## Step 2: Configure `config.yaml`

The config file uses `${VAR}` placeholders — **never hardcode real credentials** in this file.

```yaml
source:
  adapter: "snowflake"          # MUST be "snowflake" (not "mock_snowflake")
  snowflake:
    account: "${SNOWFLAKE_ACCOUNT}"
    warehouse: "${SNOWFLAKE_WAREHOUSE}"
    database: "${SNOWFLAKE_DB}"
    user: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"
    role: "${SNOWFLAKE_ROLE}"
```

### Key Points

- **`adapter: "snowflake"`** — This is the critical setting. It tells the pipeline to use `SnowflakeSourceAdapter` (real queries) instead of `MockSourceAdapter` (fake data).
- All `${VAR}` placeholders are resolved at runtime from environment variables.
- Values can come from **two sources** (in priority order):
  1. **UI input** — Entered on the Connection page (sets env vars for that session)
  2. **Environment variables** — Set in your terminal before launching

---

## Step 3: Set Environment Variables

Credentials are passed via environment variables. Choose one method:

### Method A: Via the UI (Recommended)

No command-line setup needed. The Connection page in the app lets you enter credentials interactively. When you click "Proceed to Dashboard", the app sets `os.environ` for that session automatically.

### Method B: Temporary — Set in Current Terminal Session

**Windows (CMD):**
```cmd
set SNOWFLAKE_ACCOUNT=HX52632.AWS_AP_SOUTHEAST_1
set SNOWFLAKE_WAREHOUSE=COMPUTE_WH
set SNOWFLAKE_DB=SNOWFLAKE_SAMPLE_DATA
set SNOWFLAKE_USER=rnanavaty
set SNOWFLAKE_PASSWORD=YourPasswordHere
set SNOWFLAKE_ROLE=ACCOUNTADMIN
```

**Windows (PowerShell):**
```powershell
$env:SNOWFLAKE_ACCOUNT = "HX52632.AWS_AP_SOUTHEAST_1"
$env:SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
$env:SNOWFLAKE_DB = "SNOWFLAKE_SAMPLE_DATA"
$env:SNOWFLAKE_USER = "rnanavaty"
$env:SNOWFLAKE_PASSWORD = "YourPasswordHere"
$env:SNOWFLAKE_ROLE = "ACCOUNTADMIN"
```

**Linux / macOS (Bash):**
```bash
export SNOWFLAKE_ACCOUNT="HX52632.AWS_AP_SOUTHEAST_1"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DB="SNOWFLAKE_SAMPLE_DATA"
export SNOWFLAKE_USER="rnanavaty"
export SNOWFLAKE_PASSWORD="YourPasswordHere"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"
```

> These variables only last for the current terminal session. Close the terminal and they are gone.

### Method C: Using a `.env` File (Optional)

Create a `.env` file in the project root (already in `.gitignore`):

```
SNOWFLAKE_ACCOUNT=HX52632.AWS_AP_SOUTHEAST_1
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DB=SNOWFLAKE_SAMPLE_DATA
SNOWFLAKE_USER=rnanavaty
SNOWFLAKE_PASSWORD=YourPasswordHere
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

Then load it before launching:

**Windows (CMD):** `for /f "tokens=*" %i in (.env) do set %i`
**PowerShell:** `Get-Content .env | ForEach-Object { if ($_ -match '^(.+?)=(.*)$') { [Environment]::SetEnvironmentVariable($matches[1], $matches[2]) } }`
**Bash:** `export $(cat .env | xargs)`

---

## Step 4: Launch the Application

### Option A: Via Streamlit UI (Recommended)

```bash
streamlit run app.py
```

1. **Login page** — Enter your app credentials and log in.
2. **Connection page:**
   - **Uncheck** "Use Mock Data (skip real connections)".
   - Select **Snowflake** as the Source Platform.
   - Fill in Account, Warehouse, Database, Role, User, Password.
   - Click **"Test Source Connection"** — you should see a green "Connected to Snowflake!" message.
   - Target is optional — check "Skip target connection" if you don't have Databricks.
   - Click **"Proceed to Dashboard"**.
3. The pipeline will run automatically, extracting real metadata from your Snowflake account.
4. The dashboard will display your **actual** schemas, tables, columns, and query logs.

> When you enter credentials on the UI and click "Proceed", the app sets `os.environ` for that Streamlit session. The `${VAR}` placeholders in `config.yaml` resolve to these values. No need to set env vars separately.

### Option B: Via Command Line (env vars must be set first)

```bash
# Set env vars first (see Step 3 Method B), then:
python -m src.run_demo
```

This runs the full pipeline and launches Streamlit. To run without the UI:

```bash
python -m src.run_demo --no-ui
```

---

## Step 5: Verify Real Data on Dashboard

After the pipeline completes, confirm:

| Dashboard Section     | What to Check                                                  |
|-----------------------|----------------------------------------------------------------|
| **Left sidebar**      | Schema list should match your Snowflake database schemas       |
| **Overview tab**      | Table count, column count, and row estimates are from your DB  |
| **Objects tab**       | Lists real tables with actual sizes and row counts             |
| **Schema Explorer**   | Shows real columns, data types, and nullable flags             |
| **Relationships**     | Displays actual PK/FK constraints                              |
| **Lineage tab**       | Built from real query history (if ACCOUNT_USAGE is accessible) |

---

## What the Pipeline Does (Real Mode)

When `adapter: "snowflake"`, the pipeline runs these steps:

1. **Step 1 — Extract Source Metadata**
   - Connects to Snowflake using `snowflake-connector-python`
   - Queries `INFORMATION_SCHEMA.TABLES` for table list + sizes
   - Queries `INFORMATION_SCHEMA.COLUMNS` for column details
   - Queries `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE` for constraints
   - Queries `INFORMATION_SCHEMA.PROCEDURES`, `FUNCTIONS`, `VIEWS` for stored objects
   - Queries `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` for last 90 days of queries
   - Saves everything to `mock_data/source_catalog.json` and `mock_data/query_logs.json`

2. **Step 2 — SQL Conversion** (mock engine transpiles Snowflake SQL to Spark SQL)
3. **Step 3 — Data Load Simulation** (generates sample Parquet files)
4. **Step 4 — Validation Checks** (runs confidence-score validation)
5. **Step 5 — Test Suite** (runs automated tests)

---

## How Credential Resolution Works

```
config.yaml                    Environment Variables          UI Input
    |                                |                          |
    v                                v                          v
 ${SNOWFLAKE_ACCOUNT}  --->  os.environ["SNOWFLAKE_ACCOUNT"]  <--- set by Connection page
 ${SNOWFLAKE_PASSWORD} --->  os.environ["SNOWFLAKE_PASSWORD"] <--- set by Connection page
    ...                              ...                        ...
    |
    v
 config.py: _walk_interpolate()  -- replaces ${VAR} with env var value
    |
    v
 snowflake_adapter.py: reads resolved values from config
    |
    v
 snowflake.connector.connect(account=..., password=..., ...)
```

Priority: If a value is set both in `config.yaml` (as a literal) and as an env var, the literal value wins. Use `${VAR}` placeholders to always defer to env vars.

---

## Switching Back to Mock Data

To revert to mock/demo mode:

1. Change `config.yaml`:
   ```yaml
   source:
     adapter: "mock_snowflake"
   ```
2. Restart Streamlit (`Ctrl+C`, then `streamlit run app.py`).

Or use the UI: on the Connection page, check **"Use Mock Data"** and proceed.

---

## Troubleshooting

### "Connection failed: 250001 — Could not connect to Snowflake backend"

This error means the Snowflake Python connector could not reach the Snowflake backend.
Work through the checklist below in order:

#### Step 1 — Verify the account identifier

The most common cause is an incorrectly formatted account identifier.

| ✗ Wrong (will fail)                              | ✓ Correct                    |
|--------------------------------------------------|------------------------------|
| `https://myorg.snowflakecomputing.com`           | `myorg-myaccount`            |
| `myorg.snowflakecomputing.com`                   | `xy12345.us-east-1.aws`      |
| `https://xy12345.us-east-1.aws.snowflakecomputing.com` | `xy12345.us-east-1.aws` |

**MigData normalises the account identifier automatically** — it strips `https://`
and `.snowflakecomputing.com` before passing it to the connector.  However, if the
value entered is something completely different (e.g. a full URL with a path), the
normalised form may still be invalid.

To confirm your account identifier, log in to the Snowflake web UI.  The URL in
your browser will look like:
```
https://<account-identifier>.snowflakecomputing.com/
```
Copy everything between `https://` and `.snowflakecomputing.com/`.

#### Step 2 — Check network / firewall access

Snowflake requires your machine to reach its backend servers over **HTTPS (port 443)**.
Corporate VPNs and restrictive firewalls often block these connections.

**Find the exact hostnames and ports you need to whitelist:**

1. Connect to your Snowflake account via the Snowflake web UI (Snowsight).
2. Open a worksheet and run:
   ```sql
   SELECT * FROM TABLE(FLATTEN(INPUT => PARSE_JSON(SYSTEM$ALLOWLIST())));
   ```
3. This returns a list of `host:port` entries.  Every entry marked `SNOWFLAKE_DEPLOYMENT`
   or `STAGE` must be reachable from your machine.

Typical entries you will see (exact values vary by account region):

| Type                    | Example host                              | Port |
|-------------------------|-------------------------------------------|------|
| `SNOWFLAKE_DEPLOYMENT`  | `myorg-myaccount.snowflakecomputing.com`  | 443  |
| `OCSP_CACHE`            | `ocsp.snowflakecomputing.com`             | 80   |
| `STAGE`                 | `*.s3.amazonaws.com`                      | 443  |

Forward the full list from `SYSTEM$ALLOWLIST()` to your network/firewall team and
ask them to allow outbound connections to each listed host on the listed port.

**Quick local test (without running the app):**

```bash
# Windows (PowerShell)
Test-NetConnection -ComputerName myorg-myaccount.snowflakecomputing.com -Port 443

# macOS / Linux
nc -zv myorg-myaccount.snowflakecomputing.com 443
```

If this command hangs or returns "refused/failed", your firewall is blocking the
connection and you need to contact your network administrator.

#### Step 3 — Try from a different network

If possible, temporarily disable VPN or try from a home/mobile network.  If the
connection succeeds there, the issue is your corporate firewall.

#### Step 4 — Check the Snowflake status page

Visit [status.snowflake.com](https://status.snowflake.com) to confirm there are
no active incidents on your Snowflake deployment region.

---

### "Incorrect username or password"

- Double-check credentials — entered on UI or set via env vars.
- Ensure the user is not locked out in Snowflake (check Snowflake Admin console).

### "Warehouse 'X' does not exist or not authorized"

- Verify the warehouse name matches exactly (case-sensitive in some configs).
- Ensure the warehouse is **running** (not suspended) or that your role can auto-resume it.
- Check that the role has `USAGE` privilege on the warehouse.

### "Database 'X' does not exist or not authorized"

- Verify the database name (e.g., `SNOWFLAKE_SAMPLE_DATA` is Snowflake's built-in sample DB).
- Check that the role has `USAGE` privilege on the database.

### Dashboard still shows mock data after connecting

- Ensure `config.yaml` has `adapter: "snowflake"` (not `"mock_snowflake"`).
- After changing config, either:
  - Restart Streamlit, or
  - Click "Run Full Demo Pipeline" in the sidebar.
- Check the Streamlit terminal output for errors during pipeline execution.

### Query logs are empty

- `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` requires the `ACCOUNTADMIN` role or a role with `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database.
- If your role doesn't have access, query logs will be empty (not an error — the pipeline continues).
- Grant access:
  ```sql
  GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE YOUR_ROLE;
  ```

### Pipeline times out

- The real pipeline has a 10-minute (600s) timeout. If your Snowflake account has many schemas/tables, it may take longer.
- For very large accounts, consider restricting to a specific database with fewer tables.

---

## Security Notes

- **Never hardcode real passwords** in `config.yaml` or any source file. Always use `${VAR}` placeholders.
- The "Save connection details" checkbox on the Connection page writes credentials in **plain text** to `config.yaml`. Avoid this — use env vars instead.
- Add `config.yaml` to `.gitignore` if it ever contains real credentials.
- Environment variables set via `set`/`export` are temporary and cleared when the terminal closes.

---


## Quick Reference

| Config Key         | Mock Mode            | Real Snowflake Mode         |
|--------------------|----------------------|-----------------------------|
| `source.adapter`   | `"mock_snowflake"`   | `"snowflake"`               |
| `source.snowflake` | `${ENV_VAR}` placeholders | `${ENV_VAR}` placeholders |
| `conversion.engine`| `"mock"`             | `"mock"` (or `"llm"`)      |
| `loader.engine`    | `"mock"`             | `"mock"` (or `"databricks"`)  |
| `validation.engine`| `"mock"`             | `"mock"` (or `"live"`)     |

| Env Variable           | Description                        |
|------------------------|------------------------------------|
| `SNOWFLAKE_ACCOUNT`    | Account identifier with region     |
| `SNOWFLAKE_WAREHOUSE`  | Virtual warehouse name             |
| `SNOWFLAKE_DB`         | Database to extract metadata from  |
| `SNOWFLAKE_USER`       | Login username                     |
| `SNOWFLAKE_PASSWORD`   | Login password                     |
| `SNOWFLAKE_ROLE`       | Role with INFORMATION_SCHEMA access|
