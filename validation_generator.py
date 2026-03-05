"""
validation_generator.py
=======================
Reads source catalog, transpiled objects, and FK candidates to generate
data parity validation suites for Redshift→Databricks migration.

Generates:
  1. Row count checks for every table
  2. Checksum/hash checks (md5 of concatenated stable columns or column sums)
  3. Column-level stats: null_count, distinct_count, min, max (numeric/date)
  4. Key uniqueness checks for declared PKs or inferred high-confidence PKs

Output modes:
  - Great Expectations suites (JSON per table)
  - PyDeequ metric definitions (Python script per table)
  - Reconciliation SQL pairs (source Redshift + target Databricks)

Also produces:
  run_reconciliation.py — executable runner for source/target parity checks

Outputs:
  artifacts/validation/ge_suite_<table>.json   — GE expectation suites
  artifacts/validation/deequ_<table>.py        — PyDeequ check scripts
  artifacts/validation/recon_queries.json      — paired SQL for source & target
  run_reconciliation.py                        — reconciliation runner script
"""

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
CATALOG_PATH = ARTIFACTS_DIR / "source_catalog.json"
FK_CANDIDATES_PATH = ARTIFACTS_DIR / "fk_candidates.csv"
VALIDATION_DIR = ARTIFACTS_DIR / "validation"
RECON_QUERIES_PATH = VALIDATION_DIR / "recon_queries.json"
RECON_RUNNER_PATH = Path(__file__).resolve().parent / "run_reconciliation.py"

TARGET_PROD_DB = "prod"

NUMERIC_TYPES = {
    "smallint", "int2", "integer", "int", "int4", "bigint", "int8",
    "real", "float4", "float", "float8", "double precision",
    "numeric", "decimal",
}
DATE_TYPES = {
    "date", "timestamp", "timestamp without time zone",
    "timestamp with time zone", "timestamptz",
}

EXCLUDED_SCHEMAS = ("pg_catalog", "information_schema", "pg_internal")

# ---------------------------------------------------------------------------
# Logging (deferred file handler — artifacts dir created at runtime)
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_console)


def _init_file_logging():
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(ARTIFACTS_DIR / "validation_generator.log", mode="w")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)


# ═══════════════════════════════════════════════════════════════════════════
# Loaders
# ═══════════════════════════════════════════════════════════════════════════

def _load_json(path: Path):
    if not path.exists():
        logger.warning("File not found: %s", path)
        return None
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def _load_catalog() -> dict:
    catalog = _load_json(CATALOG_PATH)
    return catalog or {}


def _load_fk_candidates() -> list[dict]:
    import csv
    if not FK_CANDIDATES_PATH.exists():
        return []
    with open(FK_CANDIDATES_PATH, "r", encoding="utf-8") as fp:
        lines = [l for l in fp if not l.startswith("#")]
    return list(csv.DictReader(lines))


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _sanitise_name(schema: str, table: str) -> str:
    s = re.sub(r"[^a-z0-9_]", "_", schema.lower())
    t = re.sub(r"[^a-z0-9_]", "_", table.lower())
    return f"{s}_{t}"


def _build_table_columns(catalog: dict) -> dict:
    """Build {(schema, table): [col_dict, ...]}."""
    result = defaultdict(list)
    for c in catalog.get("columns", []):
        schema = (c.get("table_schema") or "").lower()
        table = (c.get("table_name") or "").lower()
        if schema in EXCLUDED_SCHEMAS:
            continue
        result[(schema, table)].append(c)
    return dict(result)


def _build_pk_columns(catalog: dict) -> dict:
    """Build {(schema, table): [col_name, ...]} for PK constraints."""
    pks = defaultdict(list)
    for c in catalog.get("constraints", []):
        ctype = (c.get("constraint_type") or "").upper()
        if ctype == "PRIMARY KEY":
            schema = (c.get("constraint_schema") or "").lower()
            table = (c.get("table_name") or "").lower()
            col = (c.get("column_name") or "").lower()
            if col and col not in pks[(schema, table)]:
                pks[(schema, table)].append(col)
    return dict(pks)


def _build_inferred_pk_columns(fk_candidates: list[dict]) -> dict:
    """
    Build inferred unique key columns from FK candidates where
    parent_unique_bool is true and classification is highly_likely.
    """
    pks = defaultdict(list)
    for c in fk_candidates:
        if (c.get("classification") in ("highly_likely",) and
                str(c.get("parent_unique_bool", "")).lower() == "true"):
            ps = (c.get("parent_schema") or "").lower()
            pt = (c.get("parent_table") or "").lower()
            pc = (c.get("parent_col") or "").lower()
            if pc and pc not in pks[(ps, pt)]:
                pks[(ps, pt)].append(pc)
    return dict(pks)


def _get_stable_columns(columns: list[dict], max_cols: int = 5) -> list[str]:
    """Pick stable columns for hash check (prefer non-nullable, non-float)."""
    candidates = []
    for c in sorted(columns, key=lambda x: int(x.get("ordinal_position", 0))):
        dtype = (c.get("data_type") or "").lower()
        # Skip float types (non-deterministic hashing)
        if dtype in ("real", "float4", "float", "float8", "double precision"):
            continue
        candidates.append(c.get("column_name", ""))
    return candidates[:max_cols]


def _get_numeric_columns(columns: list[dict]) -> list[dict]:
    return [c for c in columns if (c.get("data_type") or "").lower() in NUMERIC_TYPES]


def _get_date_columns(columns: list[dict]) -> list[dict]:
    return [c for c in columns if (c.get("data_type") or "").lower() in DATE_TYPES]


# ═══════════════════════════════════════════════════════════════════════════
# SQL Generation — Reconciliation Pairs
# ═══════════════════════════════════════════════════════════════════════════

def generate_row_count_sql(schema: str, table: str) -> dict:
    src_fqn = f"{schema}.{table}"
    tgt_fqn = f"{TARGET_PROD_DB}.{_sanitise_name(schema, table)}"
    return {
        "check": "row_count",
        "source_sql": f"SELECT COUNT(*) AS cnt FROM {src_fqn};",
        "target_sql": f"SELECT COUNT(*) AS cnt FROM {tgt_fqn};",
        "comparison": "src.cnt == tgt.cnt",
    }


def generate_hash_check_sql(schema: str, table: str, columns: list[dict]) -> dict | None:
    stable = _get_stable_columns(columns)
    if not stable:
        return None
    src_fqn = f"{schema}.{table}"
    tgt_fqn = f"{TARGET_PROD_DB}.{_sanitise_name(schema, table)}"

    concat_expr_src = " || '|' || ".join(
        f"COALESCE(CAST({c} AS VARCHAR), '')" for c in stable
    )
    concat_expr_tgt = " || '|' || ".join(
        f"COALESCE(CAST(`{c}` AS STRING), '')" for c in stable
    )

    return {
        "check": "hash_checksum",
        "columns_used": stable,
        "source_sql": (
            f"SELECT COUNT(*) AS src_cnt, "
            f"SUM(HASHTEXT({concat_expr_src})) AS src_hash "
            f"FROM {src_fqn};"
        ),
        "target_sql": (
            f"SELECT COUNT(*) AS tgt_cnt, "
            f"SUM(HASH({concat_expr_tgt})) AS tgt_hash "
            f"FROM {tgt_fqn};"
        ),
        "comparison": "src.src_cnt == tgt.tgt_cnt (hash comparison is directional — validate manually if counts match but hashes differ due to function differences)",
        "note": "HASHTEXT (Redshift) vs HASH (Spark) may produce different values. "
                "If counts match, compare column-level aggregates instead.",
    }


def generate_column_stats_sql(
    schema: str, table: str, columns: list[dict]
) -> list[dict]:
    checks = []
    src_fqn = f"{schema}.{table}"
    tgt_fqn = f"{TARGET_PROD_DB}.{_sanitise_name(schema, table)}"

    # Null count + distinct count for ALL columns
    for c in columns:
        col = c.get("column_name", "")
        checks.append({
            "check": "column_null_distinct",
            "column": col,
            "source_sql": (
                f"SELECT "
                f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count, "
                f"COUNT(DISTINCT {col}) AS distinct_count "
                f"FROM {src_fqn};"
            ),
            "target_sql": (
                f"SELECT "
                f"SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END) AS null_count, "
                f"COUNT(DISTINCT `{col}`) AS distinct_count "
                f"FROM {tgt_fqn};"
            ),
            "comparison": "src.null_count == tgt.null_count AND src.distinct_count == tgt.distinct_count",
        })

    # Min/max for numeric columns
    for c in _get_numeric_columns(columns):
        col = c.get("column_name", "")
        checks.append({
            "check": "numeric_min_max_sum",
            "column": col,
            "source_sql": (
                f"SELECT MIN({col}) AS min_val, MAX({col}) AS max_val, "
                f"SUM(CAST({col} AS DECIMAL(38,4))) AS sum_val "
                f"FROM {src_fqn};"
            ),
            "target_sql": (
                f"SELECT MIN(`{col}`) AS min_val, MAX(`{col}`) AS max_val, "
                f"SUM(CAST(`{col}` AS DECIMAL(38,4))) AS sum_val "
                f"FROM {tgt_fqn};"
            ),
            "comparison": "src.min_val == tgt.min_val AND src.max_val == tgt.max_val AND src.sum_val == tgt.sum_val",
        })

    # Min/max for date columns
    for c in _get_date_columns(columns):
        col = c.get("column_name", "")
        checks.append({
            "check": "date_min_max",
            "column": col,
            "source_sql": (
                f"SELECT MIN({col}) AS min_val, MAX({col}) AS max_val "
                f"FROM {src_fqn};"
            ),
            "target_sql": (
                f"SELECT MIN(`{col}`) AS min_val, MAX(`{col}`) AS max_val "
                f"FROM {tgt_fqn};"
            ),
            "comparison": "src.min_val == tgt.min_val AND src.max_val == tgt.max_val",
        })

    return checks


def generate_pk_uniqueness_sql(
    schema: str, table: str, pk_cols: list[str]
) -> dict | None:
    if not pk_cols:
        return None
    tgt_fqn = f"{TARGET_PROD_DB}.{_sanitise_name(schema, table)}"
    col_list = ", ".join(f"`{c}`" for c in pk_cols)
    return {
        "check": "pk_uniqueness",
        "columns": pk_cols,
        "target_sql": (
            f"SELECT {col_list}, COUNT(*) AS dup_count "
            f"FROM {tgt_fqn} "
            f"GROUP BY {col_list} "
            f"HAVING COUNT(*) > 1 "
            f"LIMIT 10;"
        ),
        "comparison": "Result should be empty (0 rows). Any rows indicate PK violations.",
    }


# ═══════════════════════════════════════════════════════════════════════════
# Great Expectations Suite Generation
# ═══════════════════════════════════════════════════════════════════════════

def generate_ge_suite(
    schema: str,
    table: str,
    columns: list[dict],
    pk_cols: list[str],
    source_row_estimate: int | None = None,
) -> dict:
    """Generate a Great Expectations JSON suite for one table."""
    tgt_name = _sanitise_name(schema, table)
    expectations = []

    # Table-level: row count > 0
    expectations.append({
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {
            "min_value": 1,
        },
        "meta": {"check": "row_count_positive"},
    })

    # If we have a source estimate, check within 1% tolerance
    if source_row_estimate and source_row_estimate > 0:
        lower = int(source_row_estimate * 0.99)
        upper = int(source_row_estimate * 1.01)
        expectations.append({
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {
                "min_value": lower,
                "max_value": upper,
            },
            "meta": {
                "check": "row_count_parity",
                "source_estimate": source_row_estimate,
                "tolerance": "1%",
            },
        })

    # Column-level expectations
    for c in columns:
        col = c.get("column_name", "")
        dtype = (c.get("data_type") or "").lower()
        nullable = (c.get("is_nullable") or "YES").upper()

        # Column should exist
        expectations.append({
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": col},
        })

        # Not-null check for non-nullable columns
        if nullable == "NO":
            expectations.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col},
                "meta": {"source_nullable": "NO"},
            })

        # Numeric range checks
        if dtype in NUMERIC_TYPES:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {
                    "column": col,
                    "type_": "IntegerType" if dtype in ("integer", "int", "int4", "smallint", "int2") else "LongType" if dtype in ("bigint", "int8") else "DoubleType",
                },
                "meta": {"source_type": dtype},
            })

        # Date columns — values should be parseable
        if dtype in DATE_TYPES:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_dateutil_parseable",
                "kwargs": {"column": col},
                "meta": {"source_type": dtype},
            })

    # PK uniqueness
    if pk_cols:
        if len(pk_cols) == 1:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": pk_cols[0]},
                "meta": {"check": "pk_uniqueness"},
            })
        else:
            expectations.append({
                "expectation_type": "expect_compound_columns_to_be_unique",
                "kwargs": {"column_list": pk_cols},
                "meta": {"check": "compound_pk_uniqueness"},
            })

    suite = {
        "data_asset_type": "Dataset",
        "expectation_suite_name": f"migration_parity_{tgt_name}",
        "expectations": expectations,
        "meta": {
            "great_expectations_version": "0.18.0",
            "source_schema": schema,
            "source_table": table,
            "target_table": f"{TARGET_PROD_DB}.{tgt_name}",
            "generated_by": "validation_generator.py",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }
    return suite


# ═══════════════════════════════════════════════════════════════════════════
# PyDeequ Script Generation
# ═══════════════════════════════════════════════════════════════════════════

def generate_deequ_script(
    schema: str,
    table: str,
    columns: list[dict],
    pk_cols: list[str],
) -> str:
    """Generate a PyDeequ Python script for one table."""
    tgt_name = _sanitise_name(schema, table)
    tgt_fqn = f"{TARGET_PROD_DB}.{tgt_name}"

    numeric_cols = _get_numeric_columns(columns)
    date_cols = _get_date_columns(columns)
    non_nullable = [c for c in columns if (c.get("is_nullable") or "YES").upper() == "NO"]

    lines = [
        f'"""',
        f"PyDeequ validation for {tgt_fqn}",
        f"Source: {schema}.{table}",
        f"Generated by validation_generator.py",
        f'"""',
        "",
        "from pyspark.sql import SparkSession",
        "from pydeequ.checks import Check, CheckLevel",
        "from pydeequ.verification import VerificationSuite, VerificationResult",
        "from pydeequ.analyzers import (",
        "    AnalysisRunner, AnalyzerContext,",
        "    Size, Completeness, Uniqueness, Mean, Minimum, Maximum,",
        "    CountDistinct, ApproxCountDistinct,",
        ")",
        "",
        'spark = SparkSession.builder.appName("deequ_validation").getOrCreate()',
        "",
        f'df = spark.table("{tgt_fqn}")',
        "",
        "# ── Analysis (metrics collection) ────────────────────────────",
        "analysis_result = (",
        "    AnalysisRunner(spark)",
        "    .onData(df)",
        "    .addAnalyzer(Size())",
    ]

    for c in columns:
        col = c["column_name"]
        lines.append(f'    .addAnalyzer(Completeness("{col}"))')
        lines.append(f'    .addAnalyzer(ApproxCountDistinct("{col}"))')

    for c in numeric_cols:
        col = c["column_name"]
        lines.append(f'    .addAnalyzer(Minimum("{col}"))')
        lines.append(f'    .addAnalyzer(Maximum("{col}"))')
        lines.append(f'    .addAnalyzer(Mean("{col}"))')

    lines.extend([
        "    .run()",
        ")",
        "",
        "analysis_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)",
        'analysis_df.show(truncate=False)',
        "",
        "# ── Verification (pass/fail checks) ─────────────────────────",
        f'check = Check(spark, CheckLevel.Warning, "migration_parity_{tgt_name}")',
        "",
        "check = (check",
        '    .hasSize(lambda sz: sz > 0, "Table must not be empty")',
    ])

    for c in non_nullable:
        col = c["column_name"]
        lines.append(f'    .isComplete("{col}")')

    if pk_cols:
        if len(pk_cols) == 1:
            lines.append(f'    .isUnique("{pk_cols[0]}")')
        else:
            pk_str = '", "'.join(pk_cols)
            lines.append(f'    .hasUniqueness(["{pk_str}"], lambda u: u == 1.0)')

    for c in numeric_cols:
        col = c["column_name"]
        lines.append(f'    .isNonNegative("{col}")  # Adjust if negative values expected')

    lines.extend([
        ")",
        "",
        "result = VerificationSuite(spark).onData(df).addCheck(check).run()",
        "result_df = VerificationResult.checkResultsAsDataFrame(spark, result)",
        'result_df.show(truncate=False)',
        "",
        "# ── Save metrics ─────────────────────────────────────────────",
        f'analysis_df.write.mode("overwrite").json("dbfs:/mnt/migration/validation/deequ_{tgt_name}_metrics")',
        f'result_df.write.mode("overwrite").json("dbfs:/mnt/migration/validation/deequ_{tgt_name}_results")',
    ])

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Reconciliation Runner Generation
# ═══════════════════════════════════════════════════════════════════════════

def generate_reconciliation_runner() -> str:
    """Generate run_reconciliation.py."""
    return r'''"""
run_reconciliation.py
=====================
Executes source (Redshift) and target (Databricks) SQL pairs from
artifacts/validation/recon_queries.json and writes pass/fail results
to artifacts/validation_results.json.

Usage:
  python run_reconciliation.py
  python run_reconciliation.py --source-only    # only run Redshift queries
  python run_reconciliation.py --target-only    # only run Databricks queries
  python run_reconciliation.py --tables schema.table1,schema.table2

Environment variables:
  Redshift:    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD
  Databricks:  DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID, DATABRICKS_HTTP_PATH
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
VALIDATION_DIR = ARTIFACTS_DIR / "validation"
RECON_QUERIES_PATH = VALIDATION_DIR / "recon_queries.json"
RESULTS_PATH = ARTIFACTS_DIR / "validation_results.json"

MAX_RETRIES = 3
RETRY_DELAY = 5

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ARTIFACTS_DIR / "reconciliation.log", mode="w"),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_redshift_connection():
    required = ["REDSHIFT_HOST", "REDSHIFT_PORT", "REDSHIFT_DB",
                "REDSHIFT_USER", "REDSHIFT_PASSWORD"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"Missing Redshift env vars: {', '.join(missing)}")
    conn = psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ["REDSHIFT_PORT"]),
        dbname=os.environ["REDSHIFT_DB"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
        connect_timeout=30,
    )
    conn.autocommit = True
    return conn


def get_databricks_connection():
    """Connect to Databricks via databricks-sql-connector."""
    try:
        from databricks import sql as dbsql
    except ImportError:
        raise ImportError(
            "databricks-sql-connector is required. "
            "Install with: pip install databricks-sql-connector"
        )
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    if not all([host, token, http_path]):
        raise EnvironmentError(
            "Missing Databricks env vars: DATABRICKS_HOST, "
            "DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH"
        )
    return dbsql.connect(
        server_hostname=host.replace("https://", ""),
        http_path=http_path,
        access_token=token,
    )


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def execute_query(cursor, sql, label="query"):
    """Execute a SQL query with retry and return first row as dict."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            if rows:
                return {col: _serialise(val) for col, val in zip(columns, rows[0])}
            return {}
        except Exception as exc:
            logger.warning("Attempt %d/%d for [%s] failed: %s",
                           attempt, MAX_RETRIES, label, exc)
            if attempt == MAX_RETRIES:
                return {"__error__": str(exc)}
            time.sleep(RETRY_DELAY * attempt)
    return {"__error__": "max_retries_exceeded"}


def _serialise(val):
    """Make a value JSON-serialisable."""
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    return str(val)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def compare_results(source: dict, target: dict, check: dict) -> dict:
    """Compare source and target query results."""
    if "__error__" in source:
        return {"status": "ERROR_SOURCE", "detail": source["__error__"]}
    if "__error__" in target:
        return {"status": "ERROR_TARGET", "detail": target["__error__"]}

    check_type = check.get("check", "")

    if check_type == "row_count":
        s = source.get("cnt", source.get("src_cnt"))
        t = target.get("cnt", target.get("tgt_cnt"))
        match = s == t
        return {
            "status": "PASS" if match else "FAIL",
            "source_value": s,
            "target_value": t,
            "detail": None if match else f"Row count mismatch: {s} vs {t}",
        }

    if check_type == "hash_checksum":
        s_cnt = source.get("src_cnt")
        t_cnt = target.get("tgt_cnt")
        cnt_match = s_cnt == t_cnt
        return {
            "status": "PASS" if cnt_match else "FAIL",
            "source_count": s_cnt,
            "target_count": t_cnt,
            "note": check.get("note", "Hash functions differ between Redshift and Spark"),
            "detail": None if cnt_match else f"Count mismatch: {s_cnt} vs {t_cnt}",
        }

    if check_type in ("column_null_distinct",):
        mismatches = []
        for key in source:
            sv = source.get(key)
            tv = target.get(key)
            if sv != tv:
                mismatches.append(f"{key}: {sv} vs {tv}")
        return {
            "status": "PASS" if not mismatches else "FAIL",
            "source": source,
            "target": target,
            "mismatches": mismatches,
        }

    if check_type in ("numeric_min_max_sum", "date_min_max"):
        mismatches = []
        for key in source:
            sv = source.get(key)
            tv = target.get(key)
            if str(sv) != str(tv):
                mismatches.append(f"{key}: {sv} vs {tv}")
        return {
            "status": "PASS" if not mismatches else "FAIL",
            "source": source,
            "target": target,
            "mismatches": mismatches,
        }

    if check_type == "pk_uniqueness":
        # Target-only check: any rows returned means duplicates exist
        has_dupes = bool(target)
        return {
            "status": "FAIL" if has_dupes else "PASS",
            "detail": "Duplicate PK values found" if has_dupes else None,
            "sample_duplicates": target if has_dupes else None,
        }

    # Generic comparison
    return {"status": "UNKNOWN", "source": source, "target": target}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_reconciliation(
    source_only: bool = False,
    target_only: bool = False,
    table_filter: list[str] | None = None,
):
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    if not RECON_QUERIES_PATH.exists():
        logger.error("Recon queries not found: %s. Run validation_generator.py first.",
                      RECON_QUERIES_PATH)
        sys.exit(1)

    with open(RECON_QUERIES_PATH, "r", encoding="utf-8") as fp:
        recon_data = json.load(fp)

    tables = recon_data.get("tables", {})
    if table_filter:
        filter_set = {t.lower() for t in table_filter}
        tables = {k: v for k, v in tables.items() if k.lower() in filter_set}

    logger.info("Running reconciliation for %d tables ...", len(tables))

    # Open connections
    rs_cursor = None
    db_cursor = None

    if not target_only:
        try:
            rs_conn = get_redshift_connection()
            rs_cursor = rs_conn.cursor()
            logger.info("Connected to Redshift.")
        except Exception as exc:
            logger.error("Redshift connection failed: %s", exc)
            if not source_only:
                logger.info("Continuing with target-only checks.")
            source_only = False
            target_only = True

    if not source_only:
        try:
            db_conn = get_databricks_connection()
            db_cursor = db_conn.cursor()
            logger.info("Connected to Databricks.")
        except Exception as exc:
            logger.error("Databricks connection failed: %s", exc)
            if not target_only:
                logger.info("Continuing with source-only checks.")
            target_only = False
            source_only = True

    all_results = {}
    total_pass = 0
    total_fail = 0
    total_error = 0

    for table_key, checks in tqdm(tables.items(), desc="Tables"):
        table_results = []
        for check in tqdm(checks, desc=f"  {table_key}", leave=False):
            source_result = {}
            target_result = {}

            if rs_cursor and check.get("source_sql") and not target_only:
                source_result = execute_query(
                    rs_cursor, check["source_sql"],
                    label=f"src:{table_key}:{check['check']}"
                )

            if db_cursor and check.get("target_sql") and not source_only:
                target_result = execute_query(
                    db_cursor, check["target_sql"],
                    label=f"tgt:{table_key}:{check['check']}"
                )

            if source_only or target_only:
                comparison = {
                    "status": "PARTIAL",
                    "source": source_result if not target_only else "skipped",
                    "target": target_result if not source_only else "skipped",
                }
            else:
                comparison = compare_results(source_result, target_result, check)

            status = comparison.get("status", "UNKNOWN")
            if status == "PASS":
                total_pass += 1
            elif status == "FAIL":
                total_fail += 1
            else:
                total_error += 1

            table_results.append({
                "check": check.get("check"),
                "column": check.get("column"),
                "source_result": source_result,
                "target_result": target_result,
                "comparison": comparison,
            })

        all_results[table_key] = table_results

    # Cleanup
    if rs_cursor:
        rs_cursor.close()
    if db_cursor:
        db_cursor.close()

    # Write results
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "tables_checked": len(all_results),
            "total_checks": total_pass + total_fail + total_error,
            "passed": total_pass,
            "failed": total_fail,
            "errors": total_error,
            "pass_rate": round(total_pass / max(total_pass + total_fail, 1) * 100, 2),
        },
        "tables": all_results,
    }

    with open(RESULTS_PATH, "w", encoding="utf-8") as fp:
        json.dump(output, fp, indent=2, default=str)
    logger.info("Results written to %s", RESULTS_PATH)

    logger.info("=" * 60)
    logger.info("Reconciliation complete:")
    logger.info("  Tables   : %d", len(all_results))
    logger.info("  Passed   : %d", total_pass)
    logger.info("  Failed   : %d", total_fail)
    logger.info("  Errors   : %d", total_error)
    logger.info("  Pass rate: %.1f%%", output["summary"]["pass_rate"])
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Run source/target reconciliation checks."
    )
    parser.add_argument("--source-only", action="store_true",
                        help="Only run Redshift source queries")
    parser.add_argument("--target-only", action="store_true",
                        help="Only run Databricks target queries")
    parser.add_argument("--tables", type=str, default=None,
                        help="Comma-separated list of schema.table to check")
    args = parser.parse_args()

    table_filter = args.tables.split(",") if args.tables else None

    try:
        run_reconciliation(
            source_only=args.source_only,
            target_only=args.target_only,
            table_filter=table_filter,
        )
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
    except Exception:
        logger.exception("Unexpected error.")
        sys.exit(99)


if __name__ == "__main__":
    main()
'''


# ═══════════════════════════════════════════════════════════════════════════
# Main orchestration
# ═══════════════════════════════════════════════════════════════════════════

def run_generation(prod_db: str = TARGET_PROD_DB):
    """Generate all validation artifacts."""
    global TARGET_PROD_DB
    TARGET_PROD_DB = prod_db

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    _init_file_logging()
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

    catalog = _load_catalog()
    fk_candidates = _load_fk_candidates()

    table_columns = _build_table_columns(catalog)
    pk_columns = _build_pk_columns(catalog)
    inferred_pks = _build_inferred_pk_columns(fk_candidates)

    # Build row estimate lookup from tables array
    row_estimates = {}
    for t in catalog.get("tables", []):
        schema = (t.get("table_schema") or t.get("schema", "")).lower()
        table = (t.get("table_name") or t.get("table", "")).lower()
        row_estimates[(schema, table)] = int(t.get("row_estimate") or t.get("tbl_rows") or 0)

    all_recon_queries = {}
    ge_count = 0
    deequ_count = 0

    for (schema, table), columns in tqdm(table_columns.items(), desc="Generating validations"):
        tgt_name = _sanitise_name(schema, table)
        table_key = f"{schema}.{table}"

        # Merge declared + inferred PKs
        pks = pk_columns.get((schema, table), [])
        if not pks:
            pks = inferred_pks.get((schema, table), [])

        # --- Reconciliation SQL pairs ---
        checks = []
        rc = generate_row_count_sql(schema, table)
        checks.append(rc)

        hc = generate_hash_check_sql(schema, table, columns)
        if hc:
            checks.append(hc)

        checks.extend(generate_column_stats_sql(schema, table, columns))

        pk_check = generate_pk_uniqueness_sql(schema, table, pks)
        if pk_check:
            checks.append(pk_check)

        all_recon_queries[table_key] = checks

        # --- Great Expectations suite ---
        row_est = row_estimates.get((schema, table))
        ge_suite = generate_ge_suite(schema, table, columns, pks, row_est)
        ge_path = VALIDATION_DIR / f"ge_suite_{tgt_name}.json"
        with open(ge_path, "w", encoding="utf-8") as fp:
            json.dump(ge_suite, fp, indent=2)
        ge_count += 1

        # --- PyDeequ script ---
        deequ_script = generate_deequ_script(schema, table, columns, pks)
        deequ_path = VALIDATION_DIR / f"deequ_{tgt_name}.py"
        with open(deequ_path, "w", encoding="utf-8") as fp:
            fp.write(deequ_script)
        deequ_count += 1

    # Write recon queries JSON
    recon_output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_database": prod_db,
        "total_tables": len(all_recon_queries),
        "total_checks": sum(len(v) for v in all_recon_queries.values()),
        "tables": all_recon_queries,
    }
    with open(RECON_QUERIES_PATH, "w", encoding="utf-8") as fp:
        json.dump(recon_output, fp, indent=2, default=str)
    logger.info("Recon queries written to %s", RECON_QUERIES_PATH)

    # Write run_reconciliation.py
    runner_code = generate_reconciliation_runner()
    with open(RECON_RUNNER_PATH, "w", encoding="utf-8") as fp:
        fp.write(runner_code)
    logger.info("Reconciliation runner written to %s", RECON_RUNNER_PATH)

    # Update requirements.txt if databricks-sql-connector not present
    req_path = Path(__file__).resolve().parent / "requirements.txt"
    if req_path.exists():
        existing = req_path.read_text(encoding="utf-8")
        if "databricks-sql-connector" not in existing:
            with open(req_path, "a", encoding="utf-8") as fp:
                fp.write("databricks-sql-connector>=2.9\n")
            logger.info("Added databricks-sql-connector to requirements.txt")

    total_checks = sum(len(v) for v in all_recon_queries.values())

    logger.info("=" * 60)
    logger.info("Validation generation complete:")
    logger.info("  Tables              : %d", len(all_recon_queries))
    logger.info("  Total recon checks  : %d", total_checks)
    logger.info("  GE suites generated : %d", ge_count)
    logger.info("  Deequ scripts       : %d", deequ_count)
    logger.info("  Recon queries       : %s", RECON_QUERIES_PATH)
    logger.info("  Recon runner        : %s", RECON_RUNNER_PATH)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate data parity validation suites for Redshift→Databricks migration."
    )
    parser.add_argument(
        "--prod-db",
        default=TARGET_PROD_DB,
        help=f"Target production database (default: {TARGET_PROD_DB})",
    )
    args = parser.parse_args()

    try:
        run_generation(prod_db=args.prod_db)
    except Exception:
        logger.exception("Unexpected error during validation generation.")
        sys.exit(99)


if __name__ == "__main__":
    main()
