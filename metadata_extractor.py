"""
metadata_extractor.py
=====================
Connects to AWS Redshift and extracts a full source catalog:
  - Tables + sizes
  - Columns + datatypes
  - Constraints
  - Stored procedures & UDFs
  - Views & materialized views

Outputs:
  artifacts/source_catalog.json
  artifacts/source_summary.csv
"""

import csv
import json
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
CATALOG_PATH = ARTIFACTS_DIR / "source_catalog.json"
SUMMARY_PATH = ARTIFACTS_DIR / "source_summary.csv"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

EXCLUDED_SCHEMAS = ("pg_catalog", "information_schema", "pg_internal")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ARTIFACTS_DIR / "metadata_extractor.log", mode="w"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redshift SQL Queries
# ---------------------------------------------------------------------------

SQL_TABLE_SIZES = """
SELECT
    "schema"        AS table_schema,
    "table"         AS table_name,
    tbl_rows        AS row_estimate,
    size            AS size_mb,
    pct_used,
    diststyle,
    sortkey1,
    encoding
FROM svv_table_info
WHERE "schema" NOT IN %(excluded)s
ORDER BY size DESC;
"""

SQL_COLUMNS = """
SELECT
    table_schema,
    table_name,
    ordinal_position,
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema NOT IN %(excluded)s
ORDER BY table_schema, table_name, ordinal_position;
"""

SQL_CONSTRAINTS = """
SELECT
    tc.constraint_schema,
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name,
    ccu.table_schema  AS foreign_table_schema,
    ccu.table_name    AS foreign_table_name,
    ccu.column_name   AS foreign_column_name
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name  = kcu.constraint_name
   AND tc.constraint_schema = kcu.constraint_schema
LEFT JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name  = ccu.constraint_name
   AND tc.constraint_schema = ccu.constraint_schema
WHERE tc.constraint_schema NOT IN %(excluded)s
ORDER BY tc.constraint_schema, tc.table_name, tc.constraint_name;
"""

SQL_STORED_PROCEDURES = """
SELECT
    n.nspname           AS proc_schema,
    p.proname           AS proc_name,
    pg_get_functiondef(p.oid) AS ddl,
    p.proargnames       AS arg_names,
    p.proargtypes::text AS arg_types,
    p.prorettype::regtype::text AS return_type
FROM pg_proc_info p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE p.prokind = 'p'
  AND n.nspname NOT IN %(excluded)s
ORDER BY n.nspname, p.proname;
"""

SQL_UDFS = """
SELECT
    n.nspname           AS udf_schema,
    p.proname           AS udf_name,
    pg_get_functiondef(p.oid) AS ddl,
    p.proargnames       AS arg_names,
    p.proargtypes::text AS arg_types,
    p.prorettype::regtype::text AS return_type,
    l.lanname           AS language
FROM pg_proc_info p
JOIN pg_namespace n ON p.pronamespace = n.oid
JOIN pg_language l  ON p.prolang = l.oid
WHERE p.prokind = 'f'
  AND n.nspname NOT IN %(excluded)s
ORDER BY n.nspname, p.proname;
"""

SQL_VIEWS = """
SELECT
    table_schema,
    table_name,
    view_definition
FROM information_schema.views
WHERE table_schema NOT IN %(excluded)s
ORDER BY table_schema, table_name;
"""

SQL_MATERIALIZED_VIEWS = """
SELECT
    schema       AS mv_schema,
    name         AS mv_name,
    definition   AS view_definition
FROM stv_mv_info
ORDER BY schema, name;
"""

# Fallback for materialized views on older clusters without stv_mv_info
SQL_MATERIALIZED_VIEWS_FALLBACK = """
SELECT
    n.nspname       AS mv_schema,
    c.relname       AS mv_name,
    NULL            AS view_definition
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind = 'm'
  AND n.nspname NOT IN %(excluded)s
ORDER BY n.nspname, c.relname;
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_connection():
    """Create a Redshift connection from environment variables."""
    required_vars = [
        "REDSHIFT_HOST",
        "REDSHIFT_PORT",
        "REDSHIFT_DB",
        "REDSHIFT_USER",
        "REDSHIFT_PASSWORD",
    ]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

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


def _execute_with_retry(cursor, sql, params=None, label="query"):
    """Execute a query with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            cursor.execute(sql, params)
            return cursor.fetchall()
        except psycopg2.OperationalError as exc:
            logger.warning(
                "Attempt %d/%d for [%s] failed: %s",
                attempt,
                MAX_RETRIES,
                label,
                exc,
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY_SECONDS * attempt)
        except psycopg2.Error as exc:
            logger.error("Query [%s] failed with non-retryable error: %s", label, exc)
            raise
    return []


def _rows_to_dicts(cursor, rows):
    """Convert rows to list of dicts using cursor description."""
    if not rows or not cursor.description:
        return []
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def _safe_json(obj):
    """Make objects JSON-serialisable."""
    if isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_safe_json(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    return str(obj)


# ---------------------------------------------------------------------------
# Extraction steps
# ---------------------------------------------------------------------------

def extract_tables(cursor, excluded):
    """Extract table list with sizes."""
    logger.info("Extracting table sizes from svv_table_info ...")
    rows = _execute_with_retry(
        cursor, SQL_TABLE_SIZES, {"excluded": excluded}, "table_sizes"
    )
    results = _rows_to_dicts(cursor, rows)
    logger.info("  Found %d tables.", len(results))
    return results


def extract_columns(cursor, excluded):
    """Extract column metadata."""
    logger.info("Extracting columns from information_schema.columns ...")
    rows = _execute_with_retry(
        cursor, SQL_COLUMNS, {"excluded": excluded}, "columns"
    )
    results = _rows_to_dicts(cursor, rows)
    logger.info("  Found %d column records.", len(results))
    return results


def extract_constraints(cursor, excluded):
    """Extract constraint metadata."""
    logger.info("Extracting constraints ...")
    rows = _execute_with_retry(
        cursor, SQL_CONSTRAINTS, {"excluded": excluded}, "constraints"
    )
    results = _rows_to_dicts(cursor, rows)
    logger.info("  Found %d constraint records.", len(results))
    return results


def extract_procedures(cursor, excluded):
    """Extract stored procedure metadata."""
    logger.info("Extracting stored procedures ...")
    try:
        rows = _execute_with_retry(
            cursor, SQL_STORED_PROCEDURES, {"excluded": excluded}, "stored_procedures"
        )
        results = _rows_to_dicts(cursor, rows)
    except psycopg2.Error as exc:
        logger.warning("Could not extract procedures (may not exist): %s", exc)
        results = []
    logger.info("  Found %d stored procedures.", len(results))
    return results


def extract_udfs(cursor, excluded):
    """Extract UDF metadata."""
    logger.info("Extracting UDFs ...")
    try:
        rows = _execute_with_retry(
            cursor, SQL_UDFS, {"excluded": excluded}, "udfs"
        )
        results = _rows_to_dicts(cursor, rows)
    except psycopg2.Error as exc:
        logger.warning("Could not extract UDFs (may not exist): %s", exc)
        results = []
    logger.info("  Found %d UDFs.", len(results))
    return results


def extract_views(cursor, excluded):
    """Extract view definitions."""
    logger.info("Extracting views ...")
    rows = _execute_with_retry(
        cursor, SQL_VIEWS, {"excluded": excluded}, "views"
    )
    results = _rows_to_dicts(cursor, rows)
    logger.info("  Found %d views.", len(results))
    return results


def extract_materialized_views(cursor, excluded):
    """Extract materialized view definitions (with fallback)."""
    logger.info("Extracting materialized views ...")
    try:
        rows = _execute_with_retry(
            cursor, SQL_MATERIALIZED_VIEWS, {"excluded": excluded}, "mat_views"
        )
        results = _rows_to_dicts(cursor, rows)
    except psycopg2.Error:
        logger.info("  stv_mv_info not available; trying fallback query ...")
        try:
            rows = _execute_with_retry(
                cursor,
                SQL_MATERIALIZED_VIEWS_FALLBACK,
                {"excluded": excluded},
                "mat_views_fallback",
            )
            results = _rows_to_dicts(cursor, rows)
        except psycopg2.Error as exc:
            logger.warning("Could not extract materialized views: %s", exc)
            results = []
    logger.info("  Found %d materialized views.", len(results))
    return results


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

EXTRACTION_STEPS = [
    ("tables", extract_tables),
    ("columns", extract_columns),
    ("constraints", extract_constraints),
    ("procs", extract_procedures),
    ("udfs", extract_udfs),
    ("views", extract_views),
    ("materialized_views", extract_materialized_views),
]


def run_extraction():
    """Run all extraction steps and persist results."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Connecting to Redshift ...")
    conn = _get_connection()
    cursor = conn.cursor()
    logger.info("Connected successfully.")

    excluded = tuple(EXCLUDED_SCHEMAS)
    catalog = {}

    for label, func in tqdm(EXTRACTION_STEPS, desc="Extraction steps"):
        try:
            catalog[label] = _safe_json(func(cursor, excluded))
        except Exception:
            logger.exception("Failed extraction step: %s", label)
            catalog[label] = []

    cursor.close()
    conn.close()
    logger.info("Redshift connection closed.")

    # ---- Write JSON catalog ------------------------------------------------
    with open(CATALOG_PATH, "w", encoding="utf-8") as fp:
        json.dump(catalog, fp, indent=2, default=str)
    logger.info("Catalog written to %s", CATALOG_PATH)

    # ---- Write CSV summary -------------------------------------------------
    summary_rows = []
    for section, items in catalog.items():
        count = len(items) if isinstance(items, list) else 0
        total_size = 0.0
        total_rows = 0
        if section == "tables":
            for t in items:
                total_size += float(t.get("size_mb", 0) or 0)
                total_rows += int(t.get("row_estimate", 0) or 0)
        summary_rows.append(
            {
                "section": section,
                "object_count": count,
                "total_size_mb": round(total_size, 2),
                "total_row_estimate": total_rows,
            }
        )

    with open(SUMMARY_PATH, "w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=["section", "object_count", "total_size_mb", "total_row_estimate"],
        )
        writer.writeheader()
        writer.writerows(summary_rows)
    logger.info("Summary written to %s", SUMMARY_PATH)

    # ---- Console recap -----------------------------------------------------
    logger.info("=" * 60)
    logger.info("Extraction complete — summary:")
    for row in summary_rows:
        logger.info(
            "  %-22s %6d objects   %10.2f MB   %12d rows",
            row["section"],
            row["object_count"],
            row["total_size_mb"],
            row["total_row_estimate"],
        )
    logger.info("=" * 60)

    return catalog


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        run_extraction()
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
    except psycopg2.OperationalError as exc:
        logger.error("Could not connect to Redshift: %s", exc)
        sys.exit(2)
    except Exception:
        logger.exception("Unexpected error during extraction.")
        sys.exit(99)
