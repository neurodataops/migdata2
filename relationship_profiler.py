"""
relationship_profiler.py
========================
Infers candidate foreign-key relationships by profiling column pairs across
tables in Redshift.  Works in two modes:

  1. **Catalog mode** (default) — reads artifacts/source_catalog.json to build
     candidate pairs, then connects to Redshift to run profiling queries.
  2. **Live mode** (--live) — fetches column metadata directly from Redshift.

For every candidate pair (child_table.column → parent_table.column) where
datatypes match and naming patterns suggest a relationship (e.g. *_id columns),
the profiler computes:
  - child distinct count / non-null count
  - parent distinct count
  - anti-join count (child values missing from parent)
  - overlap ratio = 1 - (missing / child_non_null)
  - parent uniqueness test

Outputs:
  artifacts/fk_candidates.csv          — ranked candidate list
  artifacts/fk_candidates.json         — full detail with SQL snippets
  artifacts/fk_validation_queries.sql  — ready-to-run Redshift SQL for each candidate
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from itertools import product
from pathlib import Path

import psycopg2
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
CATALOG_PATH = ARTIFACTS_DIR / "source_catalog.json"
FK_CANDIDATES_CSV = ARTIFACTS_DIR / "fk_candidates.csv"
FK_CANDIDATES_JSON = ARTIFACTS_DIR / "fk_candidates.json"
FK_VALIDATION_SQL = ARTIFACTS_DIR / "fk_validation_queries.sql"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# Thresholds
OVERLAP_HIGHLY_LIKELY = 0.999
MAX_CANDIDATES_PER_TABLE = 500  # safety cap to avoid combinatorial explosion

EXCLUDED_SCHEMAS = ("pg_catalog", "information_schema", "pg_internal")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ARTIFACTS_DIR / "relationship_profiler.log", mode="w"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def _get_connection():
    """Create a Redshift connection from environment variables."""
    required_vars = [
        "REDSHIFT_HOST", "REDSHIFT_PORT", "REDSHIFT_DB",
        "REDSHIFT_USER", "REDSHIFT_PASSWORD",
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
                attempt, MAX_RETRIES, label, exc,
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY_SECONDS * attempt)
        except psycopg2.Error as exc:
            logger.error("Query [%s] failed: %s", label, exc)
            raise
    return []


def _rows_to_dicts(cursor, rows):
    if not rows or not cursor.description:
        return []
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


# ---------------------------------------------------------------------------
# Redshift SQL for live column fetch
# ---------------------------------------------------------------------------

SQL_COLUMNS_LIVE = """
SELECT
    table_schema,
    table_name,
    column_name,
    data_type,
    ordinal_position,
    is_nullable
FROM information_schema.columns
WHERE table_schema NOT IN %(excluded)s
ORDER BY table_schema, table_name, ordinal_position;
"""

SQL_CONSTRAINTS_LIVE = """
SELECT
    tc.constraint_schema,
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name  = kcu.constraint_name
   AND tc.constraint_schema = kcu.constraint_schema
WHERE tc.constraint_schema NOT IN %(excluded)s
  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
ORDER BY tc.constraint_schema, tc.table_name;
"""

# ---------------------------------------------------------------------------
# Profiling SQL templates (safe — identifiers are quoted)
# ---------------------------------------------------------------------------

SQL_PROFILE_CANDIDATE = """
SELECT
    (SELECT COUNT(*)
     FROM {child_schema}.{child_table}
     WHERE {child_col} IS NOT NULL
    ) AS child_non_null,

    (SELECT COUNT(DISTINCT {child_col})
     FROM {child_schema}.{child_table}
     WHERE {child_col} IS NOT NULL
    ) AS child_distinct,

    (SELECT COUNT(*)
     FROM {parent_schema}.{parent_table}
    ) AS parent_count,

    (SELECT COUNT(DISTINCT {parent_col})
     FROM {parent_schema}.{parent_table}
    ) AS parent_distinct,

    (SELECT COUNT(*)
     FROM {child_schema}.{child_table} c
     LEFT JOIN {parent_schema}.{parent_table} p
        ON c.{child_col} = p.{parent_col}
     WHERE p.{parent_col} IS NULL
       AND c.{child_col} IS NOT NULL
    ) AS missing_in_parent;
"""

SQL_PARENT_UNIQUE_CHECK = """
SELECT
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT {parent_col})
        THEN 1 ELSE 0
    END AS is_unique
FROM {parent_schema}.{parent_table}
WHERE {parent_col} IS NOT NULL;
"""

# ---------------------------------------------------------------------------
# Datatype compatibility
# ---------------------------------------------------------------------------

# Broad type families for matching child→parent columns
_TYPE_FAMILIES = {
    "integer":   {"integer", "bigint", "smallint", "int", "int2", "int4", "int8"},
    "bigint":    {"integer", "bigint", "smallint", "int", "int2", "int4", "int8"},
    "smallint":  {"integer", "bigint", "smallint", "int", "int2", "int4", "int8"},
    "numeric":   {"numeric", "decimal", "float", "double precision", "real",
                  "float4", "float8"},
    "decimal":   {"numeric", "decimal"},
    "character varying": {"character varying", "character", "text", "varchar", "char",
                          "nvarchar", "bpchar"},
    "text":      {"character varying", "character", "text", "varchar", "char",
                  "nvarchar", "bpchar"},
    "varchar":   {"character varying", "character", "text", "varchar", "char",
                  "nvarchar", "bpchar"},
    "uuid":      {"uuid", "character varying", "varchar", "char"},
    "date":      {"date", "timestamp without time zone", "timestamp with time zone"},
    "timestamp without time zone": {"date", "timestamp without time zone",
                                     "timestamp with time zone"},
}


def _types_compatible(dtype_a: str, dtype_b: str) -> bool:
    """Check if two Redshift datatypes are compatible for FK matching."""
    a = dtype_a.lower().strip()
    b = dtype_b.lower().strip()
    if a == b:
        return True
    family = _TYPE_FAMILIES.get(a, {a})
    return b in family


# ---------------------------------------------------------------------------
# Naming-pattern heuristics
# ---------------------------------------------------------------------------

# Patterns that suggest a column is a foreign key reference
_RE_FK_SUFFIX = re.compile(
    r"^(.+?)(_id|_key|_code|_ref|_fk|id|_uuid|_pk)$", re.IGNORECASE
)


def _is_id_column(col_name: str) -> bool:
    """Return True if column name looks like a reference/FK column."""
    return bool(_RE_FK_SUFFIX.match(col_name))


def _derive_parent_name(child_col: str) -> str | None:
    """
    Guess the parent table name from a child column name.
    e.g. 'customer_id' → 'customer', 'order_ref' → 'order'
    """
    m = _RE_FK_SUFFIX.match(child_col)
    if m:
        return m.group(1).lower()
    return None


# ---------------------------------------------------------------------------
# Candidate pair generation
# ---------------------------------------------------------------------------

def load_catalog():
    """Load the source catalog JSON produced by metadata_extractor.py."""
    if not CATALOG_PATH.exists():
        raise FileNotFoundError(
            f"Catalog not found at {CATALOG_PATH}. "
            "Run metadata_extractor.py first or use --live mode."
        )
    with open(CATALOG_PATH, "r", encoding="utf-8") as fp:
        return json.load(fp)


def _build_column_index(columns: list[dict]) -> dict:
    """
    Build lookup structures:
      - by_table: {(schema, table): [col_dict, ...]}
      - by_name:  {col_name_lower: [(schema, table, col_dict), ...]}
    """
    by_table = {}
    by_name = {}
    for c in columns:
        schema = c.get("table_schema", "").lower()
        table = c.get("table_name", "").lower()
        col = c.get("column_name", "").lower()
        dtype = c.get("data_type", "").lower()

        key = (schema, table)
        entry = {"column_name": col, "data_type": dtype, "schema": schema, "table": table}
        by_table.setdefault(key, []).append(entry)
        by_name.setdefault(col, []).append(entry)

    return by_table, by_name


def _build_constraint_set(constraints: list[dict]) -> set:
    """
    Build a set of (schema, table, column) tuples that already have
    declared PK/FK/UNIQUE constraints.
    """
    declared = set()
    for c in constraints:
        schema = (c.get("constraint_schema") or "").lower()
        table = (c.get("table_name") or "").lower()
        col = (c.get("column_name") or "").lower()
        declared.add((schema, table, col))
    return declared


def generate_candidates(columns, constraints, tables=None):
    """
    Generate candidate FK pairs using naming patterns and type compatibility.

    Strategy:
      1. For each *_id / *_key / *_ref column (the "child"), derive the likely
         parent table name from the column name.
      2. Look for a table whose name matches the derived parent name.
      3. In that parent table, look for a column with the same name, or named
         'id', or a PK column.
      4. Verify datatype compatibility.
      5. Skip pairs that already have declared FK constraints.
    """
    by_table, by_name = _build_column_index(columns)
    declared = _build_constraint_set(constraints)

    # Build table name → (schema, table) lookup
    table_lookup = {}
    for schema, table in by_table:
        table_lookup.setdefault(table, []).append(schema)

    # Also build a set of known PK/unique columns per table
    pk_columns = {}
    for c in constraints:
        ctype = (c.get("constraint_type") or "").upper()
        if ctype in ("PRIMARY KEY", "UNIQUE"):
            schema = (c.get("constraint_schema") or "").lower()
            table = (c.get("table_name") or "").lower()
            col = (c.get("column_name") or "").lower()
            pk_columns.setdefault((schema, table), set()).add(col)

    candidates = []
    seen = set()

    for (child_schema, child_table), child_cols in by_table.items():
        for child_col_info in child_cols:
            child_col = child_col_info["column_name"]
            child_dtype = child_col_info["data_type"]

            if not _is_id_column(child_col):
                continue

            # Skip if already declared as FK
            if (child_schema, child_table, child_col) in declared:
                continue

            parent_name_guess = _derive_parent_name(child_col)
            if not parent_name_guess:
                continue

            # Find matching parent tables
            # Try exact match, then plural/singular variants
            parent_table_candidates = set()
            for variant in (
                parent_name_guess,
                parent_name_guess + "s",
                parent_name_guess + "es",
                parent_name_guess.rstrip("s"),
            ):
                if variant in table_lookup:
                    for pschema in table_lookup[variant]:
                        parent_table_candidates.add((pschema, variant))

            for parent_schema, parent_table in parent_table_candidates:
                # Don't self-reference unless column name differs
                if (child_schema, child_table) == (parent_schema, parent_table):
                    continue

                parent_cols = by_table.get((parent_schema, parent_table), [])

                # Determine the best parent column to match against
                parent_col_match = None
                for pc in parent_cols:
                    pcol = pc["column_name"]
                    pdtype = pc["data_type"]

                    if not _types_compatible(child_dtype, pdtype):
                        continue

                    # Priority: same name > 'id' > PK column
                    if pcol == child_col:
                        parent_col_match = pc
                        break
                    if pcol == "id" and parent_col_match is None:
                        parent_col_match = pc
                    pk_set = pk_columns.get((parent_schema, parent_table), set())
                    if pcol in pk_set and parent_col_match is None:
                        parent_col_match = pc

                if parent_col_match is None:
                    continue

                pair_key = (
                    child_schema, child_table, child_col,
                    parent_schema, parent_table, parent_col_match["column_name"],
                )
                if pair_key in seen:
                    continue
                seen.add(pair_key)

                candidates.append({
                    "child_schema": child_schema,
                    "child_table": child_table,
                    "child_col": child_col,
                    "child_dtype": child_dtype,
                    "parent_schema": parent_schema,
                    "parent_table": parent_table,
                    "parent_col": parent_col_match["column_name"],
                    "parent_dtype": parent_col_match["data_type"],
                })

    logger.info("Generated %d candidate FK pairs from naming patterns.", len(candidates))
    return candidates


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------

def _quote_ident(name: str) -> str:
    """Quote a SQL identifier to prevent injection."""
    return '"' + name.replace('"', '""') + '"'


def _build_profile_sql(candidate: dict) -> str:
    """Build the profiling SQL for a single candidate pair."""
    return SQL_PROFILE_CANDIDATE.format(
        child_schema=_quote_ident(candidate["child_schema"]),
        child_table=_quote_ident(candidate["child_table"]),
        child_col=_quote_ident(candidate["child_col"]),
        parent_schema=_quote_ident(candidate["parent_schema"]),
        parent_table=_quote_ident(candidate["parent_table"]),
        parent_col=_quote_ident(candidate["parent_col"]),
    )


def _build_unique_check_sql(candidate: dict) -> str:
    """Build the uniqueness check SQL for the parent column."""
    return SQL_PARENT_UNIQUE_CHECK.format(
        parent_schema=_quote_ident(candidate["parent_schema"]),
        parent_table=_quote_ident(candidate["parent_table"]),
        parent_col=_quote_ident(candidate["parent_col"]),
    )


def _build_validation_sql_snippet(candidate: dict) -> str:
    """
    Build a human-readable validation SQL snippet for documentation.
    Uses unquoted identifiers for readability.
    """
    cs = candidate["child_schema"]
    ct = candidate["child_table"]
    cc = candidate["child_col"]
    ps = candidate["parent_schema"]
    pt = candidate["parent_table"]
    pc = candidate["parent_col"]

    return f"""-- Validation query for: {cs}.{ct}.{cc} → {ps}.{pt}.{pc}
SELECT
    (SELECT COUNT(*)
     FROM {cs}.{ct}
     WHERE {cc} IS NOT NULL
    ) AS child_non_null,

    (SELECT COUNT(*)
     FROM {ps}.{pt}
    ) AS parent_count,

    (SELECT COUNT(*)
     FROM {cs}.{ct} c
     LEFT JOIN {ps}.{pt} p
        ON c.{cc} = p.{pc}
     WHERE p.{pc} IS NULL
       AND c.{cc} IS NOT NULL
    ) AS missing_in_parent;
"""


def profile_candidate(cursor, candidate: dict) -> dict:
    """
    Run profiling queries for a single candidate and return enriched result.
    """
    label = (
        f"{candidate['child_schema']}.{candidate['child_table']}.{candidate['child_col']}"
        f" → {candidate['parent_schema']}.{candidate['parent_table']}.{candidate['parent_col']}"
    )

    result = {**candidate}

    # Profile query
    try:
        sql = _build_profile_sql(candidate)
        rows = _execute_with_retry(cursor, sql, label=f"profile:{label}")
        if rows and rows[0]:
            row = rows[0]
            child_non_null = int(row[0] or 0)
            child_distinct = int(row[1] or 0)
            parent_count = int(row[2] or 0)
            parent_distinct = int(row[3] or 0)
            missing = int(row[4] or 0)

            overlap = 0.0
            if child_non_null > 0:
                overlap = 1.0 - (missing / child_non_null)

            result.update({
                "child_non_null": child_non_null,
                "child_distinct": child_distinct,
                "parent_count": parent_count,
                "parent_distinct": parent_distinct,
                "missing_count": missing,
                "overlap_ratio": round(overlap, 6),
            })
        else:
            result.update({
                "child_non_null": 0, "child_distinct": 0,
                "parent_count": 0, "parent_distinct": 0,
                "missing_count": 0, "overlap_ratio": 0.0,
                "error": "empty_result",
            })
    except psycopg2.Error as exc:
        logger.warning("Profile query failed for %s: %s", label, exc)
        result.update({
            "child_non_null": 0, "child_distinct": 0,
            "parent_count": 0, "parent_distinct": 0,
            "missing_count": 0, "overlap_ratio": 0.0,
            "error": str(exc),
        })

    # Uniqueness check
    try:
        sql = _build_unique_check_sql(candidate)
        rows = _execute_with_retry(cursor, sql, label=f"unique:{label}")
        result["parent_unique_bool"] = bool(rows and rows[0] and rows[0][0] == 1)
    except psycopg2.Error as exc:
        logger.warning("Uniqueness check failed for %s: %s", label, exc)
        result["parent_unique_bool"] = False

    # Classification
    overlap = result.get("overlap_ratio", 0.0)
    unique = result.get("parent_unique_bool", False)

    if overlap >= OVERLAP_HIGHLY_LIKELY and unique:
        result["classification"] = "highly_likely"
    elif overlap >= 0.95 and unique:
        result["classification"] = "likely"
    elif overlap >= 0.80:
        result["classification"] = "possible"
    else:
        result["classification"] = "unlikely"

    # Attach validation SQL snippet
    result["validation_sql"] = _build_validation_sql_snippet(candidate)

    return result


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

CSV_FIELDNAMES = [
    "child_schema", "child_table", "child_col",
    "parent_schema", "parent_table", "parent_col",
    "child_non_null", "child_distinct",
    "parent_count", "parent_distinct",
    "missing_count", "overlap_ratio", "parent_unique_bool",
    "classification",
]

DISCLAIMER = (
    "NOTE: All candidates marked 'highly_likely' are SUGGESTIONS based on "
    "statistical profiling only. Human validation is REQUIRED before creating "
    "declared foreign key constraints in the target Databricks environment. "
    "Verify business semantics, check for soft-delete patterns, and confirm "
    "referential integrity expectations with data owners."
)


def write_csv(results):
    """Write fk_candidates.csv."""
    with open(FK_CANDIDATES_CSV, "w", newline="", encoding="utf-8") as fp:
        # Write disclaimer as comment header
        fp.write(f"# {DISCLAIMER}\n")
        writer = csv.DictWriter(fp, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)
    logger.info("FK candidates CSV written to %s (%d rows)", FK_CANDIDATES_CSV, len(results))


def write_json(results):
    """Write fk_candidates.json with full detail."""
    payload = {
        "disclaimer": DISCLAIMER,
        "total_candidates": len(results),
        "highly_likely": sum(1 for r in results if r.get("classification") == "highly_likely"),
        "likely": sum(1 for r in results if r.get("classification") == "likely"),
        "possible": sum(1 for r in results if r.get("classification") == "possible"),
        "unlikely": sum(1 for r in results if r.get("classification") == "unlikely"),
        "candidates": results,
    }
    with open(FK_CANDIDATES_JSON, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2, default=str)
    logger.info("FK candidates JSON written to %s", FK_CANDIDATES_JSON)


def write_validation_sql(results):
    """Write a .sql file with one validation query per candidate."""
    with open(FK_VALIDATION_SQL, "w", encoding="utf-8") as fp:
        fp.write(f"-- {DISCLAIMER}\n")
        fp.write(f"-- Generated validation queries: {len(results)}\n\n")
        for r in results:
            if r.get("classification") in ("highly_likely", "likely", "possible"):
                fp.write(r.get("validation_sql", ""))
                fp.write("\n")
    logger.info("Validation SQL written to %s", FK_VALIDATION_SQL)


# ---------------------------------------------------------------------------
# Live column fetch
# ---------------------------------------------------------------------------

def fetch_columns_live(cursor):
    """Fetch column metadata directly from Redshift."""
    logger.info("Fetching columns from Redshift (live mode) ...")
    rows = _execute_with_retry(
        cursor, SQL_COLUMNS_LIVE, {"excluded": tuple(EXCLUDED_SCHEMAS)}, "columns_live"
    )
    return _rows_to_dicts(cursor, rows)


def fetch_constraints_live(cursor):
    """Fetch constraint metadata directly from Redshift."""
    logger.info("Fetching constraints from Redshift (live mode) ...")
    rows = _execute_with_retry(
        cursor, SQL_CONSTRAINTS_LIVE, {"excluded": tuple(EXCLUDED_SCHEMAS)}, "constraints_live"
    )
    return _rows_to_dicts(cursor, rows)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_profiling(live: bool = False, max_candidates: int = MAX_CANDIDATES_PER_TABLE):
    """Run the full relationship profiling pipeline."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Load or fetch metadata -------------------------------------------
    conn = _get_connection()
    cursor = conn.cursor()

    if live:
        logger.info("Running in LIVE mode — fetching metadata from Redshift ...")
        columns = fetch_columns_live(cursor)
        constraints = fetch_constraints_live(cursor)
    else:
        logger.info("Loading catalog from %s ...", CATALOG_PATH)
        catalog = load_catalog()
        columns = catalog.get("columns", [])
        constraints = catalog.get("constraints", [])
        logger.info(
            "  Loaded %d columns, %d constraints from catalog.",
            len(columns), len(constraints),
        )

    # --- Generate candidate pairs -----------------------------------------
    candidates = generate_candidates(columns, constraints)

    if len(candidates) > max_candidates:
        logger.warning(
            "Candidate count (%d) exceeds cap (%d). Truncating to cap. "
            "Use --max-candidates to raise the limit.",
            len(candidates), max_candidates,
        )
        candidates = candidates[:max_candidates]

    if not candidates:
        logger.info("No FK candidates found. Exiting.")
        cursor.close()
        conn.close()
        write_csv([])
        write_json([])
        return

    # --- Profile each candidate -------------------------------------------
    logger.info("Profiling %d candidates against Redshift ...", len(candidates))
    results = []
    for cand in tqdm(candidates, desc="Profiling FK candidates"):
        result = profile_candidate(cursor, cand)
        results.append(result)

    cursor.close()
    conn.close()
    logger.info("Redshift connection closed.")

    # --- Sort: highly_likely first, then by overlap desc ------------------
    class_order = {"highly_likely": 0, "likely": 1, "possible": 2, "unlikely": 3}
    results.sort(key=lambda r: (
        class_order.get(r.get("classification", "unlikely"), 9),
        -r.get("overlap_ratio", 0),
    ))

    # --- Write outputs ----------------------------------------------------
    write_csv(results)
    write_json(results)
    write_validation_sql(results)

    # --- Recap ------------------------------------------------------------
    counts = {}
    for r in results:
        c = r.get("classification", "unknown")
        counts[c] = counts.get(c, 0) + 1

    logger.info("=" * 60)
    logger.info("Relationship profiling complete:")
    logger.info("  Total candidates profiled: %d", len(results))
    for cls in ("highly_likely", "likely", "possible", "unlikely"):
        logger.info("    %-16s: %d", cls, counts.get(cls, 0))
    logger.info("")
    logger.info("  %s", DISCLAIMER)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Profile Redshift tables to infer candidate foreign-key relationships."
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help="Fetch column metadata live from Redshift instead of reading source_catalog.json",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=MAX_CANDIDATES_PER_TABLE,
        help=f"Maximum number of candidates to profile (default: {MAX_CANDIDATES_PER_TABLE})",
    )
    args = parser.parse_args()

    try:
        run_profiling(live=args.live, max_candidates=args.max_candidates)
    except FileNotFoundError as exc:
        logger.error("File error: %s", exc)
        sys.exit(1)
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
    except psycopg2.OperationalError as exc:
        logger.error("Could not connect to Redshift: %s", exc)
        sys.exit(2)
    except Exception:
        logger.exception("Unexpected error during relationship profiling.")
        sys.exit(99)


if __name__ == "__main__":
    main()
