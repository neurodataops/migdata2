"""
ddl_deployer.py
===============
Reads transpiled SQL from artifacts/transpiled/, source catalog, and FK
candidates to produce deployment-ready DDL for Databricks Delta Lake.

Handles:
  - Table and view DDL from transpiled output
  - Declared constraints from source catalog (PK/UNIQUE as metadata comments,
    FK as structural comments + optional async validation job)
  - High-likelihood inferred FKs from fk_candidates.csv (marked MANUAL)
  - Generates deploy.sh for Databricks SQL execution
  - Produces manual_tasks.md checklist for human review
  - Explicitly flags what is NOT auto-created

Outputs:
  artifacts/deploy/                     — all deployment DDL files
  artifacts/deploy/00_databases.sql     — CREATE DATABASE statements
  artifacts/deploy/01_tables.sql        — CREATE TABLE statements
  artifacts/deploy/02_views.sql         — CREATE VIEW statements
  artifacts/deploy/03_constraints.sql   — constraint comments + ALTER TABLE stubs
  artifacts/deploy/04_validation_job.sql— async FK validation notebook SQL
  artifacts/deploy_manifest.json        — full inventory of what will be deployed
  artifacts/manual_tasks.md             — checklist for human review
  deploy.sh                             — shell script to execute DDL
"""

import argparse
import csv
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
TRANSPILED_DIR = ARTIFACTS_DIR / "transpiled"
CATALOG_PATH = ARTIFACTS_DIR / "source_catalog.json"
FK_CANDIDATES_PATH = ARTIFACTS_DIR / "fk_candidates.csv"
CONVERT_REPORT_PATH = ARTIFACTS_DIR / "convert_report.json"
DEPLOY_DIR = ARTIFACTS_DIR / "deploy"
DEPLOY_MANIFEST_PATH = ARTIFACTS_DIR / "deploy_manifest.json"
MANUAL_TASKS_PATH = ARTIFACTS_DIR / "manual_tasks.md"
DEPLOY_SCRIPT_PATH = Path(__file__).resolve().parent / "deploy.sh"

TARGET_STG_DB = "stg"
TARGET_PROD_DB = "prod"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ARTIFACTS_DIR / "ddl_deployer.log", mode="w"),
    ],
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Loaders
# ═══════════════════════════════════════════════════════════════════════════

def _load_json(path: Path) -> dict | list | None:
    if not path.exists():
        logger.warning("File not found: %s", path)
        return None
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def _load_catalog() -> dict:
    catalog = _load_json(CATALOG_PATH)
    if not catalog:
        logger.warning("No source catalog — constraint generation will be limited.")
        return {}
    return catalog


def _load_convert_report() -> dict:
    report = _load_json(CONVERT_REPORT_PATH)
    if not report:
        return {"objects": []}
    return report


def _load_transpiled_files() -> dict[str, str]:
    """Load all transpiled SQL files. Returns {filename: content}."""
    files = {}
    if not TRANSPILED_DIR.exists():
        logger.warning("Transpiled directory not found: %s", TRANSPILED_DIR)
        return files
    for f in sorted(TRANSPILED_DIR.glob("*.sql")):
        files[f.name] = f.read_text(encoding="utf-8", errors="replace")
    logger.info("Loaded %d transpiled SQL files.", len(files))
    return files


def _load_fk_candidates() -> list[dict]:
    """Load FK candidates from CSV."""
    if not FK_CANDIDATES_PATH.exists():
        logger.warning("FK candidates file not found: %s", FK_CANDIDATES_PATH)
        return []
    candidates = []
    with open(FK_CANDIDATES_PATH, "r", encoding="utf-8") as fp:
        # Skip comment lines
        lines = [line for line in fp if not line.startswith("#")]
    reader = csv.DictReader(lines)
    for row in reader:
        candidates.append(row)
    logger.info("Loaded %d FK candidates.", len(candidates))
    return candidates


# ═══════════════════════════════════════════════════════════════════════════
# Classification helpers
# ═══════════════════════════════════════════════════════════════════════════

_RE_CREATE_TABLE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
    re.IGNORECASE,
)
_RE_CREATE_VIEW = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
    re.IGNORECASE,
)
_RE_CLASSIFICATION = re.compile(r"--\s*Classification:\s*(\S+)")
_RE_MANUAL_REWRITE = re.compile(r"--\s*MANUAL REWRITE:")


def _classify_transpiled(filename: str, content: str) -> dict:
    """Classify a transpiled file as table DDL, view DDL, or other."""
    classification_match = _RE_CLASSIFICATION.search(content)
    classification = classification_match.group(1) if classification_match else "UNKNOWN"
    is_manual = bool(_RE_MANUAL_REWRITE.search(content))

    table_match = _RE_CREATE_TABLE.search(content)
    view_match = _RE_CREATE_VIEW.search(content)

    obj_type = "unknown"
    obj_name = filename
    if view_match:
        obj_type = "view"
        obj_name = view_match.group(1)
    elif table_match:
        obj_type = "table"
        obj_name = table_match.group(1)

    return {
        "filename": filename,
        "object_type": obj_type,
        "object_name": obj_name,
        "classification": classification,
        "is_manual_rewrite": is_manual,
        "content": content,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Constraint DDL generation
# ═══════════════════════════════════════════════════════════════════════════

def _sanitise_name(schema: str, table: str) -> str:
    s = re.sub(r"[^a-z0-9_]", "_", schema.lower())
    t = re.sub(r"[^a-z0-9_]", "_", table.lower())
    return f"{s}_{t}"


def generate_declared_constraints_ddl(catalog: dict) -> tuple[list[str], list[dict]]:
    """
    Generate DDL for declared constraints from the source catalog.

    Delta Lake limitations:
      - PRIMARY KEY / UNIQUE: informational only (not enforced) in Unity Catalog
      - FOREIGN KEY: informational only in Unity Catalog; not supported in
        Hive metastore mode
      - CHECK: not supported

    Strategy:
      - Emit PK/UNIQUE as ALTER TABLE ... ADD CONSTRAINT (Unity Catalog mode)
      - Emit FK as metadata comments + optional validation query
      - Return list of DDL statements and manual task entries
    """
    constraints = catalog.get("constraints", [])
    if not constraints:
        return [], []

    ddl_lines = []
    manual_tasks = []

    # Group constraints by (schema, table, constraint_name)
    grouped = defaultdict(list)
    for c in constraints:
        key = (
            (c.get("constraint_schema") or "").lower(),
            (c.get("table_name") or "").lower(),
            c.get("constraint_name", ""),
            (c.get("constraint_type") or "").upper(),
        )
        grouped[key].append(c)

    ddl_lines.append("-- ============================================================")
    ddl_lines.append("-- Declared constraints from source Redshift catalog")
    ddl_lines.append("-- Delta Lake: PK/UNIQUE are informational in Unity Catalog.")
    ddl_lines.append("-- FK constraints are NOT enforced — included as metadata.")
    ddl_lines.append("-- ============================================================")
    ddl_lines.append("")

    for (schema, table, cname, ctype), cols in sorted(grouped.items()):
        target_table = f"{TARGET_PROD_DB}.{_sanitise_name(schema, table)}"
        col_names = [c.get("column_name", "") for c in cols if c.get("column_name")]
        col_list = ", ".join(f"`{c}`" for c in col_names)

        if ctype == "PRIMARY KEY":
            ddl_lines.append(f"-- Source PK: {schema}.{table} ({', '.join(col_names)})")
            ddl_lines.append(
                f"ALTER TABLE {target_table} ADD CONSTRAINT pk_{_sanitise_name(schema, table)} "
                f"PRIMARY KEY ({col_list}) NOT ENFORCED;"
            )
            ddl_lines.append("")

        elif ctype == "UNIQUE":
            ddl_lines.append(f"-- Source UNIQUE: {schema}.{table} ({', '.join(col_names)})")
            ddl_lines.append(
                f"ALTER TABLE {target_table} ADD CONSTRAINT uq_{_sanitise_name(schema, table)}_{col_names[0] if col_names else 'col'} "
                f"UNIQUE ({col_list}) NOT ENFORCED;"
            )
            ddl_lines.append("")

        elif ctype == "FOREIGN KEY":
            # Extract referenced table info
            ref_schema = cols[0].get("foreign_table_schema", "") if cols else ""
            ref_table = cols[0].get("foreign_table_name", "") if cols else ""
            ref_col = cols[0].get("foreign_column_name", "") if cols else ""
            ref_target = f"{TARGET_PROD_DB}.{_sanitise_name(ref_schema, ref_table)}"

            ddl_lines.append(f"-- Source FK: {schema}.{table}({', '.join(col_names)}) -> "
                             f"{ref_schema}.{ref_table}({ref_col})")
            ddl_lines.append(f"-- Delta Lake does NOT enforce FK constraints.")
            ddl_lines.append(f"-- The following is informational metadata (Unity Catalog):")
            ddl_lines.append(
                f"ALTER TABLE {target_table} ADD CONSTRAINT fk_{cname or _sanitise_name(schema, table)} "
                f"FOREIGN KEY ({col_list}) REFERENCES {ref_target}(`{ref_col}`) NOT ENFORCED;"
            )
            ddl_lines.append("")

            manual_tasks.append({
                "type": "declared_fk",
                "source": f"{schema}.{table}({', '.join(col_names)})",
                "target": f"{ref_schema}.{ref_table}({ref_col})",
                "action": "Verify FK relationship holds in migrated data. "
                          "Run validation job to check referential integrity.",
                "ddl": ddl_lines[-3],
            })

        else:
            ddl_lines.append(f"-- Unsupported constraint type '{ctype}' on {schema}.{table}: {cname}")
            ddl_lines.append(f"-- Columns: {', '.join(col_names)}")
            ddl_lines.append("")

    return ddl_lines, manual_tasks


def generate_inferred_fk_ddl(fk_candidates: list[dict]) -> tuple[list[str], list[dict]]:
    """
    Generate ALTER TABLE ... ADD CONSTRAINT for high-likelihood inferred FKs.
    ALL are marked with -- MANUAL CONFIRMATION REQUIRED.
    """
    ddl_lines = []
    manual_tasks = []

    high_candidates = [
        c for c in fk_candidates
        if c.get("classification") in ("highly_likely", "likely")
    ]

    if not high_candidates:
        return ddl_lines, manual_tasks

    ddl_lines.append("")
    ddl_lines.append("-- ============================================================")
    ddl_lines.append("-- Inferred FK relationships (from relationship_profiler.py)")
    ddl_lines.append("-- ALL require manual confirmation before enabling.")
    ddl_lines.append("-- ============================================================")
    ddl_lines.append("")

    for i, c in enumerate(high_candidates, 1):
        cs = c.get("child_schema", "public")
        ct = c.get("child_table", "")
        cc = c.get("child_col", "")
        ps = c.get("parent_schema", "public")
        pt = c.get("parent_table", "")
        pc = c.get("parent_col", "")
        overlap = c.get("overlap_ratio", "")
        classification = c.get("classification", "")
        parent_unique = c.get("parent_unique_bool", "")

        child_target = f"{TARGET_PROD_DB}.{_sanitise_name(cs, ct)}"
        parent_target = f"{TARGET_PROD_DB}.{_sanitise_name(ps, pt)}"
        constraint_name = f"fk_inferred_{_sanitise_name(cs, ct)}_{cc}"

        ddl_lines.append(f"-- [{i}] Inferred FK: {cs}.{ct}.{cc} -> {ps}.{pt}.{pc}")
        ddl_lines.append(f"--   Classification: {classification}")
        ddl_lines.append(f"--   Overlap ratio:  {overlap}")
        ddl_lines.append(f"--   Parent unique:  {parent_unique}")
        ddl_lines.append(f"-- MANUAL CONFIRMATION REQUIRED — do not enable without data owner approval.")
        ddl_lines.append(
            f"ALTER TABLE {child_target} ADD CONSTRAINT {constraint_name} "
            f"FOREIGN KEY (`{cc}`) REFERENCES {parent_target}(`{pc}`) NOT ENFORCED;"
        )
        ddl_lines.append("")

        manual_tasks.append({
            "type": "inferred_fk",
            "classification": classification,
            "source": f"{cs}.{ct}.{cc} -> {ps}.{pt}.{pc}",
            "overlap_ratio": overlap,
            "parent_unique": parent_unique,
            "action": (
                f"Confirm that {cs}.{ct}.{cc} is a true foreign key to "
                f"{ps}.{pt}.{pc}. Check business semantics, soft-delete "
                f"patterns, and data quality. Only then uncomment/enable "
                f"the constraint."
            ),
        })

    return ddl_lines, manual_tasks


def generate_validation_job_sql(catalog: dict, fk_candidates: list[dict]) -> list[str]:
    """
    Generate SQL for an async validation job that checks referential integrity
    for all declared and inferred FK relationships.
    """
    lines = []
    lines.append("-- ============================================================")
    lines.append("-- Asynchronous FK Validation Job")
    lines.append("-- Run this as a scheduled Databricks SQL query or notebook")
    lines.append("-- to continuously verify referential integrity.")
    lines.append("-- ============================================================")
    lines.append("")

    # Declared FKs
    constraints = catalog.get("constraints", [])
    fk_constraints = [c for c in constraints if (c.get("constraint_type") or "").upper() == "FOREIGN KEY"]

    seen = set()
    for c in fk_constraints:
        cs = (c.get("constraint_schema") or "").lower()
        ct = (c.get("table_name") or "").lower()
        cc = (c.get("column_name") or "").lower()
        ps = (c.get("foreign_table_schema") or "").lower()
        pt = (c.get("foreign_table_name") or "").lower()
        pc = (c.get("foreign_column_name") or "").lower()

        key = (cs, ct, cc, ps, pt, pc)
        if key in seen or not all(key):
            continue
        seen.add(key)

        child_t = f"{TARGET_PROD_DB}.{_sanitise_name(cs, ct)}"
        parent_t = f"{TARGET_PROD_DB}.{_sanitise_name(ps, pt)}"

        lines.append(f"-- Declared FK: {cs}.{ct}.{cc} -> {ps}.{pt}.{pc}")
        lines.append(f"SELECT")
        lines.append(f"  '{child_t}' AS child_table,")
        lines.append(f"  '{cc}' AS child_column,")
        lines.append(f"  '{parent_t}' AS parent_table,")
        lines.append(f"  '{pc}' AS parent_column,")
        lines.append(f"  'declared' AS fk_source,")
        lines.append(f"  COUNT(*) AS orphan_count")
        lines.append(f"FROM {child_t} c")
        lines.append(f"LEFT JOIN {parent_t} p ON c.`{cc}` = p.`{pc}`")
        lines.append(f"WHERE p.`{pc}` IS NULL AND c.`{cc}` IS NOT NULL;")
        lines.append("")

    # Inferred FKs
    high_candidates = [
        c for c in fk_candidates
        if c.get("classification") in ("highly_likely", "likely")
    ]
    for c in high_candidates:
        cs = c.get("child_schema", "")
        ct = c.get("child_table", "")
        cc = c.get("child_col", "")
        ps = c.get("parent_schema", "")
        pt = c.get("parent_table", "")
        pc = c.get("parent_col", "")

        key = (cs, ct, cc, ps, pt, pc)
        if key in seen or not all(key):
            continue
        seen.add(key)

        child_t = f"{TARGET_PROD_DB}.{_sanitise_name(cs, ct)}"
        parent_t = f"{TARGET_PROD_DB}.{_sanitise_name(ps, pt)}"

        lines.append(f"-- Inferred FK ({c.get('classification','')}): {cs}.{ct}.{cc} -> {ps}.{pt}.{pc}")
        lines.append(f"SELECT")
        lines.append(f"  '{child_t}' AS child_table,")
        lines.append(f"  '{cc}' AS child_column,")
        lines.append(f"  '{parent_t}' AS parent_table,")
        lines.append(f"  '{pc}' AS parent_column,")
        lines.append(f"  'inferred_{c.get('classification','')}' AS fk_source,")
        lines.append(f"  COUNT(*) AS orphan_count")
        lines.append(f"FROM {child_t} c")
        lines.append(f"LEFT JOIN {parent_t} p ON c.`{cc}` = p.`{pc}`")
        lines.append(f"WHERE p.`{pc}` IS NULL AND c.`{cc}` IS NOT NULL;")
        lines.append("")

    return lines


# ═══════════════════════════════════════════════════════════════════════════
# Manual tasks markdown
# ═══════════════════════════════════════════════════════════════════════════

def generate_manual_tasks_md(
    manual_fk_tasks: list[dict],
    manual_rewrite_objects: list[dict],
    not_auto_created: list[str],
) -> str:
    """Generate the manual_tasks.md checklist."""
    lines = []
    lines.append("# Manual Tasks Checklist — Redshift to Databricks Migration")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")

    # Section 1: NOT auto-created
    lines.append("## Objects NOT Auto-Created")
    lines.append("")
    lines.append("The following object types require manual implementation in Databricks:")
    lines.append("")
    for item in not_auto_created:
        lines.append(f"- [ ] {item}")
    lines.append("")

    # Section 2: Manual rewrite objects
    if manual_rewrite_objects:
        lines.append("## Objects Requiring Manual Rewrite")
        lines.append("")
        lines.append("These objects were classified as `MANUAL_REWRITE_REQUIRED` by the transpiler.")
        lines.append("They contain procedural logic not supported in Databricks SQL.")
        lines.append("")
        for obj in manual_rewrite_objects:
            fname = obj.get("filename", "unknown")
            reasons = obj.get("manual_reasons", [])
            lines.append(f"### `{fname}`")
            lines.append("")
            for r in reasons:
                lines.append(f"- {r}")
            lines.append("")
            lines.append("**Action:** Rewrite as Databricks notebook, Python UDF, or Databricks workflow.")
            lines.append("")

    # Section 3: FK constraints needing approval
    if manual_fk_tasks:
        lines.append("## FK Constraints Requiring Data Owner Approval")
        lines.append("")
        lines.append("These FK relationships were either declared in the source or inferred")
        lines.append("by statistical profiling. **Do not enable without verification.**")
        lines.append("")

        declared = [t for t in manual_fk_tasks if t.get("type") == "declared_fk"]
        inferred = [t for t in manual_fk_tasks if t.get("type") == "inferred_fk"]

        if declared:
            lines.append("### Declared FKs (from source catalog)")
            lines.append("")
            for t in declared:
                lines.append(f"- [ ] **{t['source']}** -> {t['target']}")
                lines.append(f"  - {t['action']}")
            lines.append("")

        if inferred:
            lines.append("### Inferred FKs (from relationship profiler)")
            lines.append("")
            for t in inferred:
                lines.append(
                    f"- [ ] **{t['source']}** "
                    f"(overlap: {t['overlap_ratio']}, "
                    f"unique: {t['parent_unique']}, "
                    f"class: {t['classification']})"
                )
                lines.append(f"  - {t['action']}")
            lines.append("")

    # Section 4: Validation job
    lines.append("## Post-Deployment Validation")
    lines.append("")
    lines.append("- [ ] Run `artifacts/deploy/04_validation_job.sql` to check referential integrity")
    lines.append("- [ ] Review `artifacts/ingestion_job_report.json` for schema mismatches")
    lines.append("- [ ] Compare row counts between source Redshift tables and target Delta tables")
    lines.append("- [ ] Run sample queries from `artifacts/top_queries.csv` against Databricks")
    lines.append("- [ ] Verify data types are correctly mapped (spot-check STRING vs original VARCHAR lengths)")
    lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Deploy script generation
# ═══════════════════════════════════════════════════════════════════════════

def generate_deploy_sh() -> str:
    """Generate deploy.sh for executing DDL against Databricks."""
    return r"""#!/usr/bin/env bash
# ============================================================
# deploy.sh — Execute migration DDL against Databricks
# ============================================================
#
# Usage:
#   ./deploy.sh                          # dry-run: print DDL to stdout
#   ./deploy.sh --execute                # execute via Databricks SQL endpoint
#   ./deploy.sh --execute --warehouse-id <id>  # specify SQL warehouse
#
# Prerequisites:
#   - Databricks CLI configured (databricks auth login)
#   - Or set DATABRICKS_HOST and DATABRICKS_TOKEN env vars
#
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${SCRIPT_DIR}/artifacts/deploy"
EXECUTE=false
WAREHOUSE_ID="${DATABRICKS_WAREHOUSE_ID:-}"
DATABRICKS_HOST="${DATABRICKS_HOST:-}"
DATABRICKS_TOKEN="${DATABRICKS_TOKEN:-}"

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --execute)
            EXECUTE=true
            shift
            ;;
        --warehouse-id)
            WAREHOUSE_ID="$2"
            shift 2
            ;;
        --host)
            DATABRICKS_HOST="$2"
            shift 2
            ;;
        --token)
            DATABRICKS_TOKEN="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--execute] [--warehouse-id <id>] [--host <url>] [--token <token>]"
            echo ""
            echo "Options:"
            echo "  --execute        Execute DDL against Databricks (default: dry-run/print)"
            echo "  --warehouse-id   Databricks SQL warehouse ID"
            echo "  --host           Databricks workspace URL"
            echo "  --token          Databricks personal access token"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check deploy directory
if [ ! -d "$DEPLOY_DIR" ]; then
    echo "ERROR: Deploy directory not found: $DEPLOY_DIR"
    echo "Run ddl_deployer.py first."
    exit 1
fi

# Ordered list of DDL files to execute
DDL_FILES=(
    "00_databases.sql"
    "01_tables.sql"
    "02_views.sql"
    "03_constraints.sql"
)

echo "============================================================"
echo "Redshift → Databricks DDL Deployment"
echo "============================================================"
echo "Deploy dir  : $DEPLOY_DIR"
echo "Mode        : $([ "$EXECUTE" = true ] && echo "EXECUTE" || echo "DRY RUN (review only)")"
echo "Warehouse   : ${WAREHOUSE_ID:-<not set>}"
echo "============================================================"
echo ""

execute_sql_file() {
    local file="$1"
    local filepath="${DEPLOY_DIR}/${file}"

    if [ ! -f "$filepath" ]; then
        echo "SKIP: $file (not found)"
        return
    fi

    echo "--- Processing: $file ---"

    if [ "$EXECUTE" = false ]; then
        echo ""
        cat "$filepath"
        echo ""
        echo "--- End: $file ---"
        echo ""
        return
    fi

    # Execute via Databricks SQL Statement API
    if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
        echo "ERROR: --execute requires DATABRICKS_HOST and DATABRICKS_TOKEN"
        exit 1
    fi

    if [ -z "$WAREHOUSE_ID" ]; then
        echo "ERROR: --execute requires --warehouse-id or DATABRICKS_WAREHOUSE_ID env var"
        exit 1
    fi

    # Read file and split on semicolons, execute each statement
    while IFS= read -r stmt; do
        # Skip empty lines and comments
        trimmed="$(echo "$stmt" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
        if [ -z "$trimmed" ] || [[ "$trimmed" == --* ]]; then
            continue
        fi

        echo "  Executing: ${trimmed:0:80}..."

        response=$(curl -s -X POST \
            "${DATABRICKS_HOST}/api/2.0/sql/statements/" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{
                \"warehouse_id\": \"${WAREHOUSE_ID}\",
                \"statement\": $(echo "$trimmed" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'),
                \"wait_timeout\": \"30s\"
            }")

        status=$(echo "$response" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "ERROR")

        if [ "$status" = "SUCCEEDED" ]; then
            echo "    -> OK"
        elif [ "$status" = "FAILED" ]; then
            error=$(echo "$response" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status',{}).get('error',{}).get('message','unknown'))" 2>/dev/null || echo "unknown")
            echo "    -> FAILED: $error"
        else
            echo "    -> Status: $status"
        fi
    done < <(python3 -c "
import re, sys
sql = open('$filepath', encoding='utf-8').read()
# Split on semicolons not inside comments
stmts = re.split(r';\s*$', sql, flags=re.MULTILINE)
for s in stmts:
    s = s.strip()
    if s and not all(line.strip().startswith('--') or not line.strip() for line in s.splitlines()):
        print(s + ';')
        print('---STMT_DELIMITER---')
" | awk '/---STMT_DELIMITER---/{next}{buf=buf?buf"\n"$0:$0} /;$/{print buf; buf=""}')

    echo "--- Done: $file ---"
    echo ""
}

for ddl_file in "${DDL_FILES[@]}"; do
    execute_sql_file "$ddl_file"
done

echo ""
echo "============================================================"
echo "Deployment $([ "$EXECUTE" = true ] && echo "complete" || echo "preview complete (use --execute to apply)")"
echo ""
echo "IMPORTANT — Items NOT auto-deployed:"
echo "  - Stored procedures (require manual rewrite as notebooks/workflows)"
echo "  - Complex Python UDFs (require manual rewrite as Databricks UDFs)"
echo "  - PL/pgSQL functions (no Spark SQL equivalent)"
echo "  - FK constraints marked MANUAL CONFIRMATION REQUIRED"
echo "  - COPY/UNLOAD commands (handled by export_to_s3.py + databricks_ingest.py)"
echo ""
echo "Review: artifacts/manual_tasks.md for full checklist."
echo "============================================================"
"""


# ═══════════════════════════════════════════════════════════════════════════
# Main orchestration
# ═══════════════════════════════════════════════════════════════════════════

def run_deploy_generation(
    prod_db: str = TARGET_PROD_DB,
    stg_db: str = TARGET_STG_DB,
):
    """Generate all deployment artifacts."""
    global TARGET_PROD_DB, TARGET_STG_DB
    TARGET_PROD_DB = prod_db
    TARGET_STG_DB = stg_db

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    DEPLOY_DIR.mkdir(parents=True, exist_ok=True)

    # --- Load inputs -------------------------------------------------------
    catalog = _load_catalog()
    convert_report = _load_convert_report()
    transpiled_files = _load_transpiled_files()
    fk_candidates = _load_fk_candidates()

    all_manual_tasks = []
    deploy_inventory = {
        "databases": [],
        "tables": [],
        "views": [],
        "constraints": [],
        "manual_rewrite": [],
        "not_auto_created": [],
    }

    # --- 00_databases.sql ---------------------------------------------------
    db_lines = [
        "-- ============================================================",
        "-- Database creation",
        "-- ============================================================",
        "",
        f"CREATE DATABASE IF NOT EXISTS {stg_db};",
        f"CREATE DATABASE IF NOT EXISTS {prod_db};",
        "",
    ]
    deploy_inventory["databases"] = [stg_db, prod_db]

    # --- Classify transpiled files ------------------------------------------
    classified = []
    for fname, content in transpiled_files.items():
        info = _classify_transpiled(fname, content)
        classified.append(info)

    # --- 01_tables.sql ------------------------------------------------------
    table_lines = [
        "-- ============================================================",
        "-- Table DDL (from transpiled output)",
        "-- ============================================================",
        "",
    ]
    for obj in classified:
        if obj["object_type"] == "table" and not obj["is_manual_rewrite"]:
            table_lines.append(f"-- Source file: {obj['filename']}")
            table_lines.append(obj["content"])
            table_lines.append("")
            deploy_inventory["tables"].append({
                "name": obj["object_name"],
                "source_file": obj["filename"],
                "classification": obj["classification"],
            })

    # --- 02_views.sql -------------------------------------------------------
    view_lines = [
        "-- ============================================================",
        "-- View DDL (from transpiled output)",
        "-- ============================================================",
        "",
    ]
    for obj in classified:
        if obj["object_type"] == "view" and not obj["is_manual_rewrite"]:
            view_lines.append(f"-- Source file: {obj['filename']}")
            view_lines.append(obj["content"])
            view_lines.append("")
            deploy_inventory["views"].append({
                "name": obj["object_name"],
                "source_file": obj["filename"],
                "classification": obj["classification"],
            })

    # --- Manual rewrite objects ---------------------------------------------
    manual_rewrite_objects = []
    for obj in convert_report.get("objects", []):
        if obj.get("classification") == "MANUAL_REWRITE_REQUIRED":
            manual_rewrite_objects.append({
                "filename": obj.get("source_path", "unknown"),
                "manual_reasons": obj.get("manual_reasons", []),
                "difficulty_score": obj.get("difficulty_score", 10),
            })
            deploy_inventory["manual_rewrite"].append({
                "name": obj.get("source_path", "unknown"),
                "reasons": obj.get("manual_reasons", []),
            })

    # --- 03_constraints.sql -------------------------------------------------
    declared_ddl, declared_tasks = generate_declared_constraints_ddl(catalog)
    inferred_ddl, inferred_tasks = generate_inferred_fk_ddl(fk_candidates)

    constraint_lines = declared_ddl + inferred_ddl
    all_manual_tasks.extend(declared_tasks)
    all_manual_tasks.extend(inferred_tasks)

    deploy_inventory["constraints"] = {
        "declared_pk_unique": sum(
            1 for c in catalog.get("constraints", [])
            if (c.get("constraint_type") or "").upper() in ("PRIMARY KEY", "UNIQUE")
        ),
        "declared_fk": len(declared_tasks),
        "inferred_fk": len(inferred_tasks),
    }

    # --- 04_validation_job.sql ----------------------------------------------
    validation_lines = generate_validation_job_sql(catalog, fk_candidates)

    # --- NOT auto-created list ----------------------------------------------
    not_auto_created = [
        "**Stored procedures** — Redshift stored procedures use PL/pgSQL which has no "
        "Spark SQL equivalent. Rewrite as Databricks notebooks or workflows.",
        "**Complex Python UDFs (plpythonu)** — Rewrite as Databricks Python UDFs "
        "registered via `CREATE FUNCTION` or as notebook logic.",
        "**PL/pgSQL functions** — Procedural SQL functions with control flow "
        "(IF/THEN, LOOP, CURSOR, RAISE) must be converted to notebooks or Delta Live Tables.",
        "**FK constraints marked MANUAL CONFIRMATION REQUIRED** — Inferred FK relationships "
        "must be approved by data owners before enabling.",
        "**COPY / UNLOAD commands** — Handled separately by `export_to_s3.py` and "
        "`databricks_ingest.py`, not by DDL deployment.",
        "**External schemas / Spectrum tables** — Map manually to Unity Catalog "
        "external locations or federated queries.",
        "**Redshift CREATE LIBRARY** — No Databricks equivalent; install Python "
        "packages via cluster libraries or `%pip install`.",
        "**Scheduled jobs / cron-triggered queries** — Recreate as Databricks "
        "Workflows or scheduled SQL queries.",
    ]
    deploy_inventory["not_auto_created"] = not_auto_created

    # --- Write all files ----------------------------------------------------
    _write_sql(DEPLOY_DIR / "00_databases.sql", db_lines)
    _write_sql(DEPLOY_DIR / "01_tables.sql", table_lines)
    _write_sql(DEPLOY_DIR / "02_views.sql", view_lines)
    _write_sql(DEPLOY_DIR / "03_constraints.sql", constraint_lines)
    _write_sql(DEPLOY_DIR / "04_validation_job.sql", validation_lines)

    # Deploy manifest
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_prod_database": prod_db,
        "target_stg_database": stg_db,
        "inventory": deploy_inventory,
    }
    with open(DEPLOY_MANIFEST_PATH, "w", encoding="utf-8") as fp:
        json.dump(manifest, fp, indent=2, default=str)
    logger.info("Deploy manifest written to %s", DEPLOY_MANIFEST_PATH)

    # Manual tasks markdown
    md_content = generate_manual_tasks_md(
        all_manual_tasks, manual_rewrite_objects, not_auto_created
    )
    with open(MANUAL_TASKS_PATH, "w", encoding="utf-8") as fp:
        fp.write(md_content)
    logger.info("Manual tasks checklist written to %s", MANUAL_TASKS_PATH)

    # deploy.sh
    sh_content = generate_deploy_sh()
    with open(DEPLOY_SCRIPT_PATH, "w", encoding="utf-8", newline="\n") as fp:
        fp.write(sh_content)
    logger.info("Deploy script written to %s", DEPLOY_SCRIPT_PATH)

    # --- Recap --------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("DDL deployment artifacts generated:")
    logger.info("  Deploy dir         : %s", DEPLOY_DIR)
    logger.info("  Tables             : %d", len(deploy_inventory["tables"]))
    logger.info("  Views              : %d", len(deploy_inventory["views"]))
    logger.info("  Declared constraints: PK/UNIQUE + %d FK", len(declared_tasks))
    logger.info("  Inferred FK        : %d (all MANUAL)", len(inferred_tasks))
    logger.info("  Manual rewrite     : %d objects", len(manual_rewrite_objects))
    logger.info("  Not auto-created   : %d categories", len(not_auto_created))
    logger.info("")
    logger.info("  Deploy script      : %s", DEPLOY_SCRIPT_PATH)
    logger.info("  Manual tasks       : %s", MANUAL_TASKS_PATH)
    logger.info("=" * 60)


def _write_sql(path: Path, lines: list[str]):
    with open(path, "w", encoding="utf-8") as fp:
        fp.write("\n".join(lines))
    logger.info("Written %s (%d lines)", path.name, len(lines))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate Databricks Delta DDL deployment artifacts from "
            "transpiled SQL, source catalog, and FK candidates."
        )
    )
    parser.add_argument(
        "--prod-db",
        default=TARGET_PROD_DB,
        help=f"Target production database name (default: {TARGET_PROD_DB})",
    )
    parser.add_argument(
        "--stg-db",
        default=TARGET_STG_DB,
        help=f"Target staging database name (default: {TARGET_STG_DB})",
    )
    args = parser.parse_args()

    try:
        run_deploy_generation(prod_db=args.prod_db, stg_db=args.stg_db)
    except Exception:
        logger.exception("Unexpected error during DDL deployment generation.")
        sys.exit(99)


if __name__ == "__main__":
    main()
