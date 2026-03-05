"""
confidence_calculator.py
========================
Reads workload scores, transpiler classification, ingestion report, and
validation results to compute a per-object migration confidence score.

Formula (weights editable in CONFIG):
  confidence = W_VALIDATION * validation_pass_rate
             + W_DIFFICULTY * (1 - difficulty_normalized)
             + W_COVERAGE  * coverage_score

Where:
  validation_pass_rate        = fraction of parity checks that passed
  difficulty_normalized       = transpiler difficulty score / 10 (0=easy → 1=hard)
  coverage_score              = fraction of columns with exact type mapping
                                and no nullability mismatch

Objects with confidence < MANUAL_REVIEW_THRESHOLD are flagged as
"needs_manual_review" and appended to artifacts/manual_tasks.md.

Outputs:
  artifacts/confidence_summary.csv   — per-object one-liner
  artifacts/confidence_report.json   — full breakdown with formula inputs
  artifacts/manual_tasks.md          — appended section for low-confidence objects
"""

import argparse
import csv
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration — editable weights
# ---------------------------------------------------------------------------
CONFIG = {
    "W_VALIDATION": 0.5,
    "W_DIFFICULTY": 0.3,
    "W_COVERAGE": 0.2,
    "MANUAL_REVIEW_THRESHOLD": 0.6,
    "MAX_DIFFICULTY": 10,
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
CATALOG_PATH = ARTIFACTS_DIR / "source_catalog.json"
CONVERT_REPORT_PATH = ARTIFACTS_DIR / "convert_report.json"
VALIDATION_RESULTS_PATH = ARTIFACTS_DIR / "validation_results.json"
INGESTION_REPORT_PATH = ARTIFACTS_DIR / "ingestion_job_report.json"
WORKLOAD_SUMMARY_PATH = ARTIFACTS_DIR / "workload_summary.json"

CONFIDENCE_CSV_PATH = ARTIFACTS_DIR / "confidence_summary.csv"
CONFIDENCE_JSON_PATH = ARTIFACTS_DIR / "confidence_report.json"
MANUAL_TASKS_PATH = ARTIFACTS_DIR / "manual_tasks.md"

# ---------------------------------------------------------------------------
# Redshift → Databricks expected type map (for coverage scoring)
# ---------------------------------------------------------------------------
EXPECTED_TYPE_MAP = {
    "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "smallint": "SMALLINT", "int2": "SMALLINT",
    "integer": "INT", "int": "INT", "int4": "INT",
    "bigint": "BIGINT", "int8": "BIGINT",
    "real": "FLOAT", "float4": "FLOAT",
    "float": "DOUBLE", "float8": "DOUBLE", "double precision": "DOUBLE",
    "numeric": "DECIMAL", "decimal": "DECIMAL",
    "character": "STRING", "char": "STRING", "nchar": "STRING",
    "bpchar": "STRING", "character varying": "STRING",
    "varchar": "STRING", "nvarchar": "STRING", "text": "STRING",
    "date": "DATE",
    "timestamp": "TIMESTAMP", "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMP", "timestamptz": "TIMESTAMP",
    "time": "STRING", "time without time zone": "STRING", "timetz": "STRING",
    "super": "STRING", "varbyte": "BINARY", "bytea": "BINARY",
    "geometry": "STRING", "hllsketch": "BINARY",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_console)


def _init_file_logging():
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(ARTIFACTS_DIR / "confidence_calculator.log", mode="w")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)


# ═══════════════════════════════════════════════════════════════════════════
# Loaders
# ═══════════════════════════════════════════════════════════════════════════

def _load_json(path: Path):
    if not path.exists():
        logger.warning("Not found: %s", path)
        return None
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def _sanitise_name(schema: str, table: str) -> str:
    s = re.sub(r"[^a-z0-9_]", "_", schema.lower())
    t = re.sub(r"[^a-z0-9_]", "_", table.lower())
    return f"{s}_{t}"


# ═══════════════════════════════════════════════════════════════════════════
# Build per-table indexes from input artifacts
# ═══════════════════════════════════════════════════════════════════════════

def _build_validation_index(validation: dict | None) -> dict:
    """
    Build {table_key: {"passed": n, "failed": n, "total": n, "pass_rate": float}}
    from validation_results.json.
    """
    idx = {}
    if not validation:
        return idx
    for table_key, checks in validation.get("tables", {}).items():
        passed = 0
        failed = 0
        errors = 0
        for chk in checks:
            status = (chk.get("comparison") or {}).get("status", "UNKNOWN")
            if status == "PASS":
                passed += 1
            elif status == "FAIL":
                failed += 1
            else:
                errors += 1
        total = passed + failed
        idx[table_key.lower()] = {
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "total": total,
            "pass_rate": round(passed / total, 4) if total > 0 else 0.0,
        }
    return idx


def _build_difficulty_index(convert_report: dict | None) -> dict:
    """
    Build {source_path: {"difficulty": int, "classification": str}}
    from convert_report.json.
    """
    idx = {}
    if not convert_report:
        return idx
    for obj in convert_report.get("objects", []):
        src = (obj.get("source_path") or "").lower()
        idx[src] = {
            "difficulty_score": obj.get("difficulty_score", 5),
            "classification": obj.get("classification", "UNKNOWN"),
            "warnings": obj.get("warnings", []),
            "manual_reasons": obj.get("manual_reasons", []),
        }
    return idx


def _build_ingestion_index(ingestion: dict | None) -> dict:
    """
    Build {table_key: {"rows_loaded": n, "schema_mismatches": [...]}}
    from ingestion_job_report.json.
    """
    idx = {}
    if not ingestion:
        return idx
    for t in ingestion.get("tables", []):
        schema = (t.get("source_schema") or "").lower()
        table = (t.get("source_table") or "").lower()
        key = f"{schema}.{table}"
        idx[key] = {
            "rows_loaded": t.get("rows_loaded", 0),
            "status": t.get("status", "unknown"),
            "schema_mismatches": t.get("schema_mismatches", []),
        }
    return idx


def _build_coverage_index(catalog: dict | None, ingestion_idx: dict) -> dict:
    """
    Build {table_key: coverage_score} where coverage_score is the fraction
    of columns with a known type mapping and no detected nullability mismatch.
    """
    idx = {}
    if not catalog:
        return idx

    # Group columns by table
    table_cols = {}
    for c in catalog.get("columns", []):
        schema = (c.get("table_schema") or "").lower()
        table = (c.get("table_name") or "").lower()
        key = f"{schema}.{table}"
        table_cols.setdefault(key, []).append(c)

    for key, cols in table_cols.items():
        if not cols:
            continue

        matched = 0
        total = len(cols)
        mismatches_from_ingestion = {
            m.get("column", "").lower()
            for m in ingestion_idx.get(key, {}).get("schema_mismatches", [])
        }

        for c in cols:
            col_name = (c.get("column_name") or "").lower()
            src_type = (c.get("data_type") or "").lower().strip()

            # Check: do we have a known mapping?
            # Strip parameterised part for lookup
            base_type = re.sub(r"\(.*\)", "", src_type).strip()
            has_mapping = base_type in EXPECTED_TYPE_MAP

            # Check: no nullability mismatch detected in ingestion
            no_mismatch = col_name not in mismatches_from_ingestion

            if has_mapping and no_mismatch:
                matched += 1

        idx[key] = round(matched / total, 4) if total > 0 else 0.0

    return idx


def _build_workload_index(workload: dict | None) -> dict:
    """Extract per-table workload info (query count, complexity) if available."""
    idx = {}
    if not workload:
        return idx
    for fp in workload.get("fingerprint_stats", []):
        text = (fp.get("sample_query_text") or "").lower()
        # Try to extract table references — best effort
        for m in re.finditer(r"\bfrom\s+(\w+\.\w+)", text):
            table_ref = m.group(1).lower()
            if table_ref not in idx:
                idx[table_ref] = {
                    "query_count": 0,
                    "max_complexity": 0,
                }
            idx[table_ref]["query_count"] += fp.get("execution_count", 0)
            complexity = (fp.get("complexity") or {}).get("complexity_score", 0)
            idx[table_ref]["max_complexity"] = max(
                idx[table_ref]["max_complexity"], complexity
            )
    return idx


# ═══════════════════════════════════════════════════════════════════════════
# Confidence calculation
# ═══════════════════════════════════════════════════════════════════════════

def compute_confidence(
    table_key: str,
    validation_idx: dict,
    difficulty_idx: dict,
    coverage_idx: dict,
    ingestion_idx: dict,
    workload_idx: dict,
    config: dict,
) -> dict:
    """
    Compute confidence score for a single table/object.
    Returns a dict with the score and all formula inputs for auditability.
    """
    w_val = config["W_VALIDATION"]
    w_diff = config["W_DIFFICULTY"]
    w_cov = config["W_COVERAGE"]
    max_diff = config["MAX_DIFFICULTY"]
    threshold = config["MANUAL_REVIEW_THRESHOLD"]

    # --- Validation pass rate ------------------------------------------------
    val_info = validation_idx.get(table_key, {})
    validation_pass_rate = val_info.get("pass_rate", 0.0)
    val_passed = val_info.get("passed", 0)
    val_failed = val_info.get("failed", 0)
    val_errors = val_info.get("errors", 0)
    val_total = val_info.get("total", 0)

    # --- Difficulty (normalised) ---------------------------------------------
    # Try multiple key patterns to find difficulty
    diff_info = {}
    for candidate_key in _difficulty_key_variants(table_key):
        if candidate_key in difficulty_idx:
            diff_info = difficulty_idx[candidate_key]
            break

    raw_difficulty = diff_info.get("difficulty_score", 5)
    difficulty_normalized = min(raw_difficulty / max_diff, 1.0)
    classification = diff_info.get("classification", "UNKNOWN")

    # Force MANUAL_REWRITE_REQUIRED to low confidence
    if classification == "MANUAL_REWRITE_REQUIRED":
        difficulty_normalized = 1.0

    # --- Coverage score ------------------------------------------------------
    coverage_score = coverage_idx.get(table_key, 1.0)

    # --- Ingestion status bonus/penalty --------------------------------------
    ing_info = ingestion_idx.get(table_key, {})
    ing_status = ing_info.get("status", "unknown")
    ing_mismatches = len(ing_info.get("schema_mismatches", []))

    # Penalise coverage if ingestion had schema mismatches
    if ing_mismatches > 0:
        coverage_score = max(0.0, coverage_score - 0.1 * ing_mismatches)

    # --- Workload context (informational, not in formula) --------------------
    wl_info = workload_idx.get(table_key, {})

    # --- Compute confidence --------------------------------------------------
    confidence = (
        w_val * validation_pass_rate
        + w_diff * (1.0 - difficulty_normalized)
        + w_cov * coverage_score
    )
    confidence = round(max(0.0, min(1.0, confidence)), 4)

    needs_review = confidence < threshold

    return {
        "table": table_key,
        "confidence_score": confidence,
        "needs_manual_review": needs_review,
        "formula_inputs": {
            "validation_pass_rate": validation_pass_rate,
            "validation_passed": val_passed,
            "validation_failed": val_failed,
            "validation_errors": val_errors,
            "validation_total": val_total,
            "difficulty_score_raw": raw_difficulty,
            "difficulty_normalized": round(difficulty_normalized, 4),
            "classification": classification,
            "coverage_score": round(coverage_score, 4),
            "ingestion_status": ing_status,
            "ingestion_schema_mismatches": ing_mismatches,
            "workload_query_count": wl_info.get("query_count", 0),
            "workload_max_complexity": wl_info.get("max_complexity", 0),
        },
        "weights_used": {
            "W_VALIDATION": w_val,
            "W_DIFFICULTY": w_diff,
            "W_COVERAGE": w_cov,
        },
    }


def _difficulty_key_variants(table_key: str) -> list[str]:
    """Generate possible keys to look up in the difficulty index."""
    parts = table_key.split(".")
    variants = [table_key]
    if len(parts) == 2:
        schema, table = parts
        # transpiler source_path patterns
        variants.extend([
            f"views/{schema}.{table}.sql",
            f"procs/{schema}.{table}.sql",
            f"udfs/{schema}.{table}.sql",
            f"materialized_views/{schema}.{table}.sql",
            f"{schema}.{table}.sql",
            f"{schema}__{table}.sql",
        ])
    return variants


# ═══════════════════════════════════════════════════════════════════════════
# Output writers
# ═══════════════════════════════════════════════════════════════════════════

CSV_FIELDS = [
    "table",
    "confidence_score",
    "needs_manual_review",
    "validation_pass_rate",
    "difficulty_normalized",
    "coverage_score",
    "classification",
    "ingestion_status",
    "ingestion_schema_mismatches",
    "workload_query_count",
]


def write_csv(results: list[dict]):
    with open(CONFIDENCE_CSV_PATH, "w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            flat = {
                "table": r["table"],
                "confidence_score": r["confidence_score"],
                "needs_manual_review": r["needs_manual_review"],
                **r["formula_inputs"],
            }
            writer.writerow(flat)
    logger.info("Confidence CSV written to %s (%d rows)", CONFIDENCE_CSV_PATH, len(results))


def write_json(results: list[dict], config: dict):
    high = sum(1 for r in results if r["confidence_score"] >= 0.8)
    medium = sum(1 for r in results if 0.6 <= r["confidence_score"] < 0.8)
    low = sum(1 for r in results if r["confidence_score"] < 0.6)
    avg = round(sum(r["confidence_score"] for r in results) / max(len(results), 1), 4)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "summary": {
            "total_objects": len(results),
            "average_confidence": avg,
            "high_confidence_gte_0_8": high,
            "medium_confidence_0_6_to_0_8": medium,
            "low_confidence_lt_0_6": low,
            "needs_manual_review": low,
        },
        "formula": (
            "confidence = W_VALIDATION * validation_pass_rate "
            "+ W_DIFFICULTY * (1 - difficulty_normalized) "
            "+ W_COVERAGE * coverage_score"
        ),
        "objects": results,
    }
    with open(CONFIDENCE_JSON_PATH, "w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2, default=str)
    logger.info("Confidence report written to %s", CONFIDENCE_JSON_PATH)


def append_manual_tasks(results: list[dict]):
    """Append low-confidence objects to manual_tasks.md."""
    low = [r for r in results if r["needs_manual_review"]]
    if not low:
        logger.info("No low-confidence objects to append to manual_tasks.md.")
        return

    lines = []

    # If file exists, read existing content; otherwise start fresh
    if MANUAL_TASKS_PATH.exists():
        existing = MANUAL_TASKS_PATH.read_text(encoding="utf-8")
        # Remove any previous confidence section if re-running
        marker = "## Low-Confidence Objects (Automated Review)"
        if marker in existing:
            existing = existing[: existing.index(marker)].rstrip()
        lines.append(existing)
        lines.append("")
    else:
        lines.append("# Manual Tasks Checklist")
        lines.append("")

    lines.append("## Low-Confidence Objects (Automated Review)")
    lines.append("")
    lines.append(
        f"Generated: {datetime.now(timezone.utc).isoformat()} | "
        f"Threshold: {CONFIG['MANUAL_REVIEW_THRESHOLD']} | "
        f"Objects flagged: {len(low)}"
    )
    lines.append("")
    lines.append(
        "The following objects scored below the confidence threshold and require "
        "manual review before the migration can be considered complete."
    )
    lines.append("")
    lines.append(
        "| # | Object | Score | Validation | Difficulty | Coverage | Classification |"
    )
    lines.append(
        "|---|--------|-------|------------|------------|----------|----------------|"
    )

    for i, r in enumerate(sorted(low, key=lambda x: x["confidence_score"]), 1):
        fi = r["formula_inputs"]
        lines.append(
            f"| {i} "
            f"| `{r['table']}` "
            f"| **{r['confidence_score']:.2f}** "
            f"| {fi['validation_pass_rate']:.0%} ({fi['validation_passed']}/{fi['validation_total']}) "
            f"| {fi['difficulty_score_raw']}/10 "
            f"| {fi['coverage_score']:.0%} "
            f"| {fi['classification']} |"
        )

    lines.append("")
    lines.append("### Actions Required")
    lines.append("")
    for r in sorted(low, key=lambda x: x["confidence_score"]):
        fi = r["formula_inputs"]
        lines.append(f"- [ ] **`{r['table']}`** (score: {r['confidence_score']:.2f})")

        if fi["validation_pass_rate"] < 0.8:
            lines.append(
                f"  - Validation pass rate is low ({fi['validation_pass_rate']:.0%}). "
                f"Investigate failed checks in `validation_results.json`."
            )
        if fi["difficulty_normalized"] > 0.7:
            lines.append(
                f"  - High transpilation difficulty ({fi['difficulty_score_raw']}/10). "
                f"Review transpiled SQL for correctness."
            )
        if fi["classification"] == "MANUAL_REWRITE_REQUIRED":
            lines.append(
                "  - Classified as MANUAL_REWRITE_REQUIRED. This object must be "
                "manually converted."
            )
        if fi["coverage_score"] < 0.9:
            lines.append(
                f"  - Coverage score is {fi['coverage_score']:.0%}. "
                f"Check column type mappings and nullability."
            )
        if fi["ingestion_schema_mismatches"] > 0:
            lines.append(
                f"  - {fi['ingestion_schema_mismatches']} schema mismatch(es) "
                f"detected during ingestion."
            )
        lines.append("")

    with open(MANUAL_TASKS_PATH, "w", encoding="utf-8") as fp:
        fp.write("\n".join(lines))
    logger.info(
        "Appended %d low-confidence objects to %s", len(low), MANUAL_TASKS_PATH
    )


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def run_calculation(config: dict | None = None):
    """Run the confidence calculation pipeline."""
    cfg = {**CONFIG, **(config or {})}
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    _init_file_logging()

    logger.info("Loading input artifacts ...")
    catalog = _load_json(CATALOG_PATH) or {}
    convert_report = _load_json(CONVERT_REPORT_PATH)
    validation = _load_json(VALIDATION_RESULTS_PATH)
    ingestion = _load_json(INGESTION_REPORT_PATH)
    workload = _load_json(WORKLOAD_SUMMARY_PATH)

    logger.info("Building indexes ...")
    validation_idx = _build_validation_index(validation)
    difficulty_idx = _build_difficulty_index(convert_report)
    ingestion_idx = _build_ingestion_index(ingestion)
    coverage_idx = _build_coverage_index(catalog, ingestion_idx)
    workload_idx = _build_workload_index(workload)

    # Determine the universe of objects to score.
    # Primary: every table in the catalog. Also include any objects
    # that appear only in validation or difficulty indexes.
    all_keys = set()
    for c in catalog.get("columns", []):
        schema = (c.get("table_schema") or "").lower()
        table = (c.get("table_name") or "").lower()
        if schema not in ("pg_catalog", "information_schema", "pg_internal"):
            all_keys.add(f"{schema}.{table}")
    all_keys.update(validation_idx.keys())

    logger.info("Computing confidence for %d objects ...", len(all_keys))

    results = []
    for key in sorted(all_keys):
        r = compute_confidence(
            key, validation_idx, difficulty_idx, coverage_idx,
            ingestion_idx, workload_idx, cfg,
        )
        results.append(r)

    # Sort by confidence ascending (worst first)
    results.sort(key=lambda x: x["confidence_score"])

    # Write outputs
    write_csv(results)
    write_json(results, cfg)
    append_manual_tasks(results)

    # Recap
    high = sum(1 for r in results if r["confidence_score"] >= 0.8)
    medium = sum(1 for r in results if 0.6 <= r["confidence_score"] < 0.8)
    low = sum(1 for r in results if r["confidence_score"] < 0.6)
    avg = round(
        sum(r["confidence_score"] for r in results) / max(len(results), 1), 4
    )

    logger.info("=" * 60)
    logger.info("Confidence calculation complete:")
    logger.info("  Total objects        : %d", len(results))
    logger.info("  Average confidence   : %.2f", avg)
    logger.info("  High (>= 0.8)       : %d", high)
    logger.info("  Medium (0.6 – 0.8)  : %d", medium)
    logger.info("  Low (< 0.6)         : %d  ← needs manual review", low)
    logger.info("")
    logger.info("  Weights: validation=%.1f  difficulty=%.1f  coverage=%.1f",
                cfg["W_VALIDATION"], cfg["W_DIFFICULTY"], cfg["W_COVERAGE"])
    logger.info("  Threshold: %.1f", cfg["MANUAL_REVIEW_THRESHOLD"])
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Compute per-object migration confidence scores."
    )
    parser.add_argument(
        "--w-validation", type=float, default=CONFIG["W_VALIDATION"],
        help=f"Weight for validation pass rate (default: {CONFIG['W_VALIDATION']})",
    )
    parser.add_argument(
        "--w-difficulty", type=float, default=CONFIG["W_DIFFICULTY"],
        help=f"Weight for difficulty score (default: {CONFIG['W_DIFFICULTY']})",
    )
    parser.add_argument(
        "--w-coverage", type=float, default=CONFIG["W_COVERAGE"],
        help=f"Weight for coverage score (default: {CONFIG['W_COVERAGE']})",
    )
    parser.add_argument(
        "--threshold", type=float, default=CONFIG["MANUAL_REVIEW_THRESHOLD"],
        help=f"Confidence below this triggers manual review (default: {CONFIG['MANUAL_REVIEW_THRESHOLD']})",
    )
    args = parser.parse_args()

    custom = {
        "W_VALIDATION": args.w_validation,
        "W_DIFFICULTY": args.w_difficulty,
        "W_COVERAGE": args.w_coverage,
        "MANUAL_REVIEW_THRESHOLD": args.threshold,
        "MAX_DIFFICULTY": CONFIG["MAX_DIFFICULTY"],
    }

    try:
        run_calculation(config=custom)
    except Exception:
        logger.exception("Unexpected error.")
        sys.exit(99)


if __name__ == "__main__":
    main()
