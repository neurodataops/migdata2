"""
transpile.py
============
CLI for the Redshift → Databricks SQL transpiler.

Usage:
  # Transpile a single file
  python transpile.py --input path/to/query.sql

  # Transpile a directory of SQL files
  python transpile.py --input path/to/sql_dir/

  # Transpile objects embedded in the source catalog
  python transpile.py --from-catalog

  # All modes use artifacts/source_catalog.json for context (optional)
  python transpile.py --input my.sql --catalog artifacts/source_catalog.json
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from tqdm import tqdm

from sql_transpiler_service import (
    Classification,
    transpile_catalog_objects,
    transpile_directory,
    transpile_file,
    transpile_sql,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
TRANSPILED_DIR = ARTIFACTS_DIR / "transpiled"
REPORT_PATH = ARTIFACTS_DIR / "convert_report.json"
CATALOG_PATH = ARTIFACTS_DIR / "source_catalog.json"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ARTIFACTS_DIR / "transpile.log", mode="w"),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_catalog(catalog_path: Path | None) -> dict | None:
    """Load source catalog if available."""
    path = catalog_path or CATALOG_PATH
    if path.exists():
        with open(path, "r", encoding="utf-8") as fp:
            catalog = json.load(fp)
        logger.info("Loaded catalog from %s", path)
        return catalog
    logger.info("No catalog found at %s — proceeding without catalog context.", path)
    return None


def _sanitise_filename(source_path: str) -> str:
    """Convert a source path to a safe output filename."""
    return source_path.replace("/", "__").replace("\\", "__").replace(" ", "_")


def _write_transpiled(results: list, output_dir: Path):
    """Write transpiled SQL files to output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for r in results:
        fname = _sanitise_filename(r.source_path)
        if not fname.endswith(".sql"):
            fname += ".sql"
        out_path = output_dir / fname

        header = (
            f"-- ============================================================\n"
            f"-- Source:         {r.source_path}\n"
            f"-- Classification: {r.classification.value}\n"
            f"-- Difficulty:     {r.difficulty_score}/10\n"
            f"-- Rules applied:  {len(r.applied_rules)}\n"
            f"-- Warnings:       {len(r.warnings)}\n"
        )
        if r.manual_reasons:
            header += f"-- MANUAL REWRITE: {'; '.join(r.manual_reasons)}\n"
        if r.warnings:
            for w in r.warnings:
                header += f"-- WARNING: {w}\n"
        header += (
            f"-- ============================================================\n\n"
        )

        out_path.write_text(header + r.transpiled_sql + "\n", encoding="utf-8")
        logger.debug("Wrote %s", out_path)

    logger.info("Wrote %d transpiled files to %s", len(results), output_dir)


def _write_report(results: list, report_path: Path):
    """Write convert_report.json."""
    objects = [r.to_dict() for r in results]

    counts = {c.value: 0 for c in Classification}
    total_difficulty = 0
    for r in results:
        counts[r.classification.value] += 1
        total_difficulty += r.difficulty_score

    avg_difficulty = round(total_difficulty / len(results), 2) if results else 0

    report = {
        "summary": {
            "total_objects": len(results),
            "auto_convert": counts[Classification.AUTO_CONVERT.value],
            "convert_with_warnings": counts[Classification.CONVERT_WITH_WARNINGS.value],
            "manual_rewrite_required": counts[Classification.MANUAL_REWRITE_REQUIRED.value],
            "avg_difficulty_score": avg_difficulty,
            "llm_assisted_count": sum(1 for r in results if r.llm_assisted),
        },
        "objects": objects,
    }

    with open(report_path, "w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2, default=str)
    logger.info("Conversion report written to %s", report_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(
    input_path: str | None = None,
    from_catalog: bool = False,
    catalog_path: str | None = None,
    output_dir: str | None = None,
):
    """Run the transpilation pipeline."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    out_dir = Path(output_dir) if output_dir else TRANSPILED_DIR

    cat_path = Path(catalog_path) if catalog_path else None
    catalog = _load_catalog(cat_path)
    all_results = []

    # ── Mode 1: From catalog ──────────────────────────────────────────────
    if from_catalog:
        if not catalog:
            logger.error("--from-catalog requires a valid source_catalog.json")
            sys.exit(1)
        logger.info("Transpiling objects from source catalog ...")
        results = transpile_catalog_objects(catalog)
        all_results.extend(results)

    # ── Mode 2: File or directory ─────────────────────────────────────────
    if input_path:
        p = Path(input_path)
        if p.is_file():
            logger.info("Transpiling single file: %s", p)
            result = transpile_file(p, catalog=catalog)
            all_results.append(result)
        elif p.is_dir():
            logger.info("Transpiling directory: %s", p)
            results = transpile_directory(p, catalog=catalog)
            all_results.extend(results)
        else:
            logger.error("Input path does not exist: %s", p)
            sys.exit(1)

    # ── Mode 3: stdin ─────────────────────────────────────────────────────
    if not input_path and not from_catalog:
        if not sys.stdin.isatty():
            logger.info("Reading SQL from stdin ...")
            sql = sys.stdin.read()
            result = transpile_sql(sql, source_path="<stdin>", catalog=catalog)
            all_results.append(result)
        else:
            logger.error(
                "No input specified. Use --input, --from-catalog, or pipe SQL via stdin."
            )
            sys.exit(1)

    if not all_results:
        logger.warning("No SQL objects found to transpile.")
        sys.exit(0)

    # ── Write outputs ─────────────────────────────────────────────────────
    logger.info("Writing %d transpiled objects ...", len(all_results))
    _write_transpiled(all_results, out_dir)
    _write_report(all_results, REPORT_PATH)

    # ── Recap ─────────────────────────────────────────────────────────────
    auto = sum(1 for r in all_results if r.classification == Classification.AUTO_CONVERT)
    warn = sum(1 for r in all_results if r.classification == Classification.CONVERT_WITH_WARNINGS)
    manual = sum(1 for r in all_results if r.classification == Classification.MANUAL_REWRITE_REQUIRED)
    llm = sum(1 for r in all_results if r.llm_assisted)

    logger.info("=" * 60)
    logger.info("Transpilation complete:")
    logger.info("  Total objects          : %d", len(all_results))
    logger.info("  AUTO_CONVERT           : %d", auto)
    logger.info("  CONVERT_WITH_WARNINGS  : %d", warn)
    logger.info("  MANUAL_REWRITE_REQUIRED: %d", manual)
    logger.info("  LLM-assisted           : %d", llm)
    logger.info("")
    logger.info("  Output directory       : %s", out_dir)
    logger.info("  Report                 : %s", REPORT_PATH)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Transpile Redshift SQL/DDL to Databricks-compatible SQL."
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        default=None,
        help="Path to a single SQL file or a directory of SQL files",
    )
    parser.add_argument(
        "--from-catalog",
        action="store_true",
        default=False,
        help="Transpile views, procs, and UDFs embedded in artifacts/source_catalog.json",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=None,
        help="Path to source_catalog.json (default: artifacts/source_catalog.json)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for transpiled files (default: artifacts/transpiled/)",
    )
    args = parser.parse_args()

    try:
        run(
            input_path=args.input,
            from_catalog=args.from_catalog,
            catalog_path=args.catalog,
            output_dir=args.output_dir,
        )
    except Exception:
        logger.exception("Unexpected error during transpilation.")
        sys.exit(99)


if __name__ == "__main__":
    main()
