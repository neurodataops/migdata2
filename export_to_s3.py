"""
export_to_s3.py
===============
Reads source_catalog.json and generates + executes Redshift UNLOAD commands
to export all tables as Parquet to S3.

Features:
  - Generates one UNLOAD per table to s3://<bucket>/redshift_export/<schema>/<table>/
  - Writes a manifest JSON listing every exported path
  - Supports IAM role-based access (preferred) or explicit AWS credentials
  - Dry-run mode to preview UNLOAD commands without executing
  - Parallel UNLOAD (Redshift default) for performance
  - Tracks export status per table in artifacts/export_manifest.json

Usage:
  python export_to_s3.py --bucket my-migration-bucket
  python export_to_s3.py --bucket my-bucket --prefix redshift_export --dry-run
  python export_to_s3.py --bucket my-bucket --iam-role arn:aws:iam::123456789012:role/RedshiftUnload
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
CATALOG_PATH = ARTIFACTS_DIR / "source_catalog.json"
MANIFEST_PATH = ARTIFACTS_DIR / "export_manifest.json"
UNLOAD_SCRIPTS_DIR = ARTIFACTS_DIR / "unload_scripts"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ARTIFACTS_DIR / "export_to_s3.log", mode="w"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _get_connection():
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


# ---------------------------------------------------------------------------
# UNLOAD command builder
# ---------------------------------------------------------------------------

def build_unload_sql(
    schema: str,
    table: str,
    bucket: str,
    prefix: str,
    iam_role: str | None = None,
    aws_access_key: str | None = None,
    aws_secret_key: str | None = None,
    max_file_size: str = "256 MB",
) -> str:
    """
    Build a Redshift UNLOAD command for a single table.

    Prefers IAM role-based auth. Falls back to explicit credentials.
    """
    s3_path = f"s3://{bucket}/{prefix}/{schema}/{table}/"
    select_stmt = f"SELECT * FROM {schema}.{table}"

    # Auth clause
    if iam_role:
        auth = f"IAM_ROLE '{iam_role}'"
    elif aws_access_key and aws_secret_key:
        auth = (
            f"ACCESS_KEY_ID '{aws_access_key}' "
            f"SECRET_ACCESS_KEY '{aws_secret_key}'"
        )
    else:
        auth = "IAM_ROLE '<YOUR_IAM_ROLE_ARN>'"

    sql = (
        f"UNLOAD ('{select_stmt}')\n"
        f"TO '{s3_path}'\n"
        f"{auth}\n"
        f"FORMAT AS PARQUET\n"
        f"MAXFILESIZE {max_file_size}\n"
        f"PARALLEL ON\n"
        f"ALLOWOVERWRITE;"
    )
    return sql


# ---------------------------------------------------------------------------
# Manifest builder
# ---------------------------------------------------------------------------

def build_manifest(tables: list[dict], bucket: str, prefix: str) -> dict:
    """Build a manifest listing all expected S3 paths."""
    entries = []
    for t in tables:
        schema = t.get("table_schema") or t.get("schema", "public")
        table = t.get("table_name") or t.get("table", "unknown")
        entries.append({
            "schema": schema,
            "table": table,
            "s3_path": f"s3://{bucket}/{prefix}/{schema}/{table}/",
            "format": "parquet",
            "row_estimate": t.get("row_estimate") or t.get("tbl_rows"),
            "size_mb": t.get("size_mb") or t.get("size"),
        })
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bucket": bucket,
        "prefix": prefix,
        "total_tables": len(entries),
        "tables": entries,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_export(
    bucket: str,
    prefix: str = "redshift_export",
    iam_role: str | None = None,
    dry_run: bool = False,
    max_file_size: str = "256 MB",
):
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    UNLOAD_SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

    # Load catalog
    if not CATALOG_PATH.exists():
        raise FileNotFoundError(
            f"Catalog not found at {CATALOG_PATH}. Run metadata_extractor.py first."
        )
    with open(CATALOG_PATH, "r", encoding="utf-8") as fp:
        catalog = json.load(fp)

    tables = catalog.get("tables", [])
    if not tables:
        logger.warning("No tables found in catalog.")
        return

    logger.info("Found %d tables in catalog.", len(tables))

    # Resolve credentials
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    # Build UNLOAD commands
    unload_commands = []
    for t in tables:
        schema = t.get("table_schema") or t.get("schema", "public")
        table = t.get("table_name") or t.get("table", "unknown")
        sql = build_unload_sql(
            schema=schema,
            table=table,
            bucket=bucket,
            prefix=prefix,
            iam_role=iam_role,
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            max_file_size=max_file_size,
        )
        unload_commands.append({
            "schema": schema,
            "table": table,
            "sql": sql,
            "s3_path": f"s3://{bucket}/{prefix}/{schema}/{table}/",
        })

    # Write all UNLOAD scripts to a single .sql file for reference
    all_sql_path = UNLOAD_SCRIPTS_DIR / "all_unloads.sql"
    with open(all_sql_path, "w", encoding="utf-8") as fp:
        fp.write(f"-- Generated UNLOAD commands: {len(unload_commands)} tables\n")
        fp.write(f"-- Bucket: s3://{bucket}/{prefix}/\n")
        fp.write(f"-- Generated at: {datetime.now(timezone.utc).isoformat()}\n\n")
        for cmd in unload_commands:
            fp.write(f"-- Table: {cmd['schema']}.{cmd['table']}\n")
            fp.write(cmd["sql"])
            fp.write("\n\n")
    logger.info("UNLOAD scripts written to %s", all_sql_path)

    # Write manifest
    manifest = build_manifest(tables, bucket, prefix)

    if dry_run:
        logger.info("DRY RUN — no UNLOAD commands executed.")
        manifest["mode"] = "dry_run"
        manifest["export_results"] = []
        with open(MANIFEST_PATH, "w", encoding="utf-8") as fp:
            json.dump(manifest, fp, indent=2, default=str)
        logger.info("Manifest written to %s", MANIFEST_PATH)
        logger.info("Review UNLOAD commands at %s", all_sql_path)
        return

    # Execute UNLOADs
    logger.info("Connecting to Redshift to execute UNLOAD commands ...")
    conn = _get_connection()
    cursor = conn.cursor()

    export_results = []
    for cmd in tqdm(unload_commands, desc="Exporting tables"):
        fqn = f"{cmd['schema']}.{cmd['table']}"
        result = {
            "schema": cmd["schema"],
            "table": cmd["table"],
            "s3_path": cmd["s3_path"],
            "status": "pending",
            "started_at": None,
            "completed_at": None,
            "error": None,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result["started_at"] = datetime.now(timezone.utc).isoformat()
                logger.info("  UNLOAD %s (attempt %d) ...", fqn, attempt)
                cursor.execute(cmd["sql"])
                result["completed_at"] = datetime.now(timezone.utc).isoformat()
                result["status"] = "success"
                logger.info("  UNLOAD %s completed.", fqn)
                break
            except psycopg2.OperationalError as exc:
                logger.warning(
                    "  UNLOAD %s attempt %d failed: %s", fqn, attempt, exc
                )
                if attempt == MAX_RETRIES:
                    result["status"] = "failed"
                    result["error"] = str(exc)
                else:
                    time.sleep(RETRY_DELAY_SECONDS * attempt)
            except psycopg2.Error as exc:
                logger.error("  UNLOAD %s failed: %s", fqn, exc)
                result["status"] = "failed"
                result["error"] = str(exc)
                break

        export_results.append(result)

    cursor.close()
    conn.close()

    # Enrich manifest with results
    manifest["mode"] = "executed"
    manifest["export_results"] = export_results
    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()

    succeeded = sum(1 for r in export_results if r["status"] == "success")
    failed = sum(1 for r in export_results if r["status"] == "failed")
    manifest["summary"] = {
        "total": len(export_results),
        "succeeded": succeeded,
        "failed": failed,
    }

    with open(MANIFEST_PATH, "w", encoding="utf-8") as fp:
        json.dump(manifest, fp, indent=2, default=str)
    logger.info("Manifest written to %s", MANIFEST_PATH)

    logger.info("=" * 60)
    logger.info("Export complete:")
    logger.info("  Total tables : %d", len(export_results))
    logger.info("  Succeeded    : %d", succeeded)
    logger.info("  Failed       : %d", failed)
    logger.info("  Manifest     : %s", MANIFEST_PATH)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Export Redshift tables to S3 as Parquet using UNLOAD."
    )
    parser.add_argument(
        "--bucket", "-b",
        required=True,
        help="S3 bucket name (without s3:// prefix)",
    )
    parser.add_argument(
        "--prefix",
        default="redshift_export",
        help="S3 key prefix under the bucket (default: redshift_export)",
    )
    parser.add_argument(
        "--iam-role",
        default=None,
        help="IAM role ARN for Redshift UNLOAD (recommended over explicit keys)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Generate UNLOAD scripts and manifest without executing",
    )
    parser.add_argument(
        "--max-file-size",
        default="256 MB",
        help="Max file size per Parquet file (default: '256 MB')",
    )
    args = parser.parse_args()

    try:
        run_export(
            bucket=args.bucket,
            prefix=args.prefix,
            iam_role=args.iam_role,
            dry_run=args.dry_run,
            max_file_size=args.max_file_size,
        )
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
    except psycopg2.OperationalError as exc:
        logger.error("Could not connect to Redshift: %s", exc)
        sys.exit(2)
    except Exception:
        logger.exception("Unexpected error during export.")
        sys.exit(99)


if __name__ == "__main__":
    main()
