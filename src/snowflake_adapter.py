"""
snowflake_adapter.py — Real Snowflake Source Adapter (implements SourceAdapter)
===============================================================================
Connects to a real Snowflake account and extracts catalog metadata and query
history using INFORMATION_SCHEMA and ACCOUNT_USAGE views.

Credentials come from config.yaml (source.snowflake section) or environment
variables as fallback.

Output key names match MockSourceAdapter exactly so app.py needs zero changes.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import snowflake.connector

from src.interfaces import SourceAdapter
from src.config import load_config, get_path, get_seed
from src.logger import get_logger

MOCK_DATA_DIR = get_path("mock_data")


class SnowflakeSourceAdapter(SourceAdapter):
    """Real Snowflake implementation of SourceAdapter."""

    def __init__(self, seed: int = None):
        self.seed = seed if seed is not None else get_seed()
        self.log = get_logger("snowflake_adapter", "snowflake_adapter.log")
        self.config = load_config()
        sf_cfg = self.config.get("source", {}).get("snowflake", {})

        self.account = sf_cfg.get("account") or os.environ.get("SNOWFLAKE_ACCOUNT", "")
        self.warehouse = sf_cfg.get("warehouse") or os.environ.get("SNOWFLAKE_WAREHOUSE", "")
        self.database = sf_cfg.get("database") or os.environ.get("SNOWFLAKE_DB", "")
        self.user = sf_cfg.get("user") or os.environ.get("SNOWFLAKE_USER", "")
        self.password = sf_cfg.get("password") or os.environ.get("SNOWFLAKE_PASSWORD", "")
        self.role = sf_cfg.get("role") or os.environ.get("SNOWFLAKE_ROLE", "")

        self.conn = None

    def _connect(self):
        if self.conn is None:
            self.conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                role=self.role,
                login_timeout=15,
            )
        return self.conn

    def _query(self, sql: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            cols = [desc[0].lower() for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def extract_catalog(self) -> dict:
        self.log.step("extract_catalog", "started", database=self.database)

        tables = self._extract_tables()
        columns = self._extract_columns()
        constraints = self._extract_constraints()
        procs = self._extract_procs()
        udfs = self._extract_udfs()
        views = self._extract_views()
        materialized_views = self._extract_materialized_views()

        catalog = {
            "tables": tables,
            "columns": columns,
            "constraints": constraints,
            "procs": procs,
            "udfs": udfs,
            "views": views,
            "materialized_views": materialized_views,
        }

        self.log.step("extract_catalog", "completed",
                       tables=len(tables),
                       columns=len(columns),
                       constraints=len(constraints),
                       procs=len(procs),
                       udfs=len(udfs),
                       views=len(views))
        return catalog

    def _extract_tables(self) -> list[dict]:
        rows = self._query(f"""
            SELECT
                table_schema,
                table_name,
                row_count,
                bytes
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('INFORMATION_SCHEMA')
            ORDER BY table_schema, table_name
        """)
        tables = []
        for r in rows:
            size_mb = round((r.get("bytes") or 0) / (1024 * 1024), 2)
            tables.append({
                "schema": (r.get("table_schema") or "").lower(),
                "table": (r.get("table_name") or "").lower(),
                "cluster_by": None,
                "auto_clustering": False,
                "retention_time": 1,
                "transient": False,
                "size_mb": size_mb,
                "rows_estimate": r.get("row_count") or 0,
            })
        return tables

    def _extract_columns(self) -> list[dict]:
        rows = self._query(f"""
            SELECT
                table_schema,
                table_name,
                column_name,
                ordinal_position,
                data_type,
                is_nullable,
                is_identity
            FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
            ORDER BY table_schema, table_name, ordinal_position
        """)
        columns = []
        for r in rows:
            columns.append({
                "schema": (r.get("table_schema") or "").lower(),
                "table": (r.get("table_name") or "").lower(),
                "column": (r.get("column_name") or "").lower(),
                "ordinal_position": r.get("ordinal_position", 0),
                "data_type": r.get("data_type") or "VARCHAR",
                "nullable": r.get("is_nullable") or "YES",
                "autoincrement": str(r.get("is_identity", "NO")).upper() == "YES",
            })
        return columns

    def _extract_constraints(self) -> list[dict]:
        try:
            rows = self._query(f"""
                SELECT
                    tc.table_schema,
                    tc.table_name,
                    tc.constraint_name,
                    tc.constraint_type,
                    kcu.column_name,
                    kcu.referenced_table_schema AS ref_schema,
                    kcu.referenced_table_name AS ref_table,
                    kcu.referenced_column_name AS ref_column
                FROM {self.database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                LEFT JOIN (
                    SELECT
                        constraint_name,
                        table_schema,
                        table_name,
                        column_name,
                        NULL AS referenced_table_schema,
                        NULL AS referenced_table_name,
                        NULL AS referenced_column_name
                    FROM {self.database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                ) kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                 AND tc.table_name = kcu.table_name
                WHERE tc.table_schema NOT IN ('INFORMATION_SCHEMA')
                ORDER BY tc.table_schema, tc.table_name, tc.constraint_name
            """)
        except Exception:
            # KEY_COLUMN_USAGE may not exist in some databases (e.g. SNOWFLAKE_SAMPLE_DATA);
            # fall back to TABLE_CONSTRAINTS only
            try:
                rows = self._query(f"""
                    SELECT
                        table_schema,
                        table_name,
                        constraint_name,
                        constraint_type
                    FROM {self.database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                    WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
                    ORDER BY table_schema, table_name, constraint_name
                """)
                rows = [{**r, "column_name": None, "ref_schema": None, "ref_table": None, "ref_column": None} for r in rows]
            except Exception:
                rows = []

        constraints = []
        for r in rows:
            constraints.append({
                "schema": (r.get("table_schema") or "").lower(),
                "table": (r.get("table_name") or "").lower(),
                "constraint_name": (r.get("constraint_name") or "").lower(),
                "constraint_type": r.get("constraint_type") or "",
                "column": (r.get("column_name") or "").lower() if r.get("column_name") else "",
                "ref_schema": (r.get("ref_schema") or "").lower() if r.get("ref_schema") else None,
                "ref_table": (r.get("ref_table") or "").lower() if r.get("ref_table") else None,
                "ref_column": (r.get("ref_column") or "").lower() if r.get("ref_column") else None,
            })
        return constraints

    def _extract_procs(self) -> list[dict]:
        try:
            rows = self._query(f"""
                SELECT
                    procedure_schema,
                    procedure_name,
                    procedure_definition,
                    procedure_language
                FROM {self.database}.INFORMATION_SCHEMA.PROCEDURES
                WHERE procedure_schema NOT IN ('INFORMATION_SCHEMA')
                ORDER BY procedure_schema, procedure_name
            """)
        except Exception:
            rows = []

        procs = []
        for r in rows:
            procs.append({
                "schema": (r.get("procedure_schema") or "").lower(),
                "name": (r.get("procedure_name") or "").lower(),
                "language": r.get("procedure_language") or "SQL",
                "arg_types": "various",
                "return_type": "VARCHAR",
                "source": r.get("procedure_definition") or "",
            })
        return procs

    def _extract_udfs(self) -> list[dict]:
        try:
            rows = self._query(f"""
                SELECT
                    function_schema,
                    function_name,
                    function_definition,
                    function_language,
                    data_type AS return_type
                FROM {self.database}.INFORMATION_SCHEMA.FUNCTIONS
                WHERE function_schema NOT IN ('INFORMATION_SCHEMA')
                ORDER BY function_schema, function_name
            """)
        except Exception:
            rows = []

        udfs = []
        for r in rows:
            udfs.append({
                "schema": (r.get("function_schema") or "").lower(),
                "name": (r.get("function_name") or "").lower(),
                "language": r.get("function_language") or "SQL",
                "return_type": r.get("return_type") or "VARCHAR",
                "source": r.get("function_definition") or "",
            })
        return udfs

    def _extract_views(self) -> list[dict]:
        try:
            rows = self._query(f"""
                SELECT
                    table_schema,
                    table_name,
                    view_definition
                FROM {self.database}.INFORMATION_SCHEMA.VIEWS
                WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
                ORDER BY table_schema, table_name
            """)
        except Exception:
            rows = []

        views = []
        for r in rows:
            views.append({
                "schema": (r.get("table_schema") or "").lower(),
                "name": (r.get("table_name") or "").lower(),
                "definition": r.get("view_definition") or "",
            })
        return views

    def _extract_materialized_views(self) -> list[dict]:
        try:
            rows = self._query("SHOW MATERIALIZED VIEWS")
            mvs = []
            for r in rows:
                mvs.append({
                    "schema": (r.get("schema_name") or r.get("database_name") or "").lower(),
                    "name": (r.get("name") or "").lower(),
                    "definition": r.get("text") or "",
                })
            return mvs
        except Exception:
            return []

    def extract_query_logs(self, catalog: dict) -> list[dict]:
        self.log.step("extract_query_logs", "started")
        try:
            rows = self._query("""
                SELECT
                    query_id,
                    user_name,
                    start_time,
                    end_time,
                    total_elapsed_time,
                    query_type,
                    query_tag,
                    query_text,
                    execution_status,
                    rows_produced
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE start_time >= DATEADD(day, -90, CURRENT_TIMESTAMP())
                ORDER BY start_time DESC
                LIMIT 5000
            """)
        except Exception as e:
            self.log.step("extract_query_logs", "warning",
                          message=f"Cannot access ACCOUNT_USAGE: {e}. Returning empty logs.")
            return []

        logs = []
        for r in rows:
            start = r.get("start_time")
            end = r.get("end_time")
            logs.append({
                "query_id": str(r.get("query_id", "")),
                "user": r.get("user_name") or "",
                "start_time": str(start) if start else "",
                "end_time": str(end) if end else "",
                "elapsed_ms": r.get("total_elapsed_time") or 0,
                "label": r.get("query_type") or "",
                "fingerprint": r.get("query_tag") or "",
                "sql": r.get("query_text") or "",
                "status": (r.get("execution_status") or "").lower(),
                "rows_returned": r.get("rows_produced") or 0,
            })

        self.log.step("extract_query_logs", "completed", entries=len(logs))
        return logs

    def save(self, catalog: dict, query_logs: list[dict]) -> dict:
        self.log.step("save", "started")
        MOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)

        catalog_path = MOCK_DATA_DIR / "source_catalog.json"
        catalog_path.write_text(json.dumps(catalog, indent=2, default=str), encoding="utf-8")

        logs_path = MOCK_DATA_DIR / "query_logs.json"
        logs_path.write_text(json.dumps(query_logs, indent=2, default=str), encoding="utf-8")

        self.log.step("save", "completed",
                       catalog_path=str(catalog_path),
                       logs_path=str(logs_path))

        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

        return {"catalog": str(catalog_path), "query_logs": str(logs_path)}
