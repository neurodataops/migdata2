"""
interfaces.py — Abstract base classes for pluggable adapters
=============================================================
Real Redshift/Databricks implementations can replace mocks by
subclassing these ABCs and registering via config.yaml.

    source.adapter: "mock"    → MockSourceAdapter
    source.adapter: "redshift" → RedshiftSourceAdapter (implement later)
"""

from abc import ABC, abstractmethod
from typing import Any


class SourceAdapter(ABC):
    """Extract metadata and query history from a source warehouse."""

    @abstractmethod
    def extract_catalog(self) -> dict:
        """
        Return a catalog dict with keys:
            tables, columns, constraints, procs, udfs, views, materialized_views
        Each value is a list of dicts.
        """
        ...

    @abstractmethod
    def extract_query_logs(self, catalog: dict) -> list[dict]:
        """
        Return a list of query log entries, each with:
            query_id, user, start_time, end_time, elapsed_ms,
            label, fingerprint, sql, status, rows_returned
        """
        ...

    @abstractmethod
    def save(self, catalog: dict, query_logs: list[dict]) -> dict:
        """Persist outputs. Return dict of written file paths."""
        ...


class ConversionEngine(ABC):
    """Transpile SQL from source dialect to target dialect."""

    @abstractmethod
    def transpile(self, source_sql: str, object_name: str = "") -> dict:
        """
        Transpile a single SQL string. Return dict with:
            object_name, classification, difficulty, applied_rules,
            warnings, manual_flags, source_sql, transpiled_sql, diff, sha256
        """
        ...

    @abstractmethod
    def run_full_conversion(self, catalog: dict) -> dict:
        """
        Transpile all objects from catalog. Return a conversion report dict with:
            summary: {total_objects, classifications, total_warnings, ...}
            objects: [per-object results]
            flagged_manual_items: [...]
        """
        ...

    @abstractmethod
    def save(self, report: dict) -> dict:
        """Persist outputs. Return dict of written file paths."""
        ...


class DataLoader(ABC):
    """Load data from source format into target format."""

    @abstractmethod
    def load_table(self, schema: str, table: str,
                   columns: list[dict], rows_estimate: int) -> dict:
        """
        Load a single table. Return dict with:
            schema, table, fqn, rows_loaded, file_count, file_size_kb,
            partition_column, schema_mismatches, runtime_seconds, status, error
        """
        ...

    @abstractmethod
    def run_full_load(self, catalog: dict) -> dict:
        """
        Load all tables from catalog. Return a load summary dict with:
            summary: {tables_loaded, total_rows, ...}
            tables: [per-table load records]
        """
        ...

    @abstractmethod
    def save(self, summary: dict) -> dict:
        """Persist outputs. Return dict of written file paths."""
        ...


class ValidationEngine(ABC):
    """Validate data parity between source and target."""

    @abstractmethod
    def validate_table(self, table_fqn: str,
                       source_meta: dict, target_path: Any) -> dict:
        """
        Validate a single table. Return dict with:
            table, status, checks: [{check, passed, detail, ...}],
            confidence, difficulty
        """
        ...

    @abstractmethod
    def run_full_validation(self, catalog: dict,
                            load_summary: dict,
                            conversion_report: dict) -> dict:
        """
        Validate all tables. Return a validation results dict with:
            summary: {tables_validated, total_checks, passed, failed, pass_rate, ...}
            tables: [per-table results]
        """
        ...

    @abstractmethod
    def save(self, results: dict) -> dict:
        """Persist outputs. Return dict of written file paths."""
        ...


class TestRunner(ABC):
    """Generate and execute migration tests."""

    @abstractmethod
    def run(self) -> dict:
        """
        Execute all test suites. Return dict with:
            total, passed, failed, pass_rate, suites: {name: [results]}
        """
        ...

    @abstractmethod
    def save(self, results: dict) -> dict:
        """Persist outputs. Return dict of written file paths."""
        ...
