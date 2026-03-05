"""
logger.py — Structured JSON logging
=====================================
Provides a consistent logger that writes structured JSON log lines
to both console and per-step log files in artifacts/logs/.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from src.config import get_path, get_log_level, get_json_logs


class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = self.formatException(record.exc_info)
        # Merge any extra structured data
        if hasattr(record, "data") and record.data:
            entry["data"] = record.data
        return json.dumps(entry, default=str)


class StructuredLogger:
    """Wrapper providing .log() with structured data and .step() for pipeline events."""

    def __init__(self, name: str, log_file: str = None):
        self.name = name
        self._logger = logging.getLogger(name)
        self._logger.setLevel(get_log_level())
        self._logger.handlers.clear()
        self._logger.propagate = False

        use_json = get_json_logs()
        formatter = JsonFormatter() if use_json else logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s %(message)s"
        )

        # Console handler
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(get_log_level())
        console.setFormatter(formatter)
        self._logger.addHandler(console)

        # File handler
        if log_file:
            logs_dir = get_path("logs")
            fh = logging.FileHandler(logs_dir / log_file, encoding="utf-8")
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(JsonFormatter())  # always JSON in files
            self._logger.addHandler(fh)

        self._steps: list[dict] = []

    def info(self, msg: str, **data):
        record = self._logger.makeRecord(
            self.name, logging.INFO, "", 0, msg, (), None
        )
        record.data = data if data else None
        self._logger.handle(record)

    def warning(self, msg: str, **data):
        record = self._logger.makeRecord(
            self.name, logging.WARNING, "", 0, msg, (), None
        )
        record.data = data if data else None
        self._logger.handle(record)

    def error(self, msg: str, **data):
        record = self._logger.makeRecord(
            self.name, logging.ERROR, "", 0, msg, (), None
        )
        record.data = data if data else None
        self._logger.handle(record)

    def step(self, step_name: str, status: str = "started", **data):
        """Log a pipeline step event with structured data."""
        entry = {
            "step": step_name,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **data,
        }
        self._steps.append(entry)
        self.info(f"[{step_name}] {status}", **data)

    def get_steps(self) -> list[dict]:
        return list(self._steps)


def get_logger(name: str, log_file: str = None) -> StructuredLogger:
    """Factory function for creating structured loggers."""
    return StructuredLogger(name, log_file)
