"""
workload_analyzer.py
====================
Connects to AWS Redshift and analyses query workload from system logging tables:
  - Daily query volume (last N days)
  - Top N longest-running queries
  - Query fingerprint frequency (normalized via literal stripping + MD5)
  - Complexity heuristics (joins, window functions, subqueries, UDF usage)

Outputs:
  artifacts/workload_summary.json   — per-query metrics & fingerprint stats
  artifacts/top_queries.csv         — top longest-running queries
  artifacts/query_volume.html       — interactive Plotly timeline
  artifacts/owner_mapping.json      — query-to-owner mapping by user/schema
  artifacts/needs_manual_owner_mapping.csv — queries where ownership is uncertain
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import psycopg2
import psycopg2.extras
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Optional: Plotly (for HTML chart)
# ---------------------------------------------------------------------------
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
WORKLOAD_SUMMARY_PATH = ARTIFACTS_DIR / "workload_summary.json"
TOP_QUERIES_PATH = ARTIFACTS_DIR / "top_queries.csv"
QUERY_VOLUME_HTML_PATH = ARTIFACTS_DIR / "query_volume.html"
OWNER_MAPPING_PATH = ARTIFACTS_DIR / "owner_mapping.json"
MANUAL_OWNER_PATH = ARTIFACTS_DIR / "needs_manual_owner_mapping.csv"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

DEFAULT_LOOKBACK_DAYS = 90
DEFAULT_TOP_N = 50

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ARTIFACTS_DIR / "workload_analyzer.log", mode="w"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redshift SQL Queries
# ---------------------------------------------------------------------------

SQL_DAILY_QUERY_COUNTS = """
SELECT
    DATE(starttime) AS dt,
    COUNT(*)        AS num_queries
FROM stl_query
WHERE starttime >= CURRENT_DATE - %(lookback_days)s
  AND userid > 1
GROUP BY DATE(starttime)
ORDER BY dt;
"""

SQL_TOP_LONGEST_QUERIES = """
SELECT
    q.query        AS query_id,
    q.userid,
    q.starttime,
    q.endtime,
    DATEDIFF(millisecond, q.starttime, q.endtime) AS elapsed_ms,
    q.label,
    q.aborted,
    LISTAGG(qt.text) WITHIN GROUP (ORDER BY qt.sequence) AS query_text
FROM stl_query q
JOIN stl_querytext qt ON q.query = qt.query
WHERE q.starttime >= CURRENT_DATE - %(lookback_days)s
  AND q.userid > 1
GROUP BY q.query, q.userid, q.starttime, q.endtime, q.label, q.aborted
ORDER BY elapsed_ms DESC
LIMIT %(top_n)s;
"""

SQL_ALL_QUERIES_FOR_FINGERPRINT = """
SELECT
    q.query        AS query_id,
    q.starttime,
    q.endtime,
    DATEDIFF(millisecond, q.starttime, q.endtime) AS elapsed_ms,
    LISTAGG(qt.text) WITHIN GROUP (ORDER BY qt.sequence) AS query_text
FROM stl_query q
JOIN stl_querytext qt ON q.query = qt.query
WHERE q.starttime >= CURRENT_DATE - %(lookback_days)s
  AND q.userid > 1
GROUP BY q.query, q.starttime, q.endtime
ORDER BY q.starttime;
"""

SQL_KNOWN_UDF_SCHEMAS = """
SELECT DISTINCT
    n.nspname AS udf_schema,
    p.proname AS udf_name
FROM pg_proc_info p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
ORDER BY n.nspname, p.proname;
"""

SQL_QUERY_USER_SCHEMA = """
SELECT
    q.query                  AS query_id,
    u.usename                AS username,
    q.userid,
    BTRIM(qt.text)           AS query_text,
    q.starttime,
    DATEDIFF(millisecond, q.starttime, q.endtime) AS elapsed_ms
FROM stl_query q
JOIN stl_querytext qt ON q.query = qt.query AND qt.sequence = 0
JOIN pg_user u        ON q.userid = u.usesysid
WHERE q.starttime >= CURRENT_DATE - %(lookback_days)s
  AND q.userid > 1
ORDER BY q.starttime;
"""

SQL_USER_SCHEMA_ACTIVITY = """
SELECT
    u.usename            AS username,
    q.userid,
    s.nspname            AS search_path_schema,
    COUNT(*)             AS query_count
FROM stl_query q
JOIN pg_user u      ON q.userid = u.usesysid
LEFT JOIN pg_namespace s
    ON s.nspname = SPLIT_PART(
        COALESCE(NULLIF(u.usecreatedb::text,''), 'public'),
        ',', 1
    )
WHERE q.starttime >= CURRENT_DATE - %(lookback_days)s
  AND q.userid > 1
GROUP BY u.usename, q.userid, s.nspname
ORDER BY query_count DESC;
"""

# ---------------------------------------------------------------------------
# Connection helper
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
# Query fingerprinting
# ---------------------------------------------------------------------------

# Patterns to strip when normalizing query text
_RE_SINGLE_QUOTED = re.compile(r"'[^']*'")
_RE_NUMBERS = re.compile(r"\b\d+\.?\d*\b")
_RE_WHITESPACE = re.compile(r"\s+")
_RE_IN_LIST = re.compile(r"\bIN\s*\(\s*\?\s*(?:,\s*\?\s*)*\)", re.IGNORECASE)


def normalize_query(sql_text: str) -> str:
    """Strip literals and collapse whitespace to produce a canonical form."""
    text = sql_text.strip().lower()
    # Replace string literals with placeholder
    text = _RE_SINGLE_QUOTED.sub("?", text)
    # Replace numeric literals with placeholder
    text = _RE_NUMBERS.sub("?", text)
    # Collapse IN-lists of placeholders
    text = _RE_IN_LIST.sub("IN (?)", text)
    # Collapse whitespace
    text = _RE_WHITESPACE.sub(" ", text).strip()
    return text


def fingerprint(sql_text: str) -> str:
    """Return MD5 hex digest of the normalized query."""
    normalized = normalize_query(sql_text)
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Complexity heuristics
# ---------------------------------------------------------------------------

_RE_JOIN = re.compile(r"\bjoin\b", re.IGNORECASE)
_RE_WINDOW = re.compile(r"\bover\s*\(", re.IGNORECASE)
_RE_SUBQUERY = re.compile(r"\bselect\b", re.IGNORECASE)
_RE_CTE = re.compile(r"\bwith\b\s+\w+\s+as\s*\(", re.IGNORECASE)
_RE_UNION = re.compile(r"\bunion\b", re.IGNORECASE)
_RE_GROUP_BY = re.compile(r"\bgroup\s+by\b", re.IGNORECASE)
_RE_ORDER_BY = re.compile(r"\border\s+by\b", re.IGNORECASE)
_RE_CASE = re.compile(r"\bcase\b", re.IGNORECASE)
_RE_DISTINCT = re.compile(r"\bdistinct\b", re.IGNORECASE)


def compute_complexity(sql_text: str, known_udfs: set | None = None) -> dict:
    """
    Return a dict of complexity heuristics for a single query.

    Metrics:
        joins           – count of JOIN keywords
        window_functions – count of OVER( patterns
        subqueries      – count of SELECT keywords minus 1 (the outer SELECT)
        ctes            – count of WITH ... AS ( patterns
        unions          – count of UNION keywords
        group_bys       – count of GROUP BY clauses
        order_bys       – count of ORDER BY clauses
        case_exprs      – count of CASE keywords
        has_distinct    – boolean
        uses_udfs       – list of matched UDF names (if known_udfs provided)
        char_length     – length of query text
        complexity_score – weighted composite score
    """
    text = sql_text or ""
    joins = len(_RE_JOIN.findall(text))
    windows = len(_RE_WINDOW.findall(text))
    selects = len(_RE_SUBQUERY.findall(text))
    subqueries = max(0, selects - 1)
    ctes = len(_RE_CTE.findall(text))
    unions = len(_RE_UNION.findall(text))
    group_bys = len(_RE_GROUP_BY.findall(text))
    order_bys = len(_RE_ORDER_BY.findall(text))
    case_exprs = len(_RE_CASE.findall(text))
    has_distinct = bool(_RE_DISTINCT.search(text))

    # UDF detection
    matched_udfs = []
    if known_udfs:
        text_lower = text.lower()
        for udf_name in known_udfs:
            if udf_name.lower() in text_lower:
                matched_udfs.append(udf_name)

    # Weighted complexity score
    score = (
        joins * 2
        + windows * 3
        + subqueries * 3
        + ctes * 2
        + unions * 2
        + group_bys * 1
        + order_bys * 1
        + case_exprs * 1
        + (1 if has_distinct else 0)
        + len(matched_udfs) * 2
        + (1 if len(text) > 2000 else 0)
    )

    return {
        "joins": joins,
        "window_functions": windows,
        "subqueries": subqueries,
        "ctes": ctes,
        "unions": unions,
        "group_bys": group_bys,
        "order_bys": order_bys,
        "case_expressions": case_exprs,
        "has_distinct": has_distinct,
        "uses_udfs": matched_udfs,
        "char_length": len(text),
        "complexity_score": score,
    }


# ---------------------------------------------------------------------------
# Extraction steps
# ---------------------------------------------------------------------------

def fetch_daily_counts(cursor, lookback_days):
    """Fetch daily query counts."""
    logger.info("Fetching daily query counts (last %d days) ...", lookback_days)
    rows = _execute_with_retry(
        cursor,
        SQL_DAILY_QUERY_COUNTS,
        {"lookback_days": lookback_days},
        "daily_counts",
    )
    results = _rows_to_dicts(cursor, rows)
    logger.info("  Retrieved %d daily count records.", len(results))
    return results


def fetch_top_queries(cursor, lookback_days, top_n):
    """Fetch top N longest-running queries with full text."""
    logger.info("Fetching top %d longest-running queries ...", top_n)
    rows = _execute_with_retry(
        cursor,
        SQL_TOP_LONGEST_QUERIES,
        {"lookback_days": lookback_days, "top_n": top_n},
        "top_queries",
    )
    results = _rows_to_dicts(cursor, rows)
    logger.info("  Retrieved %d queries.", len(results))
    return results


def fetch_all_queries(cursor, lookback_days):
    """Fetch all queries for fingerprinting and complexity analysis."""
    logger.info("Fetching all queries for fingerprinting (last %d days) ...", lookback_days)
    rows = _execute_with_retry(
        cursor,
        SQL_ALL_QUERIES_FOR_FINGERPRINT,
        {"lookback_days": lookback_days},
        "all_queries",
    )
    results = _rows_to_dicts(cursor, rows)
    logger.info("  Retrieved %d total queries.", len(results))
    return results


def fetch_known_udfs(cursor):
    """Fetch known UDF/procedure names from the cluster."""
    logger.info("Fetching known UDF names for complexity detection ...")
    try:
        rows = _execute_with_retry(cursor, SQL_KNOWN_UDF_SCHEMAS, label="known_udfs")
        results = _rows_to_dicts(cursor, rows)
        udf_names = set()
        for row in results:
            udf_names.add(row["udf_name"])
            udf_names.add(f"{row['udf_schema']}.{row['udf_name']}")
        logger.info("  Loaded %d UDF identifiers.", len(udf_names))
        return udf_names
    except psycopg2.Error as exc:
        logger.warning("Could not fetch UDFs: %s", exc)
        return set()


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def build_fingerprint_stats(all_queries, known_udfs):
    """
    Group queries by fingerprint, compute frequency, avg elapsed,
    and attach complexity heuristics.
    """
    logger.info("Computing fingerprints and complexity heuristics ...")
    fp_groups = defaultdict(lambda: {
        "count": 0,
        "total_elapsed_ms": 0,
        "min_elapsed_ms": float("inf"),
        "max_elapsed_ms": 0,
        "sample_query_text": None,
        "sample_query_id": None,
        "first_seen": None,
        "last_seen": None,
    })

    for q in tqdm(all_queries, desc="Fingerprinting queries"):
        text = q.get("query_text") or ""
        fp = fingerprint(text)
        g = fp_groups[fp]
        elapsed = int(q.get("elapsed_ms") or 0)

        g["count"] += 1
        g["total_elapsed_ms"] += elapsed
        g["min_elapsed_ms"] = min(g["min_elapsed_ms"], elapsed)
        g["max_elapsed_ms"] = max(g["max_elapsed_ms"], elapsed)

        if g["sample_query_text"] is None:
            g["sample_query_text"] = text[:4000]
            g["sample_query_id"] = q.get("query_id")

        start_str = str(q.get("starttime", ""))
        if g["first_seen"] is None or start_str < g["first_seen"]:
            g["first_seen"] = start_str
        if g["last_seen"] is None or start_str > g["last_seen"]:
            g["last_seen"] = start_str

    # Enrich with complexity and averages
    fingerprint_list = []
    for fp, g in tqdm(fp_groups.items(), desc="Computing complexity"):
        avg_elapsed = g["total_elapsed_ms"] / g["count"] if g["count"] else 0
        complexity = compute_complexity(g["sample_query_text"] or "", known_udfs)

        fingerprint_list.append({
            "fingerprint": fp,
            "execution_count": g["count"],
            "total_elapsed_ms": g["total_elapsed_ms"],
            "avg_elapsed_ms": round(avg_elapsed, 2),
            "min_elapsed_ms": g["min_elapsed_ms"] if g["min_elapsed_ms"] != float("inf") else 0,
            "max_elapsed_ms": g["max_elapsed_ms"],
            "first_seen": g["first_seen"],
            "last_seen": g["last_seen"],
            "sample_query_id": g["sample_query_id"],
            "sample_query_text": g["sample_query_text"],
            "complexity": complexity,
        })

    # Sort by total elapsed descending
    fingerprint_list.sort(key=lambda x: x["total_elapsed_ms"], reverse=True)
    logger.info("  Produced %d unique fingerprints.", len(fingerprint_list))
    return fingerprint_list


# ---------------------------------------------------------------------------
# Owner mapping
# ---------------------------------------------------------------------------

_RE_SCHEMA_REF = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([a-z_]\w*\.[a-z_]\w*)",
    re.IGNORECASE,
)

CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_LOW = "low"


def _extract_schemas_from_sql(sql_text: str) -> set:
    """Extract schema names referenced in FROM/JOIN/INTO clauses."""
    schemas = set()
    for match in _RE_SCHEMA_REF.findall(sql_text or ""):
        schema_part = match.split(".")[0].lower()
        if schema_part not in ("pg_catalog", "information_schema", "pg_internal"):
            schemas.add(schema_part)
    return schemas


def fetch_query_owners(cursor, lookback_days):
    """Fetch query-to-user associations."""
    logger.info("Fetching query-to-user associations ...")
    try:
        rows = _execute_with_retry(
            cursor,
            SQL_QUERY_USER_SCHEMA,
            {"lookback_days": lookback_days},
            "query_user_schema",
        )
        results = _rows_to_dicts(cursor, rows)
        logger.info("  Retrieved %d query-user records.", len(results))
        return results
    except psycopg2.Error as exc:
        logger.warning("Could not fetch query-user mapping: %s", exc)
        return []


def build_owner_mapping(all_queries_with_users, fingerprint_stats):
    """
    Map each query fingerprint to candidate owners (username + schemas touched).

    Confidence levels:
      high   — single user runs >80% of executions for this fingerprint
      medium — dominant user runs 50-80%
      low    — no dominant user, or multiple users with similar counts

    Returns (owner_mapping dict, needs_manual list).
    """
    logger.info("Building owner mapping ...")

    # Group queries by fingerprint -> user counts + schemas
    fp_users = defaultdict(lambda: defaultdict(int))
    fp_schemas = defaultdict(set)

    for q in tqdm(all_queries_with_users, desc="Mapping owners"):
        text = q.get("query_text") or ""
        fp = fingerprint(text)
        username = q.get("username", "unknown")
        fp_users[fp][username] += 1
        fp_schemas[fp].update(_extract_schemas_from_sql(text))

    owner_mapping = {}
    needs_manual = []

    for fp_entry in fingerprint_stats:
        fp = fp_entry["fingerprint"]
        user_counts = fp_users.get(fp, {})
        schemas = sorted(fp_schemas.get(fp, set()))
        total_runs = sum(user_counts.values())

        if total_runs == 0:
            # No user data available
            needs_manual.append({
                "fingerprint": fp,
                "execution_count": fp_entry["execution_count"],
                "candidate_owners": [],
                "schemas_referenced": schemas,
                "confidence": CONFIDENCE_LOW,
                "reason": "no_user_data_available",
                "sample_query_text": (fp_entry.get("sample_query_text") or "")[:500],
            })
            continue

        # Sort users by query count descending
        sorted_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)
        top_user, top_count = sorted_users[0]
        top_pct = (top_count / total_runs) * 100

        if top_pct >= 80:
            confidence = CONFIDENCE_HIGH
        elif top_pct >= 50:
            confidence = CONFIDENCE_MEDIUM
        else:
            confidence = CONFIDENCE_LOW

        mapping_entry = {
            "fingerprint": fp,
            "execution_count": fp_entry["execution_count"],
            "candidate_owners": [
                {"username": u, "run_count": c, "pct": round((c / total_runs) * 100, 1)}
                for u, c in sorted_users
            ],
            "primary_owner": top_user,
            "primary_owner_pct": round(top_pct, 1),
            "schemas_referenced": schemas,
            "confidence": confidence,
        }
        owner_mapping[fp] = mapping_entry

        if confidence == CONFIDENCE_LOW:
            needs_manual.append({
                **mapping_entry,
                "reason": "no_dominant_user",
                "sample_query_text": (fp_entry.get("sample_query_text") or "")[:500],
            })

    logger.info(
        "  Owner mapping: %d mapped, %d need manual review.",
        len(owner_mapping),
        len(needs_manual),
    )
    return owner_mapping, needs_manual


def write_owner_mapping(owner_mapping, needs_manual):
    """Write owner_mapping.json and needs_manual_owner_mapping.csv."""
    # JSON — full mapping
    with open(OWNER_MAPPING_PATH, "w", encoding="utf-8") as fp:
        json.dump(_safe_json(owner_mapping), fp, indent=2, default=str)
    logger.info("Owner mapping written to %s", OWNER_MAPPING_PATH)

    # CSV — manual review needed
    if not needs_manual:
        logger.info("No queries need manual owner mapping.")
        return

    fieldnames = [
        "fingerprint",
        "execution_count",
        "confidence",
        "reason",
        "candidate_owners",
        "schemas_referenced",
        "sample_query_text",
    ]
    with open(MANUAL_OWNER_PATH, "w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for entry in needs_manual:
            row = {**entry}
            # Flatten lists for CSV readability
            row["candidate_owners"] = "; ".join(
                f"{o['username']}({o['run_count']})" for o in entry.get("candidate_owners", [])
            )
            row["schemas_referenced"] = ", ".join(entry.get("schemas_referenced", []))
            writer.writerow(row)
    logger.info(
        "Manual owner mapping report (%d entries) written to %s",
        len(needs_manual),
        MANUAL_OWNER_PATH,
    )


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_workload_summary(daily_counts, fingerprint_stats, top_queries, owner_mapping=None):
    """Write the main workload_summary.json."""
    needs_manual_count = 0
    if owner_mapping:
        needs_manual_count = sum(
            1 for v in owner_mapping.values() if v.get("confidence") == CONFIDENCE_LOW
        )

    payload = {
        "daily_query_counts": _safe_json(daily_counts),
        "fingerprint_stats": _safe_json(fingerprint_stats),
        "top_queries": _safe_json(top_queries),
        "owner_mapping": _safe_json(owner_mapping or {}),
        "summary": {
            "total_days": len(daily_counts),
            "total_queries_observed": sum(d.get("num_queries", 0) for d in daily_counts),
            "unique_fingerprints": len(fingerprint_stats),
            "top_queries_returned": len(top_queries),
            "owner_mappings": len(owner_mapping) if owner_mapping else 0,
            "needs_manual_owner_review": needs_manual_count,
        },
    }
    with open(WORKLOAD_SUMMARY_PATH, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2, default=str)
    logger.info("Workload summary written to %s", WORKLOAD_SUMMARY_PATH)


def write_top_queries_csv(top_queries):
    """Write top_queries.csv."""
    if not top_queries:
        logger.warning("No top queries to write.")
        return

    fieldnames = [
        "query_id",
        "userid",
        "starttime",
        "endtime",
        "elapsed_ms",
        "label",
        "aborted",
        "query_text",
        "fingerprint",
        "complexity_score",
        "joins",
        "window_functions",
        "subqueries",
    ]
    with open(TOP_QUERIES_PATH, "w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for q in top_queries:
            text = q.get("query_text") or ""
            complexity = compute_complexity(text)
            row = {
                **q,
                "fingerprint": fingerprint(text),
                "complexity_score": complexity["complexity_score"],
                "joins": complexity["joins"],
                "window_functions": complexity["window_functions"],
                "subqueries": complexity["subqueries"],
            }
            writer.writerow(row)
    logger.info("Top queries CSV written to %s", TOP_QUERIES_PATH)


def write_query_volume_html(daily_counts):
    """Write interactive Plotly HTML timeline of daily query volume."""
    if not HAS_PLOTLY:
        logger.warning(
            "Plotly not installed — skipping query_volume.html generation. "
            "Install with: pip install plotly"
        )
        _write_fallback_html(daily_counts)
        return

    dates = [str(d.get("dt", "")) for d in daily_counts]
    counts = [int(d.get("num_queries", 0)) for d in daily_counts]

    fig = make_subplots(
        rows=2,
        cols=1,
        subplot_titles=("Daily Query Volume", "7-Day Rolling Average"),
        vertical_spacing=0.12,
    )

    # Raw daily counts
    fig.add_trace(
        go.Bar(
            x=dates,
            y=counts,
            name="Daily queries",
            marker_color="steelblue",
        ),
        row=1,
        col=1,
    )

    # 7-day rolling average
    rolling = []
    for i in range(len(counts)):
        window = counts[max(0, i - 6) : i + 1]
        rolling.append(round(sum(window) / len(window), 1))

    fig.add_trace(
        go.Scatter(
            x=dates,
            y=rolling,
            name="7-day avg",
            line=dict(color="firebrick", width=2),
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        title_text="Redshift Query Volume Analysis",
        height=700,
        showlegend=True,
        template="plotly_white",
    )
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Query Count", row=1, col=1)
    fig.update_yaxes(title_text="Avg Count", row=2, col=1)

    fig.write_html(str(QUERY_VOLUME_HTML_PATH), include_plotlyjs="cdn")
    logger.info("Query volume HTML written to %s", QUERY_VOLUME_HTML_PATH)


def _write_fallback_html(daily_counts):
    """Write a minimal standalone HTML chart when Plotly is unavailable."""
    dates = [str(d.get("dt", "")) for d in daily_counts]
    counts = [int(d.get("num_queries", 0)) for d in daily_counts]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Redshift Query Volume</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
<h1>Redshift Query Volume (last {len(dates)} days)</h1>
<div id="chart" style="width:100%;height:500px;"></div>
<script>
var dates = {json.dumps(dates)};
var counts = {json.dumps(counts)};
Plotly.newPlot('chart', [{{
    x: dates,
    y: counts,
    type: 'bar',
    marker: {{color: 'steelblue'}}
}}], {{
    title: 'Daily Query Volume',
    xaxis: {{title: 'Date'}},
    yaxis: {{title: 'Queries'}}
}});
</script>
</body>
</html>"""
    with open(QUERY_VOLUME_HTML_PATH, "w", encoding="utf-8") as fp:
        fp.write(html)
    logger.info("Fallback query volume HTML written to %s", QUERY_VOLUME_HTML_PATH)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_analysis(lookback_days: int = DEFAULT_LOOKBACK_DAYS, top_n: int = DEFAULT_TOP_N):
    """Run the full workload analysis pipeline."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Connecting to Redshift ...")
    conn = _get_connection()
    cursor = conn.cursor()
    logger.info("Connected successfully.")

    steps = [
        "Fetch daily counts",
        "Fetch top queries",
        "Fetch all queries",
        "Fetch known UDFs",
        "Fetch query owners",
        "Build fingerprint stats",
        "Build owner mapping",
        "Write outputs",
    ]
    pbar = tqdm(steps, desc="Workload analysis")

    # Step 1 — Daily counts
    pbar.set_description("Fetching daily counts")
    daily_counts = fetch_daily_counts(cursor, lookback_days)
    next(iter([pbar.update(1)]))

    # Step 2 — Top queries
    pbar.set_description("Fetching top queries")
    top_queries = fetch_top_queries(cursor, lookback_days, top_n)
    pbar.update(1)

    # Step 3 — All queries for fingerprinting
    pbar.set_description("Fetching all queries")
    all_queries = fetch_all_queries(cursor, lookback_days)
    pbar.update(1)

    # Step 4 — Known UDFs
    pbar.set_description("Fetching UDF list")
    known_udfs = fetch_known_udfs(cursor)
    pbar.update(1)

    # Step 5 — Query-to-user associations
    pbar.set_description("Fetching query owners")
    query_owners = fetch_query_owners(cursor, lookback_days)
    pbar.update(1)

    cursor.close()
    conn.close()
    logger.info("Redshift connection closed.")

    # Step 6 — Fingerprint + complexity
    pbar.set_description("Building fingerprint stats")
    fingerprint_stats = build_fingerprint_stats(all_queries, known_udfs)
    pbar.update(1)

    # Step 7 — Owner mapping
    pbar.set_description("Building owner mapping")
    owner_mapping, needs_manual = build_owner_mapping(query_owners, fingerprint_stats)
    pbar.update(1)

    # Step 8 — Write outputs
    pbar.set_description("Writing outputs")
    write_workload_summary(daily_counts, fingerprint_stats, top_queries, owner_mapping)
    write_top_queries_csv(top_queries)
    write_query_volume_html(daily_counts)
    write_owner_mapping(owner_mapping, needs_manual)
    pbar.update(1)
    pbar.close()

    # Console recap
    total_q = sum(d.get("num_queries", 0) for d in daily_counts)
    logger.info("=" * 60)
    logger.info("Workload analysis complete:")
    logger.info("  Days analysed            : %d", len(daily_counts))
    logger.info("  Total queries            : %d", total_q)
    logger.info("  Unique fingerprints      : %d", len(fingerprint_stats))
    logger.info("  Top queries returned     : %d", len(top_queries))
    logger.info("  Owner mappings           : %d", len(owner_mapping))
    logger.info("  Needs manual owner review: %d", len(needs_manual))
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Analyse Redshift query workload for migration planning."
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Number of top longest queries to retrieve (default: {DEFAULT_TOP_N})",
    )
    args = parser.parse_args()

    try:
        run_analysis(lookback_days=args.lookback_days, top_n=args.top_n)
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
    except psycopg2.OperationalError as exc:
        logger.error("Could not connect to Redshift: %s", exc)
        sys.exit(2)
    except Exception:
        logger.exception("Unexpected error during workload analysis.")
        sys.exit(99)


if __name__ == "__main__":
    main()
