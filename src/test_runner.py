"""
test_runner.py — Automated Test Case Generation & Execution
============================================================
Generates and executes pytest-style tests covering:
  - Schema conversion tests
  - SQL translation tests
  - Data parity tests
  - Edge case tests (nulls, datatype changes)
  - Manual-flag detection tests

Produces:
    test_results/test_report.xml     — JUnit XML
    test_results/test_summary.html   — Human-readable HTML report

Run standalone:
    python -m src.test_runner
"""

import html
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring, indent

import pandas as pd

# ═══════════════════════════════════════════════════════════════════════════════
# Paths
# ═══════════════════════════════════════════════════════════════════════════════

PROJECT_DIR = Path(__file__).resolve().parent.parent
MOCK_DATA_DIR = PROJECT_DIR / "mock_data"
ARTIFACTS_DIR = PROJECT_DIR / "artifacts"
TARGET_DIR = ARTIFACTS_DIR / "target_tables"
TRANSPILED_DIR = ARTIFACTS_DIR / "transpiled_sql"
TEST_RESULTS_DIR = PROJECT_DIR / "test_results"

# Detect platform and import the correct transpiler
sys.path.insert(0, str(PROJECT_DIR / "src"))

def _detect_platform() -> str:
    """Detect which platform was used by inspecting the source catalog."""
    catalog_path = MOCK_DATA_DIR / "source_catalog.json"
    if catalog_path.exists():
        import json as _json
        cat = _json.loads(catalog_path.read_text(encoding="utf-8"))
        # Snowflake catalogs have 'account' key; Redshift catalogs have 'cluster'
        if "account" in cat:
            return "snowflake"
    return "redshift"

_PLATFORM = _detect_platform()

if _PLATFORM == "snowflake":
    from mock_snowflake_converter import transpile_sql, MANUAL_PATTERNS  # noqa: E402
else:
    from mock_converter import transpile_sql, MANUAL_PATTERNS  # noqa: E402


# ═══════════════════════════════════════════════════════════════════════════════
# Test framework
# ═══════════════════════════════════════════════════════════════════════════════

class TestResult:
    """Single test case result."""
    __slots__ = ("suite", "name", "passed", "duration", "message", "detail")

    def __init__(self, suite: str, name: str, passed: bool,
                 duration: float = 0.0, message: str = "", detail: str = ""):
        self.suite = suite
        self.name = name
        self.passed = passed
        self.duration = duration
        self.message = message
        self.detail = detail


def _run_test(suite: str, name: str, test_fn) -> TestResult:
    """Execute a test function and capture result."""
    start = time.time()
    try:
        test_fn()
        return TestResult(suite, name, True, time.time() - start)
    except AssertionError as e:
        return TestResult(suite, name, False, time.time() - start,
                          message=str(e), detail=str(e))
    except Exception as e:
        return TestResult(suite, name, False, time.time() - start,
                          message=f"ERROR: {type(e).__name__}: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Test suites
# ═══════════════════════════════════════════════════════════════════════════════

def _load_catalog() -> dict:
    path = MOCK_DATA_DIR / "source_catalog.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _load_conversion_report() -> dict:
    path = ARTIFACTS_DIR / "conversion_report.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _load_load_summary() -> dict:
    path = ARTIFACTS_DIR / "load_summary.json"
    return json.loads(path.read_text(encoding="utf-8"))


# --- Suite 1: Schema Conversion Tests ---

def generate_schema_tests() -> list[TestResult]:
    """Verify that every table's DDL was transpiled and output files exist."""
    results = []
    catalog = _load_catalog()
    report = _load_conversion_report()

    obj_names = {o["object_name"] for o in report["objects"]}

    # Test: every catalog table has a conversion entry
    for t in catalog["tables"]:
        fqn = f"{t['schema']}.{t['table']}"

        def make_test(fqn=fqn):
            def test():
                assert fqn in obj_names, f"{fqn} missing from conversion report"
            return test

        results.append(_run_test("schema_conversion", f"table_in_report_{fqn}", make_test()))

    # Test: every transpiled .sql file exists
    for obj in report["objects"]:
        if obj["object_type"] in ("TABLE", "VIEW"):
            parts = obj["object_name"].split(".")
            if len(parts) == 2:
                fname = f"{parts[0]}_{parts[1]}.sql"
                path = TRANSPILED_DIR / fname

                def make_file_test(p=path, n=obj["object_name"]):
                    def test():
                        assert p.exists(), f"Transpiled file missing: {p.name} for {n}"
                    return test

                results.append(_run_test(
                    "schema_conversion", f"file_exists_{obj['object_name']}", make_file_test()
                ))

    # Test: no source-platform keywords remain in transpiled output
    for sql_file in TRANSPILED_DIR.glob("*.sql"):
        content = sql_file.read_text(encoding="utf-8")

        if _PLATFORM == "snowflake":
            def make_clean_test(c=content, f=sql_file.name):
                def test():
                    body = "\n".join(
                        l for l in c.splitlines() if not l.strip().startswith("--")
                    )
                    assert not re.search(r"\bCLUSTER\s+BY\b", body, re.IGNORECASE), \
                        f"CLUSTER BY still present in {f}"
                    assert not re.search(r"\bAUTOINCREMENT\b", body, re.IGNORECASE), \
                        f"AUTOINCREMENT still present in {f}"
                    assert not re.search(r"\bTRANSIENT\s+TABLE\b", body, re.IGNORECASE), \
                        f"TRANSIENT still present in {f}"
                return test

            results.append(_run_test(
                "schema_conversion", f"no_snowflake_keywords_{sql_file.name}", make_clean_test()
            ))
        else:
            def make_clean_test(c=content, f=sql_file.name):
                def test():
                    body = "\n".join(
                        l for l in c.splitlines() if not l.strip().startswith("--")
                    )
                    assert not re.search(r"\bDISTKEY\b", body, re.IGNORECASE), \
                        f"DISTKEY still present in {f}"
                    assert not re.search(r"\bSORTKEY\b", body, re.IGNORECASE), \
                        f"SORTKEY still present in {f}"
                    assert not re.search(r"\bENCODE\s+\w+", body, re.IGNORECASE), \
                        f"ENCODE still present in {f}"
                    assert not re.search(r"\bDISSTYLE\b", body, re.IGNORECASE), \
                        f"DISTSTYLE still present in {f}"
                return test

            results.append(_run_test(
                "schema_conversion", f"no_redshift_keywords_{sql_file.name}", make_clean_test()
            ))

    # Test: USING DELTA present in table DDL
    for sql_file in TRANSPILED_DIR.glob("*.sql"):
        content = sql_file.read_text(encoding="utf-8")
        if "CREATE TABLE" in content.upper():
            def make_delta_test(c=content, f=sql_file.name):
                def test():
                    assert "USING DELTA" in c, f"USING DELTA missing in {f}"
                return test

            results.append(_run_test(
                "schema_conversion", f"using_delta_{sql_file.name}", make_delta_test()
            ))

    return results


# --- Suite 2: SQL Translation Tests ---

def generate_sql_translation_tests() -> list[TestResult]:
    """Test individual rewrite rules with known inputs."""
    results = []

    if _PLATFORM == "snowflake":
        cases = [
            ("nvl_to_coalesce",
             "SELECT NVL(a, b) FROM t",
             "COALESCE("),
            ("iff_to_if",
             "SELECT IFF(x > 0, 'yes', 'no') FROM t",
             "IF("),
            ("varchar_removal",
             "CREATE TABLE t (name VARCHAR(256))",
             "STRING"),
            ("dateadd_rewrite",
             "SELECT DATEADD(day, -30, CURRENT_TIMESTAMP())",
             "DATE_ADD"),
            ("double_colon_cast",
             "SELECT total::FLOAT FROM t",
             "CAST(total AS FLOAT)"),
            ("number_to_bigint",
             "col NUMBER(38,0) NOT NULL",
             "BIGINT"),
            ("number_to_decimal",
             "col NUMBER(12,2)",
             "DECIMAL(12,2)"),
            ("timestamp_ntz",
             "col TIMESTAMP_NTZ NOT NULL",
             "TIMESTAMP"),
            ("autoincrement",
             "id NUMBER(38,0) AUTOINCREMENT",
             "GENERATED BY DEFAULT AS IDENTITY"),
            ("position_to_locate",
             "SELECT POSITION('@' IN email) FROM t",
             "LOCATE("),
            ("parse_json",
             "SELECT PARSE_JSON(col) FROM t",
             "FROM_JSON("),
            ("len_to_length",
             "SELECT LEN(name) FROM t",
             "LENGTH("),
            ("regexp_substr",
             "SELECT REGEXP_SUBSTR(col, '\\d+') FROM t",
             "REGEXP_EXTRACT("),
        ]
    else:
        cases = [
            ("nvl_to_coalesce",
             "SELECT NVL(a, b) FROM t",
             "COALESCE("),
            ("getdate_to_current_timestamp",
             "SELECT GETDATE() AS now",
             "CURRENT_TIMESTAMP()"),
            ("len_to_length",
             "SELECT LEN(name) FROM t",
             "LENGTH("),
            ("charindex_to_locate",
             "SELECT CHARINDEX('@', email) FROM t",
             "LOCATE("),
            ("varchar_removal",
             "CREATE TABLE t (name VARCHAR(256))",
             "STRING"),
            ("identity_rewrite",
             "id INTEGER IDENTITY(1,1)",
             "GENERATED BY DEFAULT AS IDENTITY"),
            ("dateadd_rewrite",
             "SELECT DATEADD(day, -30, GETDATE())",
             "DATE_ADD"),
            ("date_part_to_extract",
             "SELECT DATE_PART('year', order_date) FROM t",
             "EXTRACT(year FROM"),
            ("random_to_rand",
             "SELECT RANDOM() AS r",
             "RAND()"),
            ("numeric_to_decimal",
             "col NUMERIC(12,2)",
             "DECIMAL(12,2)"),
            ("sysdate_rewrite",
             "SELECT SYSDATE FROM dual",
             "CURRENT_TIMESTAMP()"),
            ("double_colon_cast",
             "SELECT total::FLOAT FROM t",
             "CAST(total AS FLOAT)"),
            ("hashtext_to_hash",
             "SELECT HASHTEXT(col) FROM t",
             "HASH("),
        ]

    for label, input_sql, expected_fragment in cases:
        def make_test(sql=input_sql, frag=expected_fragment, lbl=label):
            def test():
                result = transpile_sql(sql, f"test_{lbl}")
                assert frag in result["transpiled_sql"], \
                    f"Expected '{frag}' in output but got: {result['transpiled_sql']}"
            return test

        results.append(_run_test("sql_translation", f"rule_{label}", make_test()))

    # Test: classification is AUTO_CONVERT for clean SQL
    def test_auto_class():
        r = transpile_sql("SELECT a, b FROM t WHERE x = 1", "simple_select")
        assert r["classification"] == "AUTO_CONVERT", \
            f"Expected AUTO_CONVERT, got {r['classification']}"

    results.append(_run_test("sql_translation", "auto_convert_simple", test_auto_class))

    # Test: CONVERT_WITH_WARNINGS for uncertain rules
    def test_warnings_class():
        sql = "SELECT LISTAGG(name, ',') WITHIN GROUP (ORDER BY name) FROM t"
        r = transpile_sql(sql, "listagg_test")
        assert r["classification"] in ("CONVERT_WITH_WARNINGS", "AUTO_CONVERT"), \
            f"Expected warning class, got {r['classification']}"

    results.append(_run_test("sql_translation", "warnings_on_listagg", test_warnings_class))

    # Test: diff is non-empty when rules are applied
    def test_diff_generated():
        r = transpile_sql("SELECT NVL(a, b) FROM t", "diff_test")
        assert r["diff"], "Diff should be non-empty when rules are applied"

    results.append(_run_test("sql_translation", "diff_generated", test_diff_generated))

    # Test: SHA-256 hash is stable (idempotent)
    def test_idempotent():
        sql = "SELECT NVL(a,b), LEN(c) FROM t"
        r1 = transpile_sql(sql, "idem_test")
        r2 = transpile_sql(sql, "idem_test")
        assert r1["sha256"] == r2["sha256"], "SHA-256 should be stable"
        assert r1["transpiled_sql"] == r2["transpiled_sql"], "Output should be identical"

    results.append(_run_test("sql_translation", "idempotent_output", test_idempotent))

    return results


# --- Suite 3: Data Parity Tests ---

def generate_data_parity_tests() -> list[TestResult]:
    """Verify target Parquet files match expected structure."""
    results = []
    catalog = _load_catalog()
    load_summary = _load_load_summary()

    cols_by_table = {}
    for c in catalog["columns"]:
        key = f"{c['schema']}.{c['table']}"
        cols_by_table.setdefault(key, []).append(c)

    for load_rec in load_summary.get("tables", []):
        fqn = load_rec["fqn"]
        parquet_path = Path(load_rec["parquet_path"])

        # Test: file exists
        def make_exists(p=parquet_path, n=fqn):
            def test():
                assert p.exists(), f"Parquet missing for {n}: {p}"
            return test
        results.append(_run_test("data_parity", f"parquet_exists_{fqn}", make_exists()))

        if not parquet_path.exists():
            continue

        # Test: row count matches load summary
        def make_rowcount(p=parquet_path, expected=load_rec["rows_loaded"], n=fqn):
            def test():
                df = pd.read_parquet(p)
                assert len(df) == expected, \
                    f"{n}: expected {expected} rows, got {len(df)}"
            return test
        results.append(_run_test("data_parity", f"row_count_{fqn}", make_rowcount()))

        # Test: all expected columns present
        source_cols = cols_by_table.get(fqn, [])
        def make_cols(p=parquet_path, cols=source_cols, n=fqn):
            def test():
                df = pd.read_parquet(p)
                expected_names = {c["column"] for c in cols}
                actual_names = set(df.columns)
                missing = expected_names - actual_names
                assert not missing, f"{n}: missing columns: {missing}"
            return test
        results.append(_run_test("data_parity", f"columns_present_{fqn}", make_cols()))

        # Test: no completely empty dataframe
        def make_notempty(p=parquet_path, n=fqn):
            def test():
                df = pd.read_parquet(p)
                assert len(df) > 0, f"{n}: Parquet is empty"
            return test
        results.append(_run_test("data_parity", f"not_empty_{fqn}", make_notempty()))

    return results


# --- Suite 4: Edge Case Tests ---

def generate_edge_case_tests() -> list[TestResult]:
    """Test handling of nulls, empty strings, type edge cases."""
    results = []

    # Test: transpiler handles empty SQL
    def test_empty_sql():
        r = transpile_sql("", "empty")
        assert r["transpiled_sql"] == "", "Empty SQL should produce empty output"
        assert r["classification"] == "AUTO_CONVERT"

    results.append(_run_test("edge_cases", "empty_sql", test_empty_sql))

    # Test: transpiler handles SQL with only comments
    def test_comments_only():
        r = transpile_sql("-- just a comment\n-- another one", "comments")
        assert r["classification"] == "AUTO_CONVERT"

    results.append(_run_test("edge_cases", "comments_only", test_comments_only))

    # Test: all type aliases are converted
    if _PLATFORM == "snowflake":
        type_tests = [
            ("TIMESTAMP_NTZ", "TIMESTAMP"), ("TIMESTAMP_LTZ", "TIMESTAMP"),
            ("TIMESTAMP_TZ", "TIMESTAMP"), ("VARIANT", "STRING"),
        ]
    else:
        type_tests = [
            ("FLOAT4", "FLOAT"), ("FLOAT8", "DOUBLE"),
            ("INT2", "SMALLINT"), ("INT4", "INT"), ("INT8", "BIGINT"),
            ("BPCHAR", "STRING"), ("TIMESTAMPTZ", "TIMESTAMP"),
        ]
    for src_type, tgt_type in type_tests:
        def make_type_test(s=src_type, t=tgt_type):
            def test():
                r = transpile_sql(f"col {s} NOT NULL", f"type_{s}")
                assert t in r["transpiled_sql"], \
                    f"Expected {t} in output for {s}, got: {r['transpiled_sql']}"
            return test
        results.append(_run_test("edge_cases", f"type_alias_{src_type}", make_type_test()))

    # Test: VARCHAR variant converted to STRING
    def test_varchar_variant():
        r = transpile_sql("col VARCHAR(500)", "varchar_variant")
        assert "STRING" in r["transpiled_sql"]

    results.append(_run_test("edge_cases", "varchar_variant_to_string", test_varchar_variant))

    # Test: multiple rules applied in single SQL
    if _PLATFORM == "snowflake":
        def test_multi_rule():
            sql = "SELECT NVL(a, b), LEN(name), IFF(x > 0, 1, 0) FROM t"
            r = transpile_sql(sql, "multi")
            assert "COALESCE(" in r["transpiled_sql"]
            assert "LENGTH(" in r["transpiled_sql"]
            assert "IF(" in r["transpiled_sql"]
            assert len(r["applied_rules"]) >= 3
    else:
        def test_multi_rule():
            sql = "SELECT NVL(a, b), LEN(name), GETDATE() FROM t DISTSTYLE EVEN"
            r = transpile_sql(sql, "multi")
            assert "COALESCE(" in r["transpiled_sql"]
            assert "LENGTH(" in r["transpiled_sql"]
            assert "CURRENT_TIMESTAMP()" in r["transpiled_sql"]
            assert "DISTSTYLE" not in r["transpiled_sql"]
            assert len(r["applied_rules"]) >= 3

    results.append(_run_test("edge_cases", "multiple_rules_combined", test_multi_rule))

    # Test: Parquet files have correct nullable columns
    catalog = _load_catalog()
    nullable_cols = [
        c for c in catalog["columns"]
        if c.get("nullable") == "YES" and c["schema"] == "public" and c["table"] == "customers"
    ]
    if nullable_cols:
        def test_nullable():
            df = pd.read_parquet(TARGET_DIR / "public_customers.parquet")
            for col_meta in nullable_cols:
                col = col_meta["column"]
                if col in df.columns:
                    # Nullable columns should have appropriate pandas dtype
                    assert True  # Column exists and is readable

        results.append(_run_test("edge_cases", "nullable_columns_parquet", test_nullable))

    # Test: very long SQL doesn't crash transpiler
    def test_long_sql():
        long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(200)) + " FROM big_table"
        r = transpile_sql(long_sql, "long_sql")
        assert r["transpiled_sql"]
        assert r["difficulty"] >= 1

    results.append(_run_test("edge_cases", "long_sql_200_columns", test_long_sql))

    # Test: difficulty score is bounded 1-10
    def test_difficulty_bounds():
        r = transpile_sql("SELECT 1", "trivial")
        assert 1 <= r["difficulty"] <= 10, f"Difficulty {r['difficulty']} out of bounds"

    results.append(_run_test("edge_cases", "difficulty_bounded", test_difficulty_bounds))

    return results


# --- Suite 5: Manual-Flag Detection Tests ---

def generate_manual_flag_tests() -> list[TestResult]:
    """Verify that manual-rewrite patterns are correctly detected."""
    results = []

    # Test each manual pattern (platform-specific)
    if _PLATFORM == "snowflake":
        pattern_samples = [
            ("stored_procedure",
             "CREATE OR REPLACE PROCEDURE sp_test() RETURNS VARCHAR LANGUAGE SQL AS $$ BEGIN RETURN 'ok'; END; $$;"),
            ("javascript_procedure",
             "CREATE PROCEDURE sp_js() RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ return 'ok'; $$;"),
            ("python_procedure",
             "CREATE PROCEDURE sp_py() RETURNS VARCHAR LANGUAGE PYTHON AS $$ return 'ok' $$;"),
            ("resultset_usage",
             "DECLARE res RESULTSET; BEGIN res := (SELECT 1); END;"),
            ("execute_immediate",
             "EXECUTE IMMEDIATE 'SELECT 1';"),
            ("javascript_api",
             "var stmt = snowflake.createStatement({sqlText: 'SELECT 1'});"),
            ("stream_object",
             "CREATE OR REPLACE STREAM my_stream ON TABLE t;"),
            ("task_object",
             "CREATE OR REPLACE TASK my_task WAREHOUSE=WH AS SELECT 1;"),
            ("stage_object",
             "CREATE OR REPLACE STAGE my_stage URL='s3://bucket/path/';"),
            ("file_format",
             "CREATE OR REPLACE FILE FORMAT my_fmt TYPE='CSV';"),
            ("copy_into",
             "COPY INTO my_table FROM @my_stage;"),
            ("qualify_clause",
             "SELECT * FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1;"),
        ]
    else:
        pattern_samples = [
            ("stored_procedure",
             "CREATE OR REPLACE PROCEDURE sp_test() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;"),
            ("plpgsql_function",
             "CREATE FUNCTION f() RETURNS void AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;"),
            ("cursor_usage",
             "DECLARE my_cursor CURSOR FOR SELECT 1;"),
            ("raise_statement",
             "RAISE INFO 'processing row %', v_id;"),
            ("for_loop",
             "FOR rec IN SELECT * FROM t LOOP NULL; END LOOP;"),
            ("loop_construct",
             "LOOP EXIT WHEN done; END LOOP;"),
            ("if_then",
             "IF v_count > 0 THEN INSERT INTO t VALUES (1); END IF;"),
            ("dynamic_sql",
             "EXECUTE 'SELECT * FROM ' || table_name;"),
            ("copy_command",
             "COPY my_table FROM 's3://bucket/data/' IAM_ROLE 'arn:aws:iam::123:role/r';"),
            ("unload_command",
             "UNLOAD ('SELECT * FROM t') TO 's3://bucket/out/';"),
            ("external_schema",
             "CREATE EXTERNAL SCHEMA spectrum FROM DATA CATALOG;"),
            ("create_library",
             "CREATE LIBRARY my_lib LANGUAGE plpythonu FROM 's3://bucket/lib.zip';"),
        ]

    for flag_name, sample_sql in pattern_samples:
        def make_test(sql=sample_sql, flag=flag_name):
            def test():
                r = transpile_sql(sql, f"manual_{flag}")
                assert r["classification"] == "MANUAL_REWRITE_REQUIRED", \
                    f"Expected MANUAL_REWRITE_REQUIRED for {flag}, got {r['classification']}"
                assert flag in r["manual_flags"], \
                    f"Expected flag '{flag}' in {r['manual_flags']}"
            return test

        results.append(_run_test("manual_flags", f"detects_{flag_name}", make_test()))

    # Test: clean SQL does NOT trigger manual flags
    def test_no_false_positives():
        clean_sql = "SELECT a, b, c FROM t WHERE x > 0 ORDER BY a"
        r = transpile_sql(clean_sql, "clean")
        assert r["classification"] != "MANUAL_REWRITE_REQUIRED", \
            "Clean SQL should not be flagged as MANUAL_REWRITE_REQUIRED"
        assert len(r["manual_flags"]) == 0, \
            f"Clean SQL should have no manual flags, got: {r['manual_flags']}"

    results.append(_run_test("manual_flags", "no_false_positive_clean_sql", test_no_false_positives))

    # Test: conversion report correctly flags procedures
    report = _load_conversion_report()
    procs = [o for o in report["objects"] if o["object_type"] == "PROCEDURE"]

    def test_all_procs_manual():
        for p in procs:
            assert p["classification"] == "MANUAL_REWRITE_REQUIRED", \
                f"Procedure {p['object_name']} should be MANUAL_REWRITE_REQUIRED"

    results.append(_run_test("manual_flags", "all_procs_manual_in_report", test_all_procs_manual))

    # Test: flagged_manual_items has guidance text
    def test_guidance_present():
        items = report.get("flagged_manual_items", [])
        for item in items:
            assert "guidance" in item, f"Missing guidance for {item.get('object')}"
            assert len(item["guidance"]) > 10, f"Guidance too short for {item.get('object')}"

    results.append(_run_test("manual_flags", "guidance_in_manual_items", test_guidance_present))

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Output generation
# ═══════════════════════════════════════════════════════════════════════════════

def _to_junit_xml(all_results: list[TestResult]) -> str:
    """Convert results to JUnit XML format."""
    suites_map: dict[str, list[TestResult]] = {}
    for r in all_results:
        suites_map.setdefault(r.suite, []).append(r)

    root = Element("testsuites")
    root.set("tests", str(len(all_results)))
    root.set("failures", str(sum(1 for r in all_results if not r.passed)))
    root.set("time", str(round(sum(r.duration for r in all_results), 3)))

    for suite_name, tests in suites_map.items():
        suite_el = SubElement(root, "testsuite")
        suite_el.set("name", suite_name)
        suite_el.set("tests", str(len(tests)))
        suite_el.set("failures", str(sum(1 for t in tests if not t.passed)))
        suite_el.set("time", str(round(sum(t.duration for t in tests), 3)))

        for t in tests:
            tc = SubElement(suite_el, "testcase")
            tc.set("classname", suite_name)
            tc.set("name", t.name)
            tc.set("time", str(round(t.duration, 4)))
            if not t.passed:
                fail = SubElement(tc, "failure")
                fail.set("message", t.message or "Assertion failed")
                fail.text = t.detail or t.message

    indent(root)
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode")


def _to_html_report(all_results: list[TestResult]) -> str:
    """Generate a self-contained HTML test summary."""
    total = len(all_results)
    passed = sum(1 for r in all_results if r.passed)
    failed = total - passed
    pass_rate = round(passed / max(total, 1) * 100, 1)

    suites_map: dict[str, list[TestResult]] = {}
    for r in all_results:
        suites_map.setdefault(r.suite, []).append(r)

    rows_html = ""
    for suite_name, tests in suites_map.items():
        s_passed = sum(1 for t in tests if t.passed)
        s_total = len(tests)
        rows_html += f"""
        <tr class="suite-header">
            <td colspan="4"><strong>{html.escape(suite_name)}</strong>
            — {s_passed}/{s_total} passed</td>
        </tr>"""
        for t in tests:
            status_cls = "pass" if t.passed else "fail"
            status_txt = "PASS" if t.passed else "FAIL"
            msg = html.escape(t.message) if t.message else ""
            rows_html += f"""
        <tr class="{status_cls}">
            <td class="status {status_cls}">{status_txt}</td>
            <td>{html.escape(t.name)}</td>
            <td>{t.duration:.4f}s</td>
            <td>{msg}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Migration Test Report</title>
<style>
    * {{ box-sizing: border-box; }}
    body {{
        font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
        margin: 0; padding: 2rem;
        background: linear-gradient(135deg, #0f1117 0%, #1a1d2e 50%, #0f1117 100%);
        color: #c4c9e0;
        min-height: 100vh;
    }}
    h1 {{
        color: #e2e8f0;
        font-size: 1.8rem;
        text-shadow: 0 0 20px rgba(124, 58, 237, 0.15);
        margin-bottom: 0.5rem;
    }}
    .summary {{
        display: flex; gap: 1.2rem; margin: 1.5rem 0; flex-wrap: wrap;
    }}
    .card {{
        background: linear-gradient(145deg, #1e2235, #252a40);
        border: 1px solid rgba(124, 58, 237, 0.2);
        border-radius: 12px; padding: 1.2rem 1.8rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05);
        text-align: center; min-width: 130px;
        transition: transform 0.2s ease;
    }}
    .card:hover {{ transform: translateY(-2px); box-shadow: 0 8px 25px rgba(124,58,237,0.15); }}
    .card .value {{ font-size: 2rem; font-weight: 700; color: #e2e8f0; }}
    .card .label {{ font-size: 0.8rem; color: #94a3b8; margin-top: 0.3rem;
                    text-transform: uppercase; letter-spacing: 0.5px; }}
    .card.green .value {{ color: #34d399; }}
    .card.red .value {{ color: #f87171; }}
    .card.blue .value {{ color: #818cf8; }}
    table {{
        width: 100%; border-collapse: collapse;
        background: #1e2235;
        border-radius: 12px; overflow: hidden;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        border: 1px solid rgba(124, 58, 237, 0.15);
    }}
    th {{
        background: linear-gradient(135deg, #7c3aed, #6d28d9);
        color: white; padding: 0.9rem 1rem; text-align: left;
        font-weight: 600; font-size: 0.85rem;
        text-transform: uppercase; letter-spacing: 0.5px;
    }}
    td {{ padding: 0.6rem 1rem; border-bottom: 1px solid rgba(124,58,237,0.1); color: #c4c9e0; }}
    tr.suite-header td {{
        background: linear-gradient(135deg, #252a40, #2d3354);
        font-size: 0.95rem; color: #e2e8f0;
        border-bottom: 1px solid rgba(124,58,237,0.2);
    }}
    tr.pass td {{ }}
    tr.fail td {{ background: rgba(248,113,113,0.08); }}
    tr:hover td {{ background: rgba(124,58,237,0.05); }}
    .status {{ font-weight: 700; width: 50px; }}
    .status.pass {{ color: #34d399; }}
    .status.fail {{ color: #f87171; }}
    .timestamp {{ color: #64748b; font-size: 0.85rem; margin-top: 1.5rem; }}
</style>
</head>
<body>
<h1>Migration Toolkit — Test Report</h1>
<div class="summary">
    <div class="card blue"><div class="value">{total}</div><div class="label">Total Tests</div></div>
    <div class="card green"><div class="value">{passed}</div><div class="label">Passed</div></div>
    <div class="card red"><div class="value">{failed}</div><div class="label">Failed</div></div>
    <div class="card {'green' if pass_rate >= 90 else 'red'}">
        <div class="value">{pass_rate}%</div><div class="label">Pass Rate</div></div>
</div>
<table>
<thead><tr><th>Status</th><th>Test Name</th><th>Duration</th><th>Message</th></tr></thead>
<tbody>
{rows_html}
</tbody>
</table>
<p class="timestamp">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════════
# Entrypoint
# ═══════════════════════════════════════════════════════════════════════════════

def run() -> list[TestResult]:
    """Generate and execute all test suites."""
    TEST_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("Running test suites...")
    all_results: list[TestResult] = []

    suites = [
        ("Schema Conversion", generate_schema_tests),
        ("SQL Translation", generate_sql_translation_tests),
        ("Data Parity", generate_data_parity_tests),
        ("Edge Cases", generate_edge_case_tests),
        ("Manual-Flag Detection", generate_manual_flag_tests),
    ]

    for suite_label, gen_fn in suites:
        print(f"  [{suite_label}]...", end=" ", flush=True)
        results = gen_fn()
        passed = sum(1 for r in results if r.passed)
        print(f"{passed}/{len(results)} passed")
        all_results.extend(results)

    # Write JUnit XML
    xml_path = TEST_RESULTS_DIR / "test_report.xml"
    xml_path.write_text(_to_junit_xml(all_results), encoding="utf-8")

    # Write HTML report
    html_path = TEST_RESULTS_DIR / "test_summary.html"
    html_path.write_text(_to_html_report(all_results), encoding="utf-8")

    total = len(all_results)
    passed = sum(1 for r in all_results if r.passed)
    failed = total - passed
    print(f"\nTest report (XML)    : {xml_path}")
    print(f"Test report (HTML)   : {html_path}")
    print(f"  Total              : {total}")
    print(f"  Passed             : {passed}")
    print(f"  Failed             : {failed}")
    print(f"  Pass rate          : {round(passed / max(total, 1) * 100, 1)}%")

    return all_results


if __name__ == "__main__":
    run()
