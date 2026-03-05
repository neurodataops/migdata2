"""
sql_transpiler_service.py
=========================
Core engine for translating Redshift SQL/DDL to Databricks-compatible SQL.

Two-stage approach:
  1. Rule-based translator — deterministic regex/string rewrites for known
     Redshift-to-Databricks differences (types, functions, DDL syntax).
  2. LLM-assisted translator (placeholder) — flags complex expressions for
     LLM review and produces an audit trail.

Classification:
  AUTO_CONVERT              — fully handled by rule-based transforms
  CONVERT_WITH_WARNINGS     — rule-based with uncertain transforms or LLM assist
  MANUAL_REWRITE_REQUIRED   — stored procedures, plpgsql, complex Python UDFs

All transforms are logged. The service is idempotent: feeding the same input
produces the same output and report.
"""

import difflib
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# Classification
# ═══════════════════════════════════════════════════════════════════════════

class Classification(str, Enum):
    AUTO_CONVERT = "AUTO_CONVERT"
    CONVERT_WITH_WARNINGS = "CONVERT_WITH_WARNINGS"
    MANUAL_REWRITE_REQUIRED = "MANUAL_REWRITE_REQUIRED"


# ═══════════════════════════════════════════════════════════════════════════
# Data-type mapping: Redshift → Databricks (Delta / Spark SQL)
# ═══════════════════════════════════════════════════════════════════════════

DATATYPE_MAP = {
    # Boolean
    "BOOL":      "BOOLEAN",
    "BOOLEAN":   "BOOLEAN",
    # Integer family
    "SMALLINT":  "SMALLINT",
    "INT2":      "SMALLINT",
    "INTEGER":   "INT",
    "INT":       "INT",
    "INT4":      "INT",
    "BIGINT":    "BIGINT",
    "INT8":      "BIGINT",
    # Float / Decimal
    "REAL":            "FLOAT",
    "FLOAT4":          "FLOAT",
    "FLOAT":           "DOUBLE",
    "FLOAT8":          "DOUBLE",
    "DOUBLE PRECISION":"DOUBLE",
    # Numeric / Decimal (parameterised handled separately)
    "NUMERIC":   "DECIMAL",
    "DECIMAL":   "DECIMAL",
    # Character
    "CHAR":             "STRING",
    "CHARACTER":        "STRING",
    "NCHAR":            "STRING",
    "BPCHAR":           "STRING",
    "VARCHAR":          "STRING",
    "CHARACTER VARYING":"STRING",
    "NVARCHAR":         "STRING",
    "TEXT":             "STRING",
    # Date/Time
    "DATE":                         "DATE",
    "TIMESTAMP":                    "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE":  "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE":     "TIMESTAMP",
    "TIMESTAMPTZ":                  "TIMESTAMP",
    "TIME":                         "STRING",  # Spark has no native TIME
    "TIME WITHOUT TIME ZONE":       "STRING",
    "TIMETZ":                       "STRING",
    "TIME WITH TIME ZONE":          "STRING",
    # Binary
    "VARBYTE":    "BINARY",
    "BYTEA":      "BINARY",
    # Super / semi-structured
    "SUPER":      "STRING",  # parse as JSON string; or VARIANT if UC
    "HLLSKETCH":  "BINARY",
    "GEOMETRY":   "STRING",
}

# ═══════════════════════════════════════════════════════════════════════════
# Function-name mapping: Redshift → Databricks
# ═══════════════════════════════════════════════════════════════════════════

FUNCTION_MAP = {
    # Null handling
    "NVL":          "COALESCE",
    "NVL2":         "NVL2",           # Databricks supports NVL2
    "ISNULL":       "ISNULL",         # Databricks supports ISNULL
    # String
    "LEN":          "LENGTH",
    "CHARINDEX":    "LOCATE",         # arg order differs — handled in regex
    "STRTOL":       "CONV",           # base conversion
    "BTRIM":        "TRIM",
    "RTRIM":        "RTRIM",
    "LTRIM":        "LTRIM",
    "BPCHARCMP":    "STRCMP",
    # Date / Time
    "SYSDATE":          "CURRENT_TIMESTAMP()",
    "GETDATE()":        "CURRENT_TIMESTAMP()",
    "GETDATE":          "CURRENT_TIMESTAMP",
    "CONVERT_TIMEZONE": "FROM_UTC_TIMESTAMP",   # arg mapping needed
    "DATEADD":          "DATEADD",              # Databricks supports DATEADD
    "DATEDIFF":         "DATEDIFF",             # Databricks supports DATEDIFF
    "DATE_PART":        "EXTRACT",              # rewrite to EXTRACT(part FROM expr)
    "DATE_TRUNC":       "DATE_TRUNC",           # Databricks supports DATE_TRUNC
    # Math
    "TRUNC":     "TRUNC",
    "RANDOM":    "RAND",
    # Encoding
    "FUNC_SHA1": "SHA1",
    "SHA1":      "SHA1",
    "SHA2":      "SHA2",
    "MD5":       "MD5",
    # JSON (Redshift SUPER)
    "JSON_EXTRACT_PATH_TEXT": "GET_JSON_OBJECT",
    "JSON_EXTRACT_ARRAY_ELEMENT_TEXT": "GET_JSON_OBJECT",
    # Aggregate
    "LISTAGG":           "CONCAT_WS",   # approximate; needs manual check
    "APPROXIMATE COUNT": "APPROX_COUNT_DISTINCT",
    "MEDIAN":            "PERCENTILE_APPROX",
    # Bitwise
    "CHECKSUM":  "CRC32",
}

# ═══════════════════════════════════════════════════════════════════════════
# Rule-based regex transforms
# Each rule: (compiled_regex, replacement, description, is_uncertain)
# is_uncertain=True → CONVERT_WITH_WARNINGS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class RewriteRule:
    name: str
    pattern: re.Pattern
    replacement: str
    description: str
    uncertain: bool = False


REWRITE_RULES: list[RewriteRule] = [
    # ── Rule 1: Remove DISTKEY clause ─────────────────────────────────────
    RewriteRule(
        name="remove_distkey",
        pattern=re.compile(
            r"\bDISTKEY\s*\(\s*\w+\s*\)", re.IGNORECASE
        ),
        replacement="",
        description="Remove Redshift DISTKEY(...) clause (not applicable in Delta Lake)",
    ),

    # ── Rule 2: Remove SORTKEY / COMPOUND SORTKEY / INTERLEAVED SORTKEY ──
    RewriteRule(
        name="remove_sortkey",
        pattern=re.compile(
            r"\b(?:COMPOUND\s+|INTERLEAVED\s+)?SORTKEY\s*\([^)]*\)",
            re.IGNORECASE,
        ),
        replacement="",
        description="Remove Redshift SORTKEY / COMPOUND SORTKEY / INTERLEAVED SORTKEY",
    ),

    # ── Rule 3: Remove DISTSTYLE ──────────────────────────────────────────
    RewriteRule(
        name="remove_diststyle",
        pattern=re.compile(
            r"\bDISTSTYLE\s+(ALL|EVEN|KEY|AUTO)\b", re.IGNORECASE
        ),
        replacement="",
        description="Remove Redshift DISTSTYLE directive",
    ),

    # ── Rule 4: Remove ENCODE column compression ─────────────────────────
    RewriteRule(
        name="remove_encode",
        pattern=re.compile(
            r"\bENCODE\s+\w+\b", re.IGNORECASE
        ),
        replacement="",
        description="Remove Redshift ENCODE compression directive",
    ),

    # ── Rule 5: IDENTITY → GENERATED BY DEFAULT AS IDENTITY ──────────────
    RewriteRule(
        name="identity_to_generated",
        pattern=re.compile(
            r"\bIDENTITY\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)",
            re.IGNORECASE,
        ),
        replacement=r"GENERATED BY DEFAULT AS IDENTITY (START WITH \1 INCREMENT BY \2)",
        description="Convert Redshift IDENTITY(seed,step) to Delta GENERATED BY DEFAULT AS IDENTITY",
    ),

    # ── Rule 6: GETDATE() / SYSDATE → CURRENT_TIMESTAMP() ────────────────
    RewriteRule(
        name="getdate_to_current_timestamp",
        pattern=re.compile(
            r"\b(?:GETDATE\s*\(\s*\)|SYSDATE)\b", re.IGNORECASE
        ),
        replacement="CURRENT_TIMESTAMP()",
        description="Replace GETDATE()/SYSDATE with CURRENT_TIMESTAMP()",
    ),

    # ── Rule 7: NVL( → COALESCE( ─────────────────────────────────────────
    RewriteRule(
        name="nvl_to_coalesce",
        pattern=re.compile(
            r"\bNVL\s*\(", re.IGNORECASE
        ),
        replacement="COALESCE(",
        description="Replace NVL( with COALESCE(",
    ),

    # ── Rule 8: LEN( → LENGTH( ───────────────────────────────────────────
    RewriteRule(
        name="len_to_length",
        pattern=re.compile(
            r"\bLEN\s*\(", re.IGNORECASE
        ),
        replacement="LENGTH(",
        description="Replace LEN( with LENGTH(",
    ),

    # ── Rule 9: VARCHAR(n) / CHAR(n) / NVARCHAR(n) → STRING ──────────────
    RewriteRule(
        name="varchar_to_string",
        pattern=re.compile(
            r"\b(?:VAR)?(?:N)?CHAR(?:ACTER)?(?:\s+VARYING)?\s*\(\s*\d+\s*\)",
            re.IGNORECASE,
        ),
        replacement="STRING",
        description="Replace VARCHAR(n)/CHAR(n)/NVARCHAR(n) with STRING",
    ),

    # ── Rule 10: NUMERIC(p,s) / DECIMAL(p,s) → DECIMAL(p,s) ─────────────
    RewriteRule(
        name="numeric_to_decimal",
        pattern=re.compile(
            r"\bNUMERIC\s*(\(\s*\d+\s*(?:,\s*\d+\s*)?\))",
            re.IGNORECASE,
        ),
        replacement=r"DECIMAL\1",
        description="Replace NUMERIC(p,s) with DECIMAL(p,s)",
    ),

    # ── Rule 11: FLOAT4 → FLOAT, FLOAT8 → DOUBLE ─────────────────────────
    RewriteRule(
        name="float_types",
        pattern=re.compile(r"\bFLOAT4\b", re.IGNORECASE),
        replacement="FLOAT",
        description="Replace FLOAT4 with FLOAT",
    ),
    RewriteRule(
        name="float8_to_double",
        pattern=re.compile(r"\bFLOAT8\b", re.IGNORECASE),
        replacement="DOUBLE",
        description="Replace FLOAT8 with DOUBLE",
    ),

    # ── Rule 12: INT2 → SMALLINT, INT4 → INT, INT8 → BIGINT ─────────────
    RewriteRule(
        name="int2_to_smallint",
        pattern=re.compile(r"\bINT2\b", re.IGNORECASE),
        replacement="SMALLINT",
        description="Replace INT2 with SMALLINT",
    ),
    RewriteRule(
        name="int4_to_int",
        pattern=re.compile(r"\bINT4\b", re.IGNORECASE),
        replacement="INT",
        description="Replace INT4 with INT",
    ),
    RewriteRule(
        name="int8_to_bigint",
        pattern=re.compile(r"\bINT8\b", re.IGNORECASE),
        replacement="BIGINT",
        description="Replace INT8 with BIGINT",
    ),

    # ── Rule 13: BPCHAR → STRING ─────────────────────────────────────────
    RewriteRule(
        name="bpchar_to_string",
        pattern=re.compile(r"\bBPCHAR\b", re.IGNORECASE),
        replacement="STRING",
        description="Replace BPCHAR with STRING",
    ),

    # ── Rule 14: TIMESTAMPTZ → TIMESTAMP ─────────────────────────────────
    RewriteRule(
        name="timestamptz_to_timestamp",
        pattern=re.compile(r"\bTIMESTAMPTZ\b", re.IGNORECASE),
        replacement="TIMESTAMP",
        description="Replace TIMESTAMPTZ with TIMESTAMP",
    ),

    # ── Rule 15: CONVERT_TIMEZONE → FROM_UTC_TIMESTAMP (uncertain) ───────
    RewriteRule(
        name="convert_timezone",
        pattern=re.compile(
            r"\bCONVERT_TIMEZONE\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*([^)]+)\)",
            re.IGNORECASE,
        ),
        replacement=r"FROM_UTC_TIMESTAMP(TO_UTC_TIMESTAMP(\3, '\1'), '\2')",
        description="Convert CONVERT_TIMEZONE('from','to',expr) — verify timezone semantics",
        uncertain=True,
    ),

    # ── Rule 16: DATE_PART → EXTRACT ─────────────────────────────────────
    RewriteRule(
        name="date_part_to_extract",
        pattern=re.compile(
            r"\bDATE_PART\s*\(\s*'?(\w+)'?\s*,\s*([^)]+)\)",
            re.IGNORECASE,
        ),
        replacement=r"EXTRACT(\1 FROM \2)",
        description="Convert DATE_PART('part', expr) to EXTRACT(part FROM expr)",
    ),

    # ── Rule 17: CHARINDEX → LOCATE (swap arguments) ─────────────────────
    RewriteRule(
        name="charindex_to_locate",
        pattern=re.compile(
            r"\bCHARINDEX\s*\(\s*([^,]+?)\s*,\s*([^)]+)\)",
            re.IGNORECASE,
        ),
        replacement=r"LOCATE(\1, \2)",
        description="Convert CHARINDEX(substr, str) to LOCATE(substr, str)",
    ),

    # ── Rule 18: RANDOM() → RAND() ──────────────────────────────────────
    RewriteRule(
        name="random_to_rand",
        pattern=re.compile(r"\bRANDOM\s*\(\s*\)", re.IGNORECASE),
        replacement="RAND()",
        description="Replace RANDOM() with RAND()",
    ),

    # ── Rule 19: LISTAGG → CONCAT_WS (uncertain — semantics differ) ─────
    RewriteRule(
        name="listagg_to_concat_ws",
        pattern=re.compile(
            r"\bLISTAGG\s*\(\s*([^,)]+?)(?:\s*,\s*'([^']*)')?\s*\)"
            r"(?:\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+[^)]+\))?",
            re.IGNORECASE,
        ),
        replacement=r"CONCAT_WS('\2', COLLECT_LIST(\1))",
        description="Convert LISTAGG to CONCAT_WS+COLLECT_LIST — verify ordering semantics",
        uncertain=True,
    ),

    # ── Rule 20: STRTOL → CONV ───────────────────────────────────────────
    RewriteRule(
        name="strtol_to_conv",
        pattern=re.compile(
            r"\bSTRTOL\s*\(\s*([^,]+)\s*,\s*(\d+)\s*\)",
            re.IGNORECASE,
        ),
        replacement=r"CONV(\1, \2, 10)",
        description="Convert STRTOL(str, base) to CONV(str, base, 10)",
    ),

    # ── Rule 21: Remove BACKUP NO / BACKUP YES ──────────────────────────
    RewriteRule(
        name="remove_backup",
        pattern=re.compile(r"\bBACKUP\s+(?:YES|NO)\b", re.IGNORECASE),
        replacement="",
        description="Remove Redshift BACKUP YES/NO table property",
    ),

    # ── Rule 22: CREATE TABLE → CREATE TABLE (add USING DELTA) ───────────
    RewriteRule(
        name="add_using_delta",
        pattern=re.compile(
            r"(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+\S+\s*\([^;]+\))\s*;",
            re.IGNORECASE | re.DOTALL,
        ),
        replacement=r"\1\nUSING DELTA;",
        description="Append USING DELTA to CREATE TABLE statements",
    ),
]


# ═══════════════════════════════════════════════════════════════════════════
# Manual-rewrite detection patterns
# ═══════════════════════════════════════════════════════════════════════════

MANUAL_PATTERNS = [
    (re.compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\b", re.IGNORECASE),
     "Stored procedure — requires manual rewrite to Databricks notebook/workflow"),
    (re.compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b[\s\S]*?\bLANGUAGE\s+plpgsql\b",
                re.IGNORECASE),
     "PL/pgSQL UDF — procedural language not supported in Databricks"),
    (re.compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b[\s\S]*?\bLANGUAGE\s+plpythonu\b",
                re.IGNORECASE),
     "Python UDF (plpythonu) — rewrite as Databricks Python UDF or notebook"),
    (re.compile(r"\bCURSOR\b", re.IGNORECASE),
     "CURSOR usage — procedural pattern, no Spark SQL equivalent"),
    (re.compile(r"\bRAISE\s+(?:NOTICE|EXCEPTION|INFO)\b", re.IGNORECASE),
     "RAISE statement — plpgsql control flow, not supported in Spark SQL"),
    (re.compile(r"\bEXECUTE\s+'[^']*'\s*;", re.IGNORECASE),
     "Dynamic SQL EXECUTE — requires manual conversion"),
    (re.compile(r"\bFOR\s+\w+\s+IN\b.*?\bLOOP\b", re.IGNORECASE | re.DOTALL),
     "FOR ... IN ... LOOP — procedural control flow"),
    (re.compile(r"\bIF\s+.*?\bTHEN\b", re.IGNORECASE),
     "IF/THEN control flow — procedural pattern"),
    (re.compile(r"\bBEGIN\b[\s\S]*?\bEND\b\s*;", re.IGNORECASE),
     "BEGIN/END block — procedural stored procedure body"),
    (re.compile(r"\bCOPY\b\s+\w+", re.IGNORECASE),
     "COPY command — replace with Databricks COPY INTO or Auto Loader"),
    (re.compile(r"\bUNLOAD\b\s*\(", re.IGNORECASE),
     "UNLOAD command — replace with Databricks write to external location"),
    (re.compile(r"\bCREATE\s+EXTERNAL\s+SCHEMA\b", re.IGNORECASE),
     "External schema — map to Unity Catalog external location"),
    (re.compile(r"\bCREATE\s+LIBRARY\b", re.IGNORECASE),
     "CREATE LIBRARY — Redshift-specific, no Databricks equivalent"),
]


# ═══════════════════════════════════════════════════════════════════════════
# Transpile result
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TranspileResult:
    source_path: str
    source_hash: str
    original_sql: str
    transpiled_sql: str
    classification: Classification
    applied_rules: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    manual_reasons: list[str] = field(default_factory=list)
    llm_assisted: bool = False
    llm_audit: list[dict] = field(default_factory=list)
    difficulty_score: int = 0
    unified_diff: str = ""

    def to_dict(self) -> dict:
        return {
            "source_path": self.source_path,
            "source_hash": self.source_hash,
            "classification": self.classification.value,
            "applied_rules": self.applied_rules,
            "warnings": self.warnings,
            "manual_reasons": self.manual_reasons,
            "llm_assisted": self.llm_assisted,
            "llm_audit": self.llm_audit,
            "difficulty_score": self.difficulty_score,
            "conversion_diff": self.unified_diff,
        }


# ═══════════════════════════════════════════════════════════════════════════
# Difficulty scoring
# ═══════════════════════════════════════════════════════════════════════════

def _compute_difficulty(result: TranspileResult) -> int:
    """
    Estimate migration difficulty on a 1-10 scale.
    1-3  = trivial DDL / simple SELECT
    4-6  = moderate rewrites, some warnings
    7-8  = complex, LLM-assisted needed
    9-10 = procedural / manual rewrite required
    """
    score = 1
    sql = result.original_sql.lower()

    # Base complexity from SQL features
    if "join" in sql:
        score += 1
    if re.search(r"\bover\s*\(", sql):
        score += 1
    if sql.count("select") > 2:
        score += 1
    if len(sql) > 5000:
        score += 1

    # From conversion results
    score += min(len(result.applied_rules), 3)
    if result.warnings:
        score += len(result.warnings)
    if result.llm_assisted:
        score += 2
    if result.manual_reasons:
        score = max(score, 9)

    return min(score, 10)


# ═══════════════════════════════════════════════════════════════════════════
# LLM placeholder
# ═══════════════════════════════════════════════════════════════════════════

def _llm_translate(sql_fragment: str, context: str = "") -> dict:
    """
    Placeholder for LLM-assisted translation of complex expressions.

    In production, this would call an LLM API (e.g., OpenAI, Anthropic)
    with the SQL fragment and source/target dialect context.

    Returns an audit record with the original, suggested translation,
    and a confidence flag.
    """
    return {
        "original_fragment": sql_fragment[:500],
        "suggested_translation": None,
        "confidence": "NOT_AVAILABLE",
        "note": (
            "LLM translation placeholder — integrate with your preferred LLM API. "
            "Pass the fragment with context: 'Translate this Redshift SQL expression "
            "to Databricks Spark SQL'."
        ),
        "context": context,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Core transpilation
# ═══════════════════════════════════════════════════════════════════════════

def transpile_sql(
    sql: str,
    source_path: str = "<inline>",
    catalog: dict | None = None,
) -> TranspileResult:
    """
    Transpile a single SQL string from Redshift to Databricks dialect.

    Parameters
    ----------
    sql : str
        The source Redshift SQL/DDL.
    source_path : str
        File path (for reporting).
    catalog : dict, optional
        The source_catalog.json content (for context-aware transforms).

    Returns
    -------
    TranspileResult
    """
    source_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]
    result = TranspileResult(
        source_path=source_path,
        source_hash=source_hash,
        original_sql=sql,
        transpiled_sql=sql,
        classification=Classification.AUTO_CONVERT,
    )

    # ── Stage 0: Check for manual-rewrite patterns ────────────────────────
    for pattern, reason in MANUAL_PATTERNS:
        if pattern.search(sql):
            result.manual_reasons.append(reason)

    if result.manual_reasons:
        result.classification = Classification.MANUAL_REWRITE_REQUIRED
        # Still apply safe surface-level transforms for documentation
        logger.info(
            "[%s] Classified MANUAL_REWRITE_REQUIRED: %s",
            source_path, "; ".join(result.manual_reasons),
        )

    # ── Stage 1: Rule-based transforms ────────────────────────────────────
    current = result.transpiled_sql

    for rule in REWRITE_RULES:
        new_text, count = rule.pattern.subn(rule.replacement, current)
        if count > 0:
            result.applied_rules.append(f"{rule.name} (x{count}): {rule.description}")
            logger.debug(
                "[%s] Applied rule '%s' (%d replacements)",
                source_path, rule.name, count,
            )
            if rule.uncertain:
                result.warnings.append(
                    f"Rule '{rule.name}' applied but may need manual verification: "
                    f"{rule.description}"
                )
            current = new_text

    # Clean up extra whitespace from removals
    current = re.sub(r"\n\s*\n\s*\n", "\n\n", current)
    current = re.sub(r"  +", " ", current)
    result.transpiled_sql = current.strip()

    # ── Stage 2: LLM-assisted pass for remaining unknowns ─────────────────
    # Detect patterns that weren't handled by rules
    remaining_redshift_patterns = [
        (r"\bCREATE\s+EXTERNAL\s+TABLE\b", "External table DDL"),
        (r"\bSPECTRUM\b", "Redshift Spectrum reference"),
        (r"\bSUPER\b", "SUPER data type usage"),
        (r"\bHLL\b", "HyperLogLog sketch"),
        (r"\bAPPROXIMATE\b", "Approximate aggregate"),
        (r"\bGEOMETRY\b", "Geometry data type"),
    ]

    for pat_str, desc in remaining_redshift_patterns:
        pat = re.compile(pat_str, re.IGNORECASE)
        matches = pat.findall(result.transpiled_sql)
        if matches:
            audit = _llm_translate(
                result.transpiled_sql[:2000],
                context=f"Remaining Redshift pattern detected: {desc}",
            )
            result.llm_audit.append(audit)
            result.llm_assisted = True
            result.warnings.append(
                f"Residual Redshift pattern '{desc}' detected — flagged for LLM/manual review"
            )

    # ── Finalise classification ───────────────────────────────────────────
    if result.classification != Classification.MANUAL_REWRITE_REQUIRED:
        if result.warnings or result.llm_assisted:
            result.classification = Classification.CONVERT_WITH_WARNINGS
        else:
            result.classification = Classification.AUTO_CONVERT

    # ── Difficulty score ──────────────────────────────────────────────────
    result.difficulty_score = _compute_difficulty(result)

    # ── Unified diff ──────────────────────────────────────────────────────
    diff_lines = difflib.unified_diff(
        result.original_sql.splitlines(keepends=True),
        result.transpiled_sql.splitlines(keepends=True),
        fromfile=f"redshift/{source_path}",
        tofile=f"databricks/{source_path}",
        lineterm="",
    )
    result.unified_diff = "\n".join(diff_lines)

    logger.info(
        "[%s] %s — %d rules applied, %d warnings, difficulty=%d",
        source_path,
        result.classification.value,
        len(result.applied_rules),
        len(result.warnings),
        result.difficulty_score,
    )

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Batch transpilation
# ═══════════════════════════════════════════════════════════════════════════

def transpile_file(filepath: Path, catalog: dict | None = None) -> TranspileResult:
    """Read a SQL file and transpile it."""
    sql = filepath.read_text(encoding="utf-8", errors="replace")
    return transpile_sql(sql, source_path=str(filepath.name), catalog=catalog)


def transpile_directory(
    src_dir: Path,
    catalog: dict | None = None,
    extensions: tuple[str, ...] = (".sql", ".ddl", ".view", ".proc", ".udf"),
) -> list[TranspileResult]:
    """Transpile all SQL files in a directory."""
    results = []
    files = sorted(
        f for f in src_dir.rglob("*") if f.suffix.lower() in extensions and f.is_file()
    )
    logger.info("Found %d SQL files in %s", len(files), src_dir)
    for f in files:
        results.append(transpile_file(f, catalog=catalog))
    return results


def transpile_catalog_objects(
    catalog: dict,
) -> list[TranspileResult]:
    """
    Transpile DDL/SQL embedded in the source catalog (views, procs, UDFs).
    Useful when source SQL files aren't available separately.
    """
    results = []

    # Views
    for view in catalog.get("views", []):
        defn = view.get("view_definition") or ""
        if not defn.strip():
            continue
        schema = view.get("table_schema", "default")
        name = view.get("table_name", "unknown")
        source_path = f"views/{schema}.{name}.sql"

        ddl = f"CREATE OR REPLACE VIEW {schema}.{name} AS\n{defn}"
        results.append(transpile_sql(ddl, source_path=source_path, catalog=catalog))

    # Materialized views
    for mv in catalog.get("materialized_views", []):
        defn = mv.get("view_definition") or ""
        if not defn.strip():
            continue
        schema = mv.get("mv_schema", "default")
        name = mv.get("mv_name", "unknown")
        source_path = f"materialized_views/{schema}.{name}.sql"

        ddl = f"-- Materialized view (convert to scheduled job or Delta table)\n"
        ddl += f"CREATE OR REPLACE VIEW {schema}.{name} AS\n{defn}"
        results.append(transpile_sql(ddl, source_path=source_path, catalog=catalog))

    # Stored procedures
    for proc in catalog.get("procs", []):
        ddl = proc.get("ddl") or ""
        if not ddl.strip():
            continue
        schema = proc.get("proc_schema", "default")
        name = proc.get("proc_name", "unknown")
        source_path = f"procs/{schema}.{name}.sql"
        results.append(transpile_sql(ddl, source_path=source_path, catalog=catalog))

    # UDFs
    for udf in catalog.get("udfs", []):
        ddl = udf.get("ddl") or ""
        if not ddl.strip():
            continue
        schema = udf.get("udf_schema", "default")
        name = udf.get("udf_name", "unknown")
        source_path = f"udfs/{schema}.{name}.sql"
        results.append(transpile_sql(ddl, source_path=source_path, catalog=catalog))

    logger.info(
        "Transpiled %d catalog objects (views, MVs, procs, UDFs).", len(results)
    )
    return results
