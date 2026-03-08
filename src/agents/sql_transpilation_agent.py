"""
sql_transpilation_agent.py — SQL Transpilation Agent
=====================================================
Replaces the rule-based regex engine with a semantic LLM-driven converter.

Task 1: Accept a single CREATE TABLE DDL in any source dialect (Redshift,
         Snowflake, or mock) and produce equivalent Databricks CREATE TABLE DDL.
Output : {"object":"table","source_ddl":"...","target_ddl":"...","diff":"..."}

Task 2: Convert SELECT queries with simple JOINs (INNER/LEFT) preserving
         column aliases.
Output : {"sql":"...","explain":{"rules_applied":[],"warnings":[]}}

Task 3: Handle CTEs (WITH ... AS) with nested sub-CTEs and return a flattened
         target SQL.
Output : {"sql":"...","explain":{}}

Task 4: Recognize and translate window functions (ROW_NUMBER, RANK, SUM OVER,
         etc.) from source dialect to Databricks SQL equivalents.
Output : {"sql":"...","functions_mapped":[],"explain":{}}
"""

from __future__ import annotations

import difflib
import json
import re
import time
from typing import Any

from src.agents.base_agent import BaseAgent, AgentResult

# ── System prompt (shared across all tasks for this agent) ───────────────────

AGENT_SYSTEM_PROMPT = """\
You are the SQL Transpilation Agent for MigData, a data-migration platform.
Your job is to convert SQL DDL and DML from a source warehouse dialect
to Databricks-compatible SQL.

You support multiple source dialects: Redshift, Snowflake, and generic/mock SQL.
Auto-detect the dialect from the DDL syntax if not explicitly specified.

Rules you MUST follow:
1. Produce syntactically valid Databricks SQL (Spark SQL / Unity Catalog).
2. Map data types accurately:
   — Redshift types:
     VARCHAR(n), NVARCHAR(n), CHAR(n), BPCHAR → STRING
     INT2 → SMALLINT, INT4 → INT, INT8 → BIGINT
     FLOAT4 → FLOAT, FLOAT8 → DOUBLE
     NUMERIC(p,s) → DECIMAL(p,s)
     TIMESTAMPTZ → TIMESTAMP
   — Snowflake types:
     VARCHAR(n), CHAR(n), TEXT → STRING
     NUMBER(38,0) → BIGINT, NUMBER(p,s) → DECIMAL(p,s)
     VARIANT → STRING, OBJECT → STRING, ARRAY → ARRAY<STRING>
     TIMESTAMP_NTZ → TIMESTAMP, TIMESTAMP_TZ → TIMESTAMP, TIMESTAMP_LTZ → TIMESTAMP
     GEOGRAPHY → STRING, GEOMETRY → STRING
   — Common types:
     BOOLEAN / BOOL → BOOLEAN
     DATE → DATE, TIME → STRING
3. Remove source-specific physical/storage clauses:
   — Redshift: DISTKEY, SORTKEY, DISTSTYLE, ENCODE, BACKUP, COMPOUND SORTKEY,
     INTERLEAVED SORTKEY
   — Snowflake: CLUSTER BY, DATA_RETENTION_TIME_IN_DAYS, CHANGE_TRACKING,
     TRANSIENT, COPY GRANTS, MAX_DATA_EXTENSION_TIME_IN_DAYS, COMMENT
4. Keep column names, constraints (NOT NULL, DEFAULT, PRIMARY KEY) intact.
5. Append USING DELTA to every CREATE TABLE.
6. Preserve schema-qualified names (schema.table).
7. Do NOT add comments or explanations in the SQL itself.

Respond ONLY with valid JSON — no markdown fences, no prose.
"""


# ── Task 1 prompt template ───────────────────────────────────────────────────

TASK1_USER_PROMPT = """\
TASK: Convert the following {dialect} CREATE TABLE DDL to Databricks CREATE TABLE DDL.

Source DDL ({dialect}):
```sql
{source_ddl}
```

Respond with ONLY this JSON (no extra keys):
{{
  "object": "table",
  "source_ddl": "<original DDL verbatim>",
  "target_ddl": "<converted Databricks DDL>",
  "diff": "<unified diff showing changes>"
}}
"""


# ── Task 2 prompt template ───────────────────────────────────────────────────

TASK2_USER_PROMPT = """\
TASK: Convert the following {dialect} SELECT query to Databricks-compatible SQL.
The query contains JOIN clauses (INNER and/or LEFT).
You MUST preserve every column alias exactly as written.
You MUST preserve the join order (the sequence of tables in FROM / JOIN).

Source SQL ({dialect}):
```sql
{source_sql}
```

Respond with ONLY this JSON (no extra keys):
{{
  "sql": "<converted Databricks-compatible SELECT query>",
  "explain": {{
    "rules_applied": ["<list of transformation rules you applied>"],
    "warnings": ["<any potential issues or manual-review items>"]
  }}
}}
"""


# ── Task 3 prompt template ───────────────────────────────────────────────────

TASK3_USER_PROMPT = """\
TASK: Convert the following {dialect} SQL query that uses CTEs (WITH ... AS)
to Databricks-compatible SQL.

The query may contain:
- Multiple CTE blocks in a single WITH clause
- Nested sub-CTEs (a CTE that references another CTE defined earlier)
- CTEs that use source-specific functions or types

Requirements:
1. Preserve ALL CTE names exactly as written.
2. Preserve the CTE dependency order (a CTE that references another must come after it).
3. The final SELECT column list must produce the EXACT same column count and aliases
   as the original query.
4. Convert all source-dialect functions and types to Databricks equivalents.
5. The output SQL must be a single flattened statement (WITH ... SELECT ...) — do NOT
   break it into multiple statements.

Source SQL ({dialect}):
```sql
{source_sql}
```

Respond with ONLY this JSON (no extra keys):
{{
  "sql": "<converted Databricks-compatible SQL with CTEs>",
  "explain": {{
    "cte_names": ["<ordered list of CTE names found>"],
    "rules_applied": ["<list of transformation rules you applied>"],
    "warnings": ["<any potential issues or manual-review items>"],
    "column_count": <number of columns in the final SELECT>
  }}
}}
"""


# ── Task 4 prompt template ───────────────────────────────────────────────────

TASK4_USER_PROMPT = """\
TASK: Convert the following {dialect} SQL query that uses window functions
to Databricks-compatible SQL.

The query may contain any of these window functions:
- ROW_NUMBER(), RANK(), DENSE_RANK(), NTILE()
- SUM(...) OVER(...), COUNT(...) OVER(...), AVG(...) OVER(...),
  MIN(...) OVER(...), MAX(...) OVER(...)
- LAG(), LEAD(), FIRST_VALUE(), LAST_VALUE(), NTH_VALUE()
- PERCENT_RANK(), CUME_DIST(), RATIO_TO_REPORT()

Requirements:
1. Preserve every OVER(PARTITION BY ... ORDER BY ...) clause exactly — same
   partition columns in the same order, same ordering columns and directions.
2. Preserve every ROWS/RANGE frame specification if present.
3. Map source-specific window functions to Databricks equivalents:
   — Redshift MEDIAN(...) → PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ...) or approx
   — Redshift RATIO_TO_REPORT → manual SUM() OVER / SUM() OVER (entire partition)
   — Snowflake QUALIFY → wrap in subquery with WHERE on the window column
4. Convert all other source-dialect functions/types as usual.
5. Return a "functions_mapped" list with each window function found and how
   it was mapped.

Source SQL ({dialect}):
```sql
{source_sql}
```

Respond with ONLY this JSON (no extra keys):
{{
  "sql": "<converted Databricks-compatible SQL>",
  "functions_mapped": [
    {{"source": "<original function call>", "target": "<converted function call>", "rule": "<short rule description>"}}
  ],
  "explain": {{
    "rules_applied": ["<list of transformation rules you applied>"],
    "warnings": ["<any potential issues or manual-review items>"]
  }}
}}
"""


# ── Task 5 prompt template ───────────────────────────────────────────────────

TASK5_USER_PROMPT = """\
TASK: Detect and rewrite source-specific functions and data types in the
following {dialect} SQL to Databricks-compatible equivalents.

This task covers TWO concerns:

A) **Function rewriting** — detect source-specific functions and replace
   them with Databricks equivalents.  Common mappings:
   — Redshift:
     NVL(a,b) → COALESCE(a,b)
     NVL2(expr,a,b) → IF(expr IS NOT NULL, a, b)
     GETDATE() → CURRENT_TIMESTAMP()
     SYSDATE → CURRENT_TIMESTAMP()
     DATEADD(part,n,d) → DATE_ADD(d,n) or dateadd(part,n,d) where available
     DATEDIFF(part,a,b) → DATEDIFF(a,b)
     CONVERT(type,expr) → CAST(expr AS type)
     LEN(s) → LENGTH(s)
     CHARINDEX(sub,s) → LOCATE(sub,s)
     STRTOL(s,base) → CONV(s,base,10)
     LISTAGG(col,sep) → CONCAT_WS(sep, COLLECT_LIST(col))
     TOP n → LIMIT n
     ISNULL(expr,alt) → COALESCE(expr,alt)
     GETDATE()::DATE → CURRENT_DATE()
   — Snowflake:
     IFF(cond,a,b) → IF(cond,a,b)
     IFNULL(a,b) → COALESCE(a,b)
     ZEROIFNULL(a) → COALESCE(a,0)
     TRY_CAST(expr AS type) → TRY_CAST(expr AS type)  (same in Databricks)
     TO_VARIANT(x) → TO_JSON(x)
     PARSE_JSON(s) → FROM_JSON(s, schema)  (note: needs schema in Databricks)
     ARRAY_CONSTRUCT(...) → ARRAY(...)
     OBJECT_CONSTRUCT(...) → MAP(...)  or NAMED_STRUCT(...)
     FLATTEN(input=>arr) → EXPLODE(arr)
     SPLIT_PART(s,d,n) → SPLIT(s,d)[n-1]
     REGEXP_SUBSTR(s,p) → REGEXP_EXTRACT(s,p)
     CURRENT_TIMESTAMP()::DATE → CURRENT_DATE()
     DATEADD('day',n,d) → DATE_ADD(d,n)
     :: (cast operator) → CAST(... AS ...)

B) **Type mapping** — for every column or expression that uses a source type,
   provide a per-column mapping suggestion:
   — Snowflake VARIANT → STRING  (loss_warning: true — nested structure is serialised)
   — Snowflake ARRAY → ARRAY<STRING>  (loss_warning: false)
   — Snowflake OBJECT → MAP<STRING,STRING>  (loss_warning: true — typed keys lost)
   — Snowflake GEOGRAPHY/GEOMETRY → STRING  (loss_warning: true)
   — Redshift SUPER → STRING  (loss_warning: true)
   — Redshift HLLSKETCH → BINARY  (loss_warning: true)
   — Redshift TIMETZ → STRING  (loss_warning: true — timezone offset lost)

Source SQL ({dialect}):
```sql
{source_sql}
```

Respond with ONLY this JSON (no extra keys):
{{
  "sql": "<converted Databricks-compatible SQL>",
  "function_mappings": [
    {{"source_function": "<original>", "target_function": "<converted>", "count": <occurrences>}}
  ],
  "type_mappings": [
    {{"column": "<column or expression>", "source_type": "<original type>", "target_type": "<Databricks type>", "rationale": "<why this mapping>", "loss_warning": <true|false>}}
  ],
  "sample_replacements": [
    {{"original": "<source snippet>", "converted": "<target snippet>"}}
  ],
  "explain": {{
    "rules_applied": ["<list of transformation rules you applied>"],
    "warnings": ["<any potential issues, data-loss risks, or manual-review items>"]
  }}
}}
"""


class SQLTranspilationAgent(BaseAgent):
    """LLM-driven SQL transpilation agent."""

    agent_name = "sql_transpilation"
    agent_description = (
        "Semantic LLM-driven SQL converter that understands full query context, "
        "handles edge cases (window functions, CTEs, dynamic SQL), and produces "
        "optimised Databricks-compatible SQL."
    )

    # ── Task router ──────────────────────────────────────────────────────

    def execute(self, task_input: dict) -> AgentResult:
        task_id = task_input.get("task_id", 1)
        dispatch = {
            1: self._task1_create_table_ddl,
            2: self._task2_select_with_joins,
            3: self._task3_cte_queries,
            4: self._task4_window_functions,
        }
        handler = dispatch.get(task_id)
        if handler is None:
            return AgentResult(success=False, error=f"Unknown task_id: {task_id}")

        trace = self._observability.start_agent_trace(
            self.agent_name,
            f"task_{task_id}",
            task_input,
        )
        try:
            result = handler(task_input)
            self._observability.end_agent_trace(trace, result.success, result.data)
            return result
        except Exception as exc:
            self._observability.end_agent_trace(trace, False, error=str(exc))
            return AgentResult(success=False, error=str(exc))

    # ── Task 1 implementation ────────────────────────────────────────────

    @staticmethod
    def _detect_dialect(ddl: str) -> str:
        """Auto-detect the source dialect from DDL syntax."""
        upper = ddl.upper()
        # Redshift indicators
        if any(kw in upper for kw in ("DISTKEY", "SORTKEY", "DISTSTYLE", "ENCODE ", "INT4", "INT8", "BPCHAR")):
            return "Redshift"
        # Snowflake indicators
        if any(kw in upper for kw in ("VARIANT", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "CLUSTER BY",
                                       "DATA_RETENTION_TIME", "CHANGE_TRACKING", "TRANSIENT", "NUMBER(")):
            return "Snowflake"
        return "auto-detected"

    def _task1_create_table_ddl(self, task_input: dict) -> AgentResult:
        """
        Accept a single CREATE TABLE DDL in any source dialect (Redshift,
        Snowflake, or mock) and produce equivalent Databricks CREATE TABLE DDL.
        """
        source_ddl = task_input.get("source_ddl", "").strip()
        if not source_ddl:
            return AgentResult(success=False, error="source_ddl is required")

        # Resolve dialect: explicit parameter, or auto-detect from DDL
        dialect = task_input.get("source_dialect", "").strip()
        if not dialect:
            dialect = self._detect_dialect(source_ddl)

        user_prompt = TASK1_USER_PROMPT.format(source_ddl=source_ddl, dialect=dialect)

        t0 = time.time()
        llm_response = self.call_llm(
            system_prompt=AGENT_SYSTEM_PROMPT,
            user_prompt=user_prompt,
        )
        latency = (time.time() - t0) * 1000

        # Parse the LLM JSON response
        try:
            result = self.parse_json_response(llm_response.content)
        except (json.JSONDecodeError, ValueError) as exc:
            return AgentResult(
                success=False,
                error=f"LLM returned invalid JSON: {exc}\n\nRaw: {llm_response.content[:500]}",
                llm_calls=1,
                total_tokens=llm_response.input_tokens + llm_response.output_tokens,
                latency_ms=latency,
            )

        # Validate required keys
        for key in ("object", "source_ddl", "target_ddl", "diff"):
            if key not in result:
                return AgentResult(
                    success=False,
                    error=f"Missing key '{key}' in LLM response",
                    llm_calls=1,
                    total_tokens=llm_response.input_tokens + llm_response.output_tokens,
                    latency_ms=latency,
                )

        # Validation: target_ddl quality checks (dialect-agnostic)
        target_ddl: str = result["target_ddl"]
        validation_issues = []

        # Common checks — no source types should survive
        if "VARCHAR" in target_ddl.upper():
            validation_issues.append("target_ddl still contains VARCHAR — should be STRING")
        if "USING DELTA" not in target_ddl:
            validation_issues.append("target_ddl missing USING DELTA clause")

        # Redshift-specific remnants
        if "DISTKEY" in target_ddl.upper():
            validation_issues.append("target_ddl still contains DISTKEY — should be removed")
        if "SORTKEY" in target_ddl.upper():
            validation_issues.append("target_ddl still contains SORTKEY — should be removed")
        if "DISTSTYLE" in target_ddl.upper():
            validation_issues.append("target_ddl still contains DISTSTYLE — should be removed")
        if "ENCODE " in target_ddl.upper():
            validation_issues.append("target_ddl still contains ENCODE — should be removed")

        # Snowflake-specific remnants
        if "CLUSTER BY" in target_ddl.upper():
            validation_issues.append("target_ddl still contains CLUSTER BY — should be removed")
        if "DATA_RETENTION_TIME" in target_ddl.upper():
            validation_issues.append("target_ddl still contains DATA_RETENTION_TIME — should be removed")
        if "CHANGE_TRACKING" in target_ddl.upper():
            validation_issues.append("target_ddl still contains CHANGE_TRACKING — should be removed")
        if "TIMESTAMP_NTZ" in target_ddl.upper() or "TIMESTAMP_LTZ" in target_ddl.upper():
            validation_issues.append("target_ddl still contains Snowflake TIMESTAMP variant — should be TIMESTAMP")

        # Recompute diff to be sure it's accurate
        computed_diff = "\n".join(difflib.unified_diff(
            source_ddl.splitlines(),
            target_ddl.splitlines(),
            fromfile=f"source/{dialect.lower()}",
            tofile="target/databricks",
            lineterm="",
        ))
        result["diff"] = computed_diff
        result["source_dialect"] = dialect

        result["validation_issues"] = validation_issues
        result["llm_provider"] = llm_response.provider
        result["llm_model"] = llm_response.model
        result["input_tokens"] = llm_response.input_tokens
        result["output_tokens"] = llm_response.output_tokens
        result["latency_ms"] = round(latency, 1)

        return AgentResult(
            success=len(validation_issues) == 0,
            data=result,
            error="; ".join(validation_issues) if validation_issues else None,
            llm_calls=1,
            total_tokens=llm_response.input_tokens + llm_response.output_tokens,
            latency_ms=latency,
        )

    # ── Task 2 implementation ────────────────────────────────────────────

    @staticmethod
    def _extract_aliases(sql: str) -> set[str]:
        """Extract column aliases from a SELECT query (AS <alias> patterns)."""
        # Match   expr AS alias   and   expr AS "alias"
        pattern = r'\bAS\s+("?)(\w+)\1'
        return {m[1] for m in re.findall(pattern, sql, re.IGNORECASE)}

    @staticmethod
    def _extract_join_tables(sql: str) -> list[str]:
        """Extract the ordered list of tables/aliases that appear in FROM/JOIN clauses."""
        tables: list[str] = []
        # FROM table
        from_match = re.search(r'\bFROM\s+([\w.]+)', sql, re.IGNORECASE)
        if from_match:
            tables.append(from_match.group(1))
        # JOIN table
        for m in re.finditer(r'\bJOIN\s+([\w.]+)', sql, re.IGNORECASE):
            tables.append(m.group(1))
        return tables

    def _task2_select_with_joins(self, task_input: dict) -> AgentResult:
        """
        Convert SELECT queries with simple JOINs (INNER/LEFT) preserving
        column aliases and join ordering.
        """
        source_sql = task_input.get("source_sql", "").strip()
        if not source_sql:
            return AgentResult(success=False, error="source_sql is required")

        dialect = task_input.get("source_dialect", "").strip()
        if not dialect:
            dialect = self._detect_dialect(source_sql)

        user_prompt = TASK2_USER_PROMPT.format(source_sql=source_sql, dialect=dialect)

        t0 = time.time()
        llm_response = self.call_llm(
            system_prompt=AGENT_SYSTEM_PROMPT,
            user_prompt=user_prompt,
        )
        latency = (time.time() - t0) * 1000

        # Parse JSON
        try:
            result = self.parse_json_response(llm_response.content)
        except (json.JSONDecodeError, ValueError) as exc:
            return AgentResult(
                success=False,
                error=f"LLM returned invalid JSON: {exc}\n\nRaw: {llm_response.content[:500]}",
                llm_calls=1,
                total_tokens=llm_response.input_tokens + llm_response.output_tokens,
                latency_ms=latency,
            )

        # Validate required schema
        if "sql" not in result:
            return AgentResult(
                success=False,
                error="Missing key 'sql' in LLM response",
                llm_calls=1,
                total_tokens=llm_response.input_tokens + llm_response.output_tokens,
                latency_ms=latency,
            )
        if "explain" not in result or not isinstance(result["explain"], dict):
            result["explain"] = {"rules_applied": [], "warnings": []}
        result["explain"].setdefault("rules_applied", [])
        result["explain"].setdefault("warnings", [])

        converted_sql: str = result["sql"]
        validation_issues: list[str] = []

        # ── Validation: aliases preserved ────────────────────────────────
        source_aliases = self._extract_aliases(source_sql)
        target_aliases = self._extract_aliases(converted_sql)
        missing_aliases = source_aliases - target_aliases
        if missing_aliases:
            validation_issues.append(
                f"Aliases lost in conversion: {', '.join(sorted(missing_aliases))}"
            )

        # ── Validation: join order preserved ─────────────────────────────
        source_tables = self._extract_join_tables(source_sql)
        target_tables = self._extract_join_tables(converted_sql)
        if source_tables and target_tables and source_tables != target_tables:
            validation_issues.append(
                f"Join order changed: source={source_tables}, target={target_tables}"
            )

        # ── Validation: source-specific syntax should not survive ────────
        upper_sql = converted_sql.upper()
        if "NVL(" in upper_sql:
            validation_issues.append("NVL() should be converted to COALESCE()")
        if "GETDATE()" in upper_sql:
            validation_issues.append("GETDATE() should be converted to CURRENT_TIMESTAMP()")
        if "SYSDATE" in upper_sql and "CURRENT_TIMESTAMP" not in upper_sql:
            validation_issues.append("SYSDATE should be converted to CURRENT_TIMESTAMP()")
        if "::" in converted_sql:
            validation_issues.append("Double-colon casts (::) should be converted to CAST()")
        if "ILIKE" in upper_sql:
            # Spark SQL doesn't have ILIKE natively (added in 3.3+ but warn)
            result["explain"]["warnings"].append("ILIKE used — verify Databricks Runtime version supports it")

        # Attach metadata
        result["source_dialect"] = dialect
        result["validation_issues"] = validation_issues
        result["llm_provider"] = llm_response.provider
        result["llm_model"] = llm_response.model
        result["input_tokens"] = llm_response.input_tokens
        result["output_tokens"] = llm_response.output_tokens
        result["latency_ms"] = round(latency, 1)

        return AgentResult(
            success=len(validation_issues) == 0,
            data=result,
            error="; ".join(validation_issues) if validation_issues else None,
            llm_calls=1,
            total_tokens=llm_response.input_tokens + llm_response.output_tokens,
            latency_ms=latency,
        )

    # ── Task 3 implementation ────────────────────────────────────────────

    @staticmethod
    def _extract_cte_names(sql: str) -> list[str]:
        """Extract ordered CTE names from a WITH clause."""
        names: list[str] = []
        # Match CTE definitions: WITH name AS ( ... or , name AS ( ...
        # Also handles WITH RECURSIVE
        upper = sql.upper().strip()
        if not upper.startswith("WITH"):
            return names
        # Remove leading WITH (and optional RECURSIVE)
        body = re.sub(r'^\s*WITH\s+(RECURSIVE\s+)?', '', sql, count=1, flags=re.IGNORECASE)
        # Each CTE: name AS (
        for m in re.finditer(r'(?:^|,)\s*(\w+)\s+AS\s*\(', body, re.IGNORECASE):
            names.append(m.group(1))
        return names

    @staticmethod
    def _count_final_select_columns(sql: str) -> int | None:
        """
        Count columns in the final SELECT of a CTE query.

        Walks backward through the SQL to find the outermost SELECT
        (at parenthesis depth 0), then counts comma-separated expressions
        between SELECT and FROM, skipping commas inside nested parentheses.
        Returns None if the count cannot be reliably determined (e.g. SELECT *).
        """
        upper = sql.upper()

        # Walk from end, tracking paren depth, find last depth-0 SELECT
        depth = 0
        last_select_pos: int | None = None
        i = len(sql) - 1
        while i >= 0:
            ch = sql[i]
            if ch == ')':
                depth += 1
            elif ch == '(':
                depth -= 1
            elif depth == 0 and upper[i:i + 6] == 'SELECT':
                before = sql[i - 1] if i > 0 else ' '
                after = sql[i + 6] if i + 6 < len(sql) else ' '
                if not before.isalnum() and before != '_' and not after.isalnum() and after != '_':
                    last_select_pos = i
                    break
            i -= 1

        if last_select_pos is None:
            return None

        # Extract column list between SELECT and FROM
        after_select = sql[last_select_pos + 6:]
        from_match = re.search(r'\bFROM\b', after_select, re.IGNORECASE)
        if not from_match:
            return None

        col_list = after_select[:from_match.start()].strip()
        if not col_list:
            return None

        # SELECT * — can't count statically
        cleaned = re.sub(r'^\s*(DISTINCT|ALL)\s+', '', col_list, flags=re.IGNORECASE).strip()
        if cleaned == '*':
            return None

        # Count commas at depth 0
        count = 1
        depth = 0
        for ch in col_list:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                count += 1
        return count

    def _task3_cte_queries(self, task_input: dict) -> AgentResult:
        """
        Handle CTEs (WITH ... AS) with nested sub-CTEs and return a
        flattened target SQL.
        """
        source_sql = task_input.get("source_sql", "").strip()
        if not source_sql:
            return AgentResult(success=False, error="source_sql is required")

        dialect = task_input.get("source_dialect", "").strip()
        if not dialect:
            dialect = self._detect_dialect(source_sql)

        user_prompt = TASK3_USER_PROMPT.format(source_sql=source_sql, dialect=dialect)

        t0 = time.time()
        llm_response = self.call_llm(
            system_prompt=AGENT_SYSTEM_PROMPT,
            user_prompt=user_prompt,
        )
        latency = (time.time() - t0) * 1000

        # Parse JSON
        try:
            result = self.parse_json_response(llm_response.content)
        except (json.JSONDecodeError, ValueError) as exc:
            return AgentResult(
                success=False,
                error=f"LLM returned invalid JSON: {exc}\n\nRaw: {llm_response.content[:500]}",
                llm_calls=1,
                total_tokens=llm_response.input_tokens + llm_response.output_tokens,
                latency_ms=latency,
            )

        # Validate required schema
        if "sql" not in result:
            return AgentResult(
                success=False,
                error="Missing key 'sql' in LLM response",
                llm_calls=1,
                total_tokens=llm_response.input_tokens + llm_response.output_tokens,
                latency_ms=latency,
            )
        if "explain" not in result or not isinstance(result["explain"], dict):
            result["explain"] = {}
        result["explain"].setdefault("cte_names", [])
        result["explain"].setdefault("rules_applied", [])
        result["explain"].setdefault("warnings", [])
        result["explain"].setdefault("column_count", None)

        converted_sql: str = result["sql"]
        validation_issues: list[str] = []

        # ── Validation 1: CTE names preserved ────────────────────────────
        source_ctes = self._extract_cte_names(source_sql)
        target_ctes = self._extract_cte_names(converted_sql)

        if source_ctes:
            missing_ctes = set(c.lower() for c in source_ctes) - set(c.lower() for c in target_ctes)
            if missing_ctes:
                validation_issues.append(
                    f"CTE names lost in conversion: {', '.join(sorted(missing_ctes))}"
                )

            # CTE dependency order check
            source_order = [c.lower() for c in source_ctes]
            target_order = [c.lower() for c in target_ctes]
            target_filtered = [c for c in target_order if c in set(source_order)]
            if target_filtered != source_order:
                validation_issues.append(
                    f"CTE order changed: source={source_order}, target={target_filtered}"
                )

        # ── Validation 2: single flattened statement ─────────────────────
        stmt_count = len([s for s in converted_sql.split(';') if s.strip()])
        if stmt_count > 1:
            validation_issues.append(
                f"Output contains {stmt_count} statements — must be a single flattened WITH...SELECT"
            )

        # ── Validation 3: column count match ─────────────────────────────
        source_col_count = self._count_final_select_columns(source_sql)
        target_col_count = self._count_final_select_columns(converted_sql)
        if source_col_count is not None and target_col_count is not None:
            if source_col_count != target_col_count:
                validation_issues.append(
                    f"Column count mismatch: source={source_col_count}, target={target_col_count}"
                )
            result["explain"]["column_count"] = target_col_count
        elif target_col_count is not None:
            result["explain"]["column_count"] = target_col_count

        # ── Validation 4: source-dialect remnants ────────────────────────
        upper_sql = converted_sql.upper()
        if "NVL(" in upper_sql:
            validation_issues.append("NVL() should be converted to COALESCE()")
        if "GETDATE()" in upper_sql:
            validation_issues.append("GETDATE() should be converted to CURRENT_TIMESTAMP()")
        if "::" in converted_sql:
            validation_issues.append("Double-colon casts (::) should be converted to CAST()")
        if "CONVERT_TIMEZONE" in upper_sql:
            validation_issues.append("CONVERT_TIMEZONE should be converted to FROM_UTC_TIMESTAMP/TO_UTC_TIMESTAMP")
        if "LISTAGG" in upper_sql:
            validation_issues.append("LISTAGG should be converted to CONCAT_WS(COLLECT_LIST(...))")

        # Attach metadata
        result["source_dialect"] = dialect
        result["source_cte_names"] = source_ctes
        result["validation_issues"] = validation_issues
        result["llm_provider"] = llm_response.provider
        result["llm_model"] = llm_response.model
        result["input_tokens"] = llm_response.input_tokens
        result["output_tokens"] = llm_response.output_tokens
        result["latency_ms"] = round(latency, 1)

        return AgentResult(
            success=len(validation_issues) == 0,
            data=result,
            error="; ".join(validation_issues) if validation_issues else None,
            llm_calls=1,
            total_tokens=llm_response.input_tokens + llm_response.output_tokens,
            latency_ms=latency,
        )

    # ── Task 4 implementation ────────────────────────────────────────────

    # Known window functions for validation
    _WINDOW_FUNCTIONS = {
        "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
        "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
        "PERCENT_RANK", "CUME_DIST",
        "SUM", "COUNT", "AVG", "MIN", "MAX",
        "MEDIAN", "RATIO_TO_REPORT", "LISTAGG",
        "PERCENTILE_CONT", "PERCENTILE_DISC",
    }

    @staticmethod
    def _extract_over_clauses(sql: str) -> list[dict]:
        """
        Extract every OVER(...) clause from a SQL string.

        Returns a list of dicts with:
            func       – the function name preceding OVER
            partition   – list of PARTITION BY column expressions (empty if none)
            order       – list of ORDER BY column expressions (empty if none)
            frame       – the ROWS/RANGE frame spec string (empty if none)
            raw         – the full raw OVER(...) content
        """
        results: list[dict] = []
        upper = sql.upper()

        # Find each  OVER  (  ...  )  — need to match balanced parens
        for m in re.finditer(r'(\w+)\s*\([^)]*\)\s*OVER\s*\(', sql, re.IGNORECASE):
            func_name = m.group(1).upper()
            # Walk forward from the opening paren of OVER( to find balanced close
            start = sql.index('(', m.end() - 1)
            depth = 1
            pos = start + 1
            while pos < len(sql) and depth > 0:
                if sql[pos] == '(':
                    depth += 1
                elif sql[pos] == ')':
                    depth -= 1
                pos += 1
            over_content = sql[start + 1:pos - 1].strip()
            over_upper = over_content.upper()

            # Parse PARTITION BY
            partition: list[str] = []
            pb_match = re.search(r'PARTITION\s+BY\s+(.+?)(?=ORDER\s+BY|ROWS\s|RANGE\s|$)',
                                 over_content, re.IGNORECASE | re.DOTALL)
            if pb_match:
                partition = [c.strip() for c in pb_match.group(1).split(',') if c.strip()]

            # Parse ORDER BY
            order: list[str] = []
            ob_match = re.search(r'ORDER\s+BY\s+(.+?)(?=ROWS\s|RANGE\s|$)',
                                 over_content, re.IGNORECASE | re.DOTALL)
            if ob_match:
                order = [c.strip() for c in ob_match.group(1).split(',') if c.strip()]

            # Parse frame (ROWS / RANGE)
            frame = ""
            fr_match = re.search(r'((?:ROWS|RANGE)\s+.+)', over_content, re.IGNORECASE)
            if fr_match:
                frame = fr_match.group(1).strip()

            results.append({
                "func": func_name,
                "partition": partition,
                "order": order,
                "frame": frame,
                "raw": over_content,
            })

        return results

    def _task4_window_functions(self, task_input: dict) -> AgentResult:
        """
        Recognize and translate window functions (ROW_NUMBER, RANK, SUM OVER,
        etc.) from source dialect to Databricks SQL equivalents.
        """
        source_sql = task_input.get("source_sql", "").strip()
        if not source_sql:
            return AgentResult(success=False, error="source_sql is required")

        dialect = task_input.get("source_dialect", "").strip()
        if not dialect:
            dialect = self._detect_dialect(source_sql)

        user_prompt = TASK4_USER_PROMPT.format(source_sql=source_sql, dialect=dialect)

        t0 = time.time()
        llm_response = self.call_llm(
            system_prompt=AGENT_SYSTEM_PROMPT,
            user_prompt=user_prompt,
        )
        latency = (time.time() - t0) * 1000

        # Parse JSON
        try:
            result = self.parse_json_response(llm_response.content)
        except (json.JSONDecodeError, ValueError) as exc:
            return AgentResult(
                success=False,
                error=f"LLM returned invalid JSON: {exc}\n\nRaw: {llm_response.content[:500]}",
                llm_calls=1,
                total_tokens=llm_response.input_tokens + llm_response.output_tokens,
                latency_ms=latency,
            )

        # Validate required schema
        if "sql" not in result:
            return AgentResult(
                success=False,
                error="Missing key 'sql' in LLM response",
                llm_calls=1,
                total_tokens=llm_response.input_tokens + llm_response.output_tokens,
                latency_ms=latency,
            )
        if not isinstance(result.get("functions_mapped"), list):
            result["functions_mapped"] = []
        if "explain" not in result or not isinstance(result["explain"], dict):
            result["explain"] = {}
        result["explain"].setdefault("rules_applied", [])
        result["explain"].setdefault("warnings", [])

        converted_sql: str = result["sql"]
        validation_issues: list[str] = []

        # ── Validation 1: OVER(PARTITION BY ...) clauses preserved ────────
        source_overs = self._extract_over_clauses(source_sql)
        target_overs = self._extract_over_clauses(converted_sql)

        # Check that every source PARTITION BY set appears in the target
        for src_ov in source_overs:
            if not src_ov["partition"]:
                continue  # no partition clause to verify
            src_parts_lower = [p.lower().strip() for p in src_ov["partition"]]
            found = False
            for tgt_ov in target_overs:
                tgt_parts_lower = [p.lower().strip() for p in tgt_ov["partition"]]
                if src_parts_lower == tgt_parts_lower:
                    found = True
                    break
            if not found:
                validation_issues.append(
                    f"PARTITION BY ({', '.join(src_ov['partition'])}) from "
                    f"{src_ov['func']} not found in target SQL"
                )

        # Check that every source ORDER BY set appears in the target
        for src_ov in source_overs:
            if not src_ov["order"]:
                continue
            src_ord_lower = [o.lower().strip() for o in src_ov["order"]]
            found = False
            for tgt_ov in target_overs:
                tgt_ord_lower = [o.lower().strip() for o in tgt_ov["order"]]
                if src_ord_lower == tgt_ord_lower:
                    found = True
                    break
            if not found:
                validation_issues.append(
                    f"ORDER BY ({', '.join(src_ov['order'])}) from "
                    f"{src_ov['func']} not found in target SQL"
                )

        # ── Validation 2: window function count preserved ─────────────────
        if len(source_overs) != len(target_overs):
            validation_issues.append(
                f"Window function count changed: source={len(source_overs)}, "
                f"target={len(target_overs)}"
            )

        # ── Validation 3: frame specs preserved if present ────────────────
        for src_ov in source_overs:
            if not src_ov["frame"]:
                continue
            # Normalise whitespace for comparison
            src_frame = re.sub(r'\s+', ' ', src_ov["frame"].upper())
            found = False
            for tgt_ov in target_overs:
                tgt_frame = re.sub(r'\s+', ' ', tgt_ov["frame"].upper())
                if src_frame == tgt_frame:
                    found = True
                    break
            if not found:
                validation_issues.append(
                    f"Frame spec '{src_ov['frame']}' from {src_ov['func']} "
                    f"not preserved in target SQL"
                )

        # ── Validation 4: source-dialect remnants ─────────────────────────
        upper_conv = converted_sql.upper()
        if "NVL(" in upper_conv:
            validation_issues.append("NVL() should be converted to COALESCE()")
        if "GETDATE()" in upper_conv:
            validation_issues.append("GETDATE() should be converted to CURRENT_TIMESTAMP()")
        if "::" in converted_sql:
            validation_issues.append("Double-colon casts (::) should be converted to CAST()")
        # Redshift MEDIAN is not valid in Databricks
        if re.search(r'\bMEDIAN\s*\(', upper_conv):
            validation_issues.append("MEDIAN() is not supported in Databricks — use PERCENTILE_CONT")
        # Snowflake QUALIFY is not valid Spark SQL
        if re.search(r'\bQUALIFY\b', upper_conv):
            validation_issues.append("QUALIFY clause is not supported in Databricks — use subquery + WHERE")

        # Attach metadata
        result["source_dialect"] = dialect
        result["source_window_functions"] = [
            {"func": o["func"], "partition": o["partition"], "order": o["order"], "frame": o["frame"]}
            for o in source_overs
        ]
        result["validation_issues"] = validation_issues
        result["llm_provider"] = llm_response.provider
        result["llm_model"] = llm_response.model
        result["input_tokens"] = llm_response.input_tokens
        result["output_tokens"] = llm_response.output_tokens
        result["latency_ms"] = round(latency, 1)

        return AgentResult(
            success=len(validation_issues) == 0,
            data=result,
            error="; ".join(validation_issues) if validation_issues else None,
            llm_calls=1,
            total_tokens=llm_response.input_tokens + llm_response.output_tokens,
            latency_ms=latency,
        )
