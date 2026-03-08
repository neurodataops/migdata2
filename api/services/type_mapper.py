"""
type_mapper.py — Source-to-Databricks type mapping
Ported from app.py: REDSHIFT_TO_DATABRICKS_TYPE, SNOWFLAKE_TO_DATABRICKS_TYPE, map_source_type()
"""

import re

REDSHIFT_TO_DATABRICKS_TYPE = {
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
}

SNOWFLAKE_TO_DATABRICKS_TYPE = {
    "boolean": "BOOLEAN",
    "number": "DECIMAL", "numeric": "DECIMAL", "decimal": "DECIMAL",
    "int": "INT", "integer": "INT",
    "bigint": "BIGINT", "smallint": "SMALLINT", "tinyint": "TINYINT",
    "float": "DOUBLE", "float4": "FLOAT", "float8": "DOUBLE",
    "double": "DOUBLE", "double precision": "DOUBLE", "real": "FLOAT",
    "varchar": "STRING", "char": "STRING", "character": "STRING",
    "string": "STRING", "text": "STRING",
    "binary": "BINARY", "varbinary": "BINARY",
    "date": "DATE",
    "timestamp": "TIMESTAMP", "timestamp_ntz": "TIMESTAMP",
    "timestamp_ltz": "TIMESTAMP", "timestamp_tz": "TIMESTAMP",
    "time": "STRING",
    "variant": "STRING", "object": "STRING", "array": "ARRAY<STRING>",
    "geography": "STRING", "geometry": "STRING",
}


def map_source_type(source_type: str, platform: str = "snowflake") -> str:
    """Map a source data type to Databricks equivalent based on platform."""
    base = re.sub(r"\(.*\)", "", source_type.lower()).strip()
    type_map = (
        SNOWFLAKE_TO_DATABRICKS_TYPE if platform == "snowflake"
        else REDSHIFT_TO_DATABRICKS_TYPE
    )
    mapped = type_map.get(base)
    if mapped:
        if mapped == "DECIMAL" and "(" in source_type:
            precision = re.search(r"\(([^)]+)\)", source_type)
            if precision:
                return f"DECIMAL({precision.group(1)})"
        if platform == "snowflake" and base == "number":
            precision = re.search(r"\((\d+)\s*,\s*0\s*\)", source_type)
            if precision and int(precision.group(1)) == 38:
                return "BIGINT"
        return mapped
    return source_type.upper()
