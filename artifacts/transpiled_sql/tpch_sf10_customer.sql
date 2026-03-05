-- Object     : tpch_sf10.customer
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : d412581bafef3facacea2affe4bfd91cabb86f39ef71cf0271113c6ff9b0c9d3
-- Generated  : 2026-03-05T17:04:00.684365
-- ---

CREATE TABLE tpch_sf10.customer (
 c_custkey NUMBER NOT NULL,
 c_name TEXT NOT NULL,
 c_address TEXT NOT NULL,
 c_nationkey NUMBER NOT NULL,
 c_phone TEXT NOT NULL,
 c_acctbal NUMBER NOT NULL,
 c_mktsegment TEXT,
 c_comment TEXT
)

USING DELTA;