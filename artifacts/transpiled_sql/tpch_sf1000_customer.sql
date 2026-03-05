-- Object     : tpch_sf1000.customer
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : cd3893133cbcfe13a4e4a7664bcc4d94c5b32e7d067ee7cf94b21a04f626a2a6
-- Generated  : 2026-03-05T17:04:00.713351
-- ---

CREATE TABLE tpch_sf1000.customer (
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