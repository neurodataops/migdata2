-- Object     : tpch_sf1.customer
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 3de24b1921ac55d80c95c1fc7e2a535a16487785b7746cd6e92d910f18116465
-- Generated  : 2026-03-05T17:04:00.667349
-- ---

CREATE TABLE tpch_sf1.customer (
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