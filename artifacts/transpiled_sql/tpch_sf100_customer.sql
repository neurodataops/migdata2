-- Object     : tpch_sf100.customer
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : c2b9e28aca066f7734afa2b5cbe7ce0bc41270c01e22e7b320dd79f6d1e8f00b
-- Generated  : 2026-03-05T17:04:00.699364
-- ---

CREATE TABLE tpch_sf100.customer (
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