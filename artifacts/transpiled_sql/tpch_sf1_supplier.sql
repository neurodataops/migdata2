-- Object     : tpch_sf1.supplier
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : f246db7ff0817316cc680521eabc9b1579dfa88114da664e7988541af302afaa
-- Generated  : 2026-03-05T17:04:00.679346
-- ---

CREATE TABLE tpch_sf1.supplier (
 s_suppkey NUMBER NOT NULL,
 s_name TEXT NOT NULL,
 s_address TEXT NOT NULL,
 s_nationkey NUMBER NOT NULL,
 s_phone TEXT NOT NULL,
 s_acctbal NUMBER NOT NULL,
 s_comment TEXT
)

USING DELTA;