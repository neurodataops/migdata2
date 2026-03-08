-- Object     : tpch_sf10.supplier
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 7acfbc5e431c48ae2836a683ef08b70f25f7dfd89cfb71c276756a421ad99dd8
-- Generated  : 2026-03-08T16:35:47.490345
-- ---

CREATE TABLE tpch_sf10.supplier (
 s_suppkey NUMBER NOT NULL,
 s_name TEXT NOT NULL,
 s_address TEXT NOT NULL,
 s_nationkey NUMBER NOT NULL,
 s_phone TEXT NOT NULL,
 s_acctbal NUMBER NOT NULL,
 s_comment TEXT
)

USING DELTA;