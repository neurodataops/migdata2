-- Object     : tpch_sf100.supplier
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 28540c99034815e1c4b1de14282f3d64cf6ea6db30d0e02dacbd452d562fedf5
-- Generated  : 2026-03-05T17:04:00.711350
-- ---

CREATE TABLE tpch_sf100.supplier (
 s_suppkey NUMBER NOT NULL,
 s_name TEXT NOT NULL,
 s_address TEXT NOT NULL,
 s_nationkey NUMBER NOT NULL,
 s_phone TEXT NOT NULL,
 s_acctbal NUMBER NOT NULL,
 s_comment TEXT
)

USING DELTA;