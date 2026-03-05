-- Object     : tpch_sf10.part
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : ce21a7a26dc2ddd9614071349cb0af5635385eb58bac0e46a50023b8349f6d84
-- Generated  : 2026-03-05T17:04:00.691351
-- ---

CREATE TABLE tpch_sf10.part (
 p_partkey NUMBER NOT NULL,
 p_name TEXT NOT NULL,
 p_mfgr TEXT NOT NULL,
 p_brand TEXT NOT NULL,
 p_type TEXT NOT NULL,
 p_size NUMBER NOT NULL,
 p_container TEXT NOT NULL,
 p_retailprice NUMBER NOT NULL,
 p_comment TEXT
)

USING DELTA;