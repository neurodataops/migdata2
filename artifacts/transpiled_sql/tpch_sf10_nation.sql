-- Object     : tpch_sf10.nation
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : c4b0d6eebc255a1216bd72ea15c18c896903c92982e87d7f0c7c53a02a7f52ae
-- Generated  : 2026-03-05T17:04:00.687347
-- ---

CREATE TABLE tpch_sf10.nation (
 n_nationkey NUMBER NOT NULL,
 n_name TEXT NOT NULL,
 n_regionkey NUMBER NOT NULL,
 n_comment TEXT
)

USING DELTA;