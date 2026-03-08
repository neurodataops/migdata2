-- Object     : tpch_sf1000.region
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 969d412e5808697a4bc291751669bdb49d03f0f76f9772eec460c100b9d5527b
-- Generated  : 2026-03-08T16:35:47.578839
-- ---

CREATE TABLE tpch_sf1000.region (
 r_regionkey NUMBER NOT NULL,
 r_name TEXT NOT NULL,
 r_comment TEXT
)

USING DELTA;