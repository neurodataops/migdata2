-- Object     : tpch_sf100.region
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : b17cde720fbec4219286f2f2490b42dd7e2e374ddf608ed6a80efdf23eb6362a
-- Generated  : 2026-03-08T16:35:47.532890
-- ---

CREATE TABLE tpch_sf100.region (
 r_regionkey NUMBER NOT NULL,
 r_name TEXT NOT NULL,
 r_comment TEXT
)

USING DELTA;