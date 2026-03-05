-- Object     : tpch_sf1000.supplier
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 83a92af6d1b348b56819acee74d211f8425d6dea31da8a77b4fe5372507bc220
-- Generated  : 2026-03-05T17:04:00.725352
-- ---

CREATE TABLE tpch_sf1000.supplier (
 s_suppkey NUMBER NOT NULL,
 s_name TEXT NOT NULL,
 s_address TEXT NOT NULL,
 s_nationkey NUMBER NOT NULL,
 s_phone TEXT NOT NULL,
 s_acctbal NUMBER NOT NULL,
 s_comment TEXT
)

USING DELTA;