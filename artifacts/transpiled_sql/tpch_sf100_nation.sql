-- Object     : tpch_sf100.nation
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : cc4a4229ba44911f007db6ff0423dc05b5435bd83dcbeae92461bc93f1b459e3
-- Generated  : 2026-03-05T17:04:00.702346
-- ---

CREATE TABLE tpch_sf100.nation (
 n_nationkey NUMBER NOT NULL,
 n_name TEXT NOT NULL,
 n_regionkey NUMBER NOT NULL,
 n_comment TEXT
)

USING DELTA;