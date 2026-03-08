-- Object     : tpch_sf1000.nation
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : b46dbbd4e41fc78c9e2eb52692d34d5218557f60d9329263bc9ac52d8980fccc
-- Generated  : 2026-03-08T16:35:47.556238
-- ---

CREATE TABLE tpch_sf1000.nation (
 n_nationkey NUMBER NOT NULL,
 n_name TEXT NOT NULL,
 n_regionkey NUMBER NOT NULL,
 n_comment TEXT
)

USING DELTA;