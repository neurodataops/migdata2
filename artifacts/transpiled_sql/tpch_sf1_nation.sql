-- Object     : tpch_sf1.nation
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 220b538d93c13a72ffe7f90510a7aa3d08ba45e19a28e18cb160eaf81e25a114
-- Generated  : 2026-03-05T17:04:00.671353
-- ---

CREATE TABLE tpch_sf1.nation (
 n_nationkey NUMBER NOT NULL,
 n_name TEXT NOT NULL,
 n_regionkey NUMBER NOT NULL,
 n_comment TEXT
)

USING DELTA;