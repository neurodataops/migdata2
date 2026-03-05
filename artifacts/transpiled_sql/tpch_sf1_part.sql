-- Object     : tpch_sf1.part
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 9538236b9d24ba46aa8b5b92c662d25cf53ae34bd520a3706b4a437bc7e7a764
-- Generated  : 2026-03-05T17:04:00.674349
-- ---

CREATE TABLE tpch_sf1.part (
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