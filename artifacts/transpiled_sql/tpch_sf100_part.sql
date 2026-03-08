-- Object     : tpch_sf100.part
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 98860603e3deb8cb9661b4f73d7036264b45dc18ee56f0d9849039863b02ed89
-- Generated  : 2026-03-08T16:35:47.521910
-- ---

CREATE TABLE tpch_sf100.part (
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