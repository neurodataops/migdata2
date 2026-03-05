-- Object     : tpch_sf1000.part
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : edd4d8962e81acd95c9636e9c9a339e6b8be06d29bededa5ccf15b085b8a2862
-- Generated  : 2026-03-05T17:04:00.721352
-- ---

CREATE TABLE tpch_sf1000.part (
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