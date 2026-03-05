-- Object     : tpch_sf10.lineitem
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 463e124cc972d9bcf4d696b3dae089f6565749e17a150227b7fc727810905645
-- Generated  : 2026-03-05T17:04:00.686352
-- ---

CREATE TABLE tpch_sf10.lineitem (
 l_orderkey NUMBER NOT NULL,
 l_partkey NUMBER NOT NULL,
 l_suppkey NUMBER NOT NULL,
 l_linenumber NUMBER NOT NULL,
 l_quantity NUMBER NOT NULL,
 l_extendedprice NUMBER NOT NULL,
 l_discount NUMBER NOT NULL,
 l_tax NUMBER NOT NULL,
 l_returnflag TEXT NOT NULL,
 l_linestatus TEXT NOT NULL,
 l_shipdate DATE NOT NULL,
 l_commitdate DATE NOT NULL,
 l_receiptdate DATE NOT NULL,
 l_shipinstruct TEXT NOT NULL,
 l_shipmode TEXT NOT NULL,
 l_comment TEXT NOT NULL
)

USING DELTA;