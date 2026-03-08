-- Object     : tpch_sf1.lineitem
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 49109aca22d6a61b0ec7cdc7f16c941bbbaf8a83e6f8ef9d0134592e4cab3a54
-- Generated  : 2026-03-08T16:35:47.405168
-- ---

CREATE TABLE tpch_sf1.lineitem (
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