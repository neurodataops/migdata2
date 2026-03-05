-- Object     : tpch_sf1000.lineitem
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : a0722f630e7fa1bbb69ec2ccf52826f0a31023cf94b651b8ecc84938ae1a1236
-- Generated  : 2026-03-05T17:04:00.715352
-- ---

CREATE TABLE tpch_sf1000.lineitem (
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