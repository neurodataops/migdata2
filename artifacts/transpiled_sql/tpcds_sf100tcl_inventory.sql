-- Object     : tpcds_sf100tcl.inventory
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 7a1721cf5b3010db8115d4e628420110761318f55497dec815193af17d69e9e0
-- Generated  : 2026-03-05T17:04:00.584072
-- ---

CREATE TABLE tpcds_sf100tcl.inventory (
 inv_date_sk NUMBER NOT NULL,
 inv_item_sk NUMBER NOT NULL,
 inv_warehouse_sk NUMBER NOT NULL,
 inv_quantity_on_hand NUMBER
)

USING DELTA;