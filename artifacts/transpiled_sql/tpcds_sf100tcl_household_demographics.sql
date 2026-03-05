-- Object     : tpcds_sf100tcl.household_demographics
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : d53d5d5866c1656ed99d8eae162b8d31b80cebfbc050344386f25a9f2363c6b8
-- Generated  : 2026-03-05T17:04:00.581072
-- ---

CREATE TABLE tpcds_sf100tcl.household_demographics (
 hd_demo_sk NUMBER NOT NULL,
 hd_income_band_sk NUMBER,
 hd_buy_potential TEXT,
 hd_dep_count NUMBER,
 hd_vehicle_count NUMBER
)

USING DELTA;