-- Object     : tpcds_sf10tcl.household_demographics
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : c956fca9efb29f4ab9d1af961e6deaab0a58058756bec485a17b76fdc3bc3d99
-- Generated  : 2026-03-05T17:04:00.633070
-- ---

CREATE TABLE tpcds_sf10tcl.household_demographics (
 hd_demo_sk NUMBER NOT NULL,
 hd_income_band_sk NUMBER,
 hd_buy_potential TEXT,
 hd_dep_count NUMBER,
 hd_vehicle_count NUMBER
)

USING DELTA;