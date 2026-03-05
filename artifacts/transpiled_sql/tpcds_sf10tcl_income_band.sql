-- Object     : tpcds_sf10tcl.income_band
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 569b54fa81f23db80f36f8acc38dde5dd7cce84b9722f43687d3611999a07748
-- Generated  : 2026-03-05T17:04:00.635072
-- ---

CREATE TABLE tpcds_sf10tcl.income_band (
 ib_income_band_sk NUMBER NOT NULL,
 ib_lower_bound NUMBER,
 ib_upper_bound NUMBER
)

USING DELTA;