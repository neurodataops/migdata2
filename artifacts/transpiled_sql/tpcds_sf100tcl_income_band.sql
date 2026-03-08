-- Object     : tpcds_sf100tcl.income_band
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 753ac1693b3575ca1abe50f57d8c7cf71b787fa5ff9189d3531158535f52f6fa
-- Generated  : 2026-03-08T16:35:46.970593
-- ---

CREATE TABLE tpcds_sf100tcl.income_band (
 ib_income_band_sk NUMBER NOT NULL,
 ib_lower_bound NUMBER,
 ib_upper_bound NUMBER
)

USING DELTA;