-- Object     : tpcds_sf100tcl.customer_demographics
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : cbbb3143eec29c7e66555b6faf5acae3f8977cd144ee2bb57aa67c8a5bb77e2a
-- Generated  : 2026-03-05T17:04:00.576068
-- ---

CREATE TABLE tpcds_sf100tcl.customer_demographics (
 cd_demo_sk NUMBER NOT NULL,
 cd_gender TEXT,
 cd_marital_status TEXT,
 cd_education_status TEXT,
 cd_purchase_estimate NUMBER,
 cd_credit_rating TEXT,
 cd_dep_count NUMBER,
 cd_dep_employed_count NUMBER,
 cd_dep_college_count NUMBER
)

USING DELTA;