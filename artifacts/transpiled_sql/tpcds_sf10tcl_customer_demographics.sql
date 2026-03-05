-- Object     : tpcds_sf10tcl.customer_demographics
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : d6abc9709a936252e27b3866de25bfcb0f9a7c827aa1fc5f30636a75da8b26d4
-- Generated  : 2026-03-05T17:04:00.629073
-- ---

CREATE TABLE tpcds_sf10tcl.customer_demographics (
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