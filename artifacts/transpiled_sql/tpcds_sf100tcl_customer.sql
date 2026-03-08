-- Object     : tpcds_sf100tcl.customer
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : d8fac45621b746a4ae79bbfece7f70e86b019081de064408daee3411535470fb
-- Generated  : 2026-03-08T16:35:46.938494
-- ---

CREATE TABLE tpcds_sf100tcl.customer (
 c_customer_sk NUMBER NOT NULL,
 c_customer_id TEXT NOT NULL,
 c_current_cdemo_sk NUMBER,
 c_current_hdemo_sk NUMBER,
 c_current_addr_sk NUMBER,
 c_first_shipto_date_sk NUMBER,
 c_first_sales_date_sk NUMBER,
 c_salutation TEXT,
 c_first_name TEXT,
 c_last_name TEXT,
 c_preferred_cust_flag TEXT,
 c_birth_day NUMBER,
 c_birth_month NUMBER,
 c_birth_year NUMBER,
 c_birth_country TEXT,
 c_login TEXT,
 c_email_address TEXT,
 c_last_review_date TEXT
)

USING DELTA;