-- Object     : tpcds_sf100tcl.catalog_returns
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : ec355f7c2c2f58ea913db909ca7f1ae93dba785a22869296e8c9cb327508765f
-- Generated  : 2026-03-05T17:04:00.567073
-- ---

CREATE TABLE tpcds_sf100tcl.catalog_returns (
 cr_returned_date_sk NUMBER,
 cr_returned_time_sk NUMBER,
 cr_item_sk NUMBER NOT NULL,
 cr_refunded_customer_sk NUMBER,
 cr_refunded_cdemo_sk NUMBER,
 cr_refunded_hdemo_sk NUMBER,
 cr_refunded_addr_sk NUMBER,
 cr_returning_customer_sk NUMBER,
 cr_returning_cdemo_sk NUMBER,
 cr_returning_hdemo_sk NUMBER,
 cr_returning_addr_sk NUMBER,
 cr_call_center_sk NUMBER,
 cr_catalog_page_sk NUMBER,
 cr_ship_mode_sk NUMBER,
 cr_warehouse_sk NUMBER,
 cr_reason_sk NUMBER,
 cr_order_number NUMBER NOT NULL,
 cr_return_quantity NUMBER,
 cr_return_amount NUMBER,
 cr_return_tax NUMBER,
 cr_return_amt_inc_tax NUMBER,
 cr_fee NUMBER,
 cr_return_ship_cost NUMBER,
 cr_refunded_cash NUMBER,
 cr_reversed_charge NUMBER,
 cr_store_credit NUMBER,
 cr_net_loss NUMBER
)

USING DELTA;