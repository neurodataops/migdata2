-- Object     : tpcds_sf100tcl.web_returns
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 32b3cccbd33500549916afa25e17dc3dce88df0546c01cb8cfebdcfdcc3154a8
-- Generated  : 2026-03-08T16:35:47.093018
-- ---

CREATE TABLE tpcds_sf100tcl.web_returns (
 wr_returned_date_sk NUMBER,
 wr_returned_time_sk NUMBER,
 wr_item_sk NUMBER NOT NULL,
 wr_refunded_customer_sk NUMBER,
 wr_refunded_cdemo_sk NUMBER,
 wr_refunded_hdemo_sk NUMBER,
 wr_refunded_addr_sk NUMBER,
 wr_returning_customer_sk NUMBER,
 wr_returning_cdemo_sk NUMBER,
 wr_returning_hdemo_sk NUMBER,
 wr_returning_addr_sk NUMBER,
 wr_web_page_sk NUMBER,
 wr_reason_sk NUMBER,
 wr_order_number NUMBER NOT NULL,
 wr_return_quantity NUMBER,
 wr_return_amt NUMBER,
 wr_return_tax NUMBER,
 wr_return_amt_inc_tax NUMBER,
 wr_fee NUMBER,
 wr_return_ship_cost NUMBER,
 wr_refunded_cash NUMBER,
 wr_reversed_charge NUMBER,
 wr_account_credit NUMBER,
 wr_net_loss NUMBER
)

USING DELTA;