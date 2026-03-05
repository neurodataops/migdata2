-- Object     : tpcds_sf10tcl.store_returns
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : e685457dbf646305ec6c51b5b0aa2f46c634070f4b516cf742df8466a4fff020
-- Generated  : 2026-03-05T17:04:00.649353
-- ---

CREATE TABLE tpcds_sf10tcl.store_returns (
 sr_returned_date_sk NUMBER,
 sr_return_time_sk NUMBER,
 sr_item_sk NUMBER NOT NULL,
 sr_customer_sk NUMBER,
 sr_cdemo_sk NUMBER,
 sr_hdemo_sk NUMBER,
 sr_addr_sk NUMBER,
 sr_store_sk NUMBER,
 sr_reason_sk NUMBER,
 sr_ticket_number NUMBER NOT NULL,
 sr_return_quantity NUMBER,
 sr_return_amt NUMBER,
 sr_return_tax NUMBER,
 sr_return_amt_inc_tax NUMBER,
 sr_fee NUMBER,
 sr_return_ship_cost NUMBER,
 sr_refunded_cash NUMBER,
 sr_reversed_charge NUMBER,
 sr_store_credit NUMBER,
 sr_net_loss NUMBER
)

USING DELTA;