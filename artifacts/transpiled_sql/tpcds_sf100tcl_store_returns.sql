-- Object     : tpcds_sf100tcl.store_returns
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 8aebfdc8358a345b7b2cfa59388245f01043b2b9cc60587a28bf9d301f9e6e12
-- Generated  : 2026-03-05T17:04:00.595071
-- ---

CREATE TABLE tpcds_sf100tcl.store_returns (
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