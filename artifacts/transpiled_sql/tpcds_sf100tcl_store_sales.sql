-- Object     : tpcds_sf100tcl.store_sales
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 874ddd7840ed08f6687ef84dd100453601ac985b2bc82f728349b2e68c22eaa3
-- Generated  : 2026-03-05T17:04:00.598070
-- ---

CREATE TABLE tpcds_sf100tcl.store_sales (
 ss_sold_date_sk NUMBER,
 ss_sold_time_sk NUMBER,
 ss_item_sk NUMBER NOT NULL,
 ss_customer_sk NUMBER,
 ss_cdemo_sk NUMBER,
 ss_hdemo_sk NUMBER,
 ss_addr_sk NUMBER,
 ss_store_sk NUMBER,
 ss_promo_sk NUMBER,
 ss_ticket_number NUMBER NOT NULL,
 ss_quantity NUMBER,
 ss_wholesale_cost NUMBER,
 ss_list_price NUMBER,
 ss_sales_price NUMBER,
 ss_ext_discount_amt NUMBER,
 ss_ext_sales_price NUMBER,
 ss_ext_wholesale_cost NUMBER,
 ss_ext_list_price NUMBER,
 ss_ext_tax NUMBER,
 ss_coupon_amt NUMBER,
 ss_net_paid NUMBER,
 ss_net_paid_inc_tax NUMBER,
 ss_net_profit NUMBER
)

USING DELTA;