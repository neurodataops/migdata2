-- Object     : tpcds_sf100tcl.web_sales
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : b18e8ab548226b677043d27097b8df50fde6cc78d0a777ad61c553694ed34548
-- Generated  : 2026-03-08T16:35:47.100017
-- ---

CREATE TABLE tpcds_sf100tcl.web_sales (
 ws_sold_date_sk NUMBER,
 ws_sold_time_sk NUMBER,
 ws_ship_date_sk NUMBER,
 ws_item_sk NUMBER NOT NULL,
 ws_bill_customer_sk NUMBER,
 ws_bill_cdemo_sk NUMBER,
 ws_bill_hdemo_sk NUMBER,
 ws_bill_addr_sk NUMBER,
 ws_ship_customer_sk NUMBER,
 ws_ship_cdemo_sk NUMBER,
 ws_ship_hdemo_sk NUMBER,
 ws_ship_addr_sk NUMBER,
 ws_web_page_sk NUMBER,
 ws_web_site_sk NUMBER,
 ws_ship_mode_sk NUMBER,
 ws_warehouse_sk NUMBER,
 ws_promo_sk NUMBER,
 ws_order_number NUMBER NOT NULL,
 ws_quantity NUMBER,
 ws_wholesale_cost NUMBER,
 ws_list_price NUMBER,
 ws_sales_price NUMBER,
 ws_ext_discount_amt NUMBER,
 ws_ext_sales_price NUMBER,
 ws_ext_wholesale_cost NUMBER,
 ws_ext_list_price NUMBER,
 ws_ext_tax NUMBER,
 ws_coupon_amt NUMBER,
 ws_ext_ship_cost NUMBER,
 ws_net_paid NUMBER,
 ws_net_paid_inc_tax NUMBER,
 ws_net_paid_inc_ship NUMBER,
 ws_net_paid_inc_ship_tax NUMBER,
 ws_net_profit NUMBER
)

USING DELTA;