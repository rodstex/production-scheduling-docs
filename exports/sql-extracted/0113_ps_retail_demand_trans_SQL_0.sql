-- Job: ps_retail_demand_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 8791512
-- Component ID: 8791595

select
    business_customer_order_id
    ,create_dt_utc
    ,update_dt_utc
    ,order_status
    ,business_customer_id
    ,customer_name
    ,business_customer_order_item_id
    ,sku
    ,quantity
from ${stg1_schema}.ps_retail_demand