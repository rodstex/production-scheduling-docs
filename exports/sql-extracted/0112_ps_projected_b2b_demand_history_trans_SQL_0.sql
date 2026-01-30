-- Job: ps_projected_b2b_demand_history_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 7612783
-- Component ID: 7614715

select
    '${inserted_dt_utc}'::timestamptz as inserted_dt_utc
    ,estimated_order_week
    ,route_to_location
    ,sb_location_name
    ,sku
    ,estimated_order_quantity
    ,estimated_orders
from ${stg2_schema}.ps_projected_b2b_demand