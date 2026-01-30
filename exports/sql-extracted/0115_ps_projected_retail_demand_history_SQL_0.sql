-- Job: ps_projected_retail_demand_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 8856090
-- Component ID: 8856519

select
    '${inserted_dt_utc}'::timestamptz as inserted_dt_utc
    ,dt
    ,customer_name
    ,sku
    ,quantity
    ,business_customer_id
    ,mapped_distribution_location
from ${stg2_schema}.ps_projected_retail_demand
where 'pope' in (${locations_list}) -- Only insert if Talladega is selected