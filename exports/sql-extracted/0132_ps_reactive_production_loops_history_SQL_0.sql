-- Job: ps_reactive_production_loops_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324212
-- Component ID: 4336286

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,iteration
    ,coalesce(lag(max_days_of_inventory) over (order by iteration asc), 0) as min_days_of_inventory
    ,max_days_of_inventory
    ,planned_manufacturing_days
    ,min_production_need
from ${stg2_schema}.ps_reactive_production_loops