-- Job: ps_target_days_of_inventory_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324200
-- Component ID: 4335617

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,mapped_distribution_location
    ,distribution_sb_alias
    ,grouped_line_type
    ,filter_type
    ,target_days_of_inventory
from ${stg2_schema}.ps_target_days_of_inventory
where distribution_sb_alias in (${locations_list})