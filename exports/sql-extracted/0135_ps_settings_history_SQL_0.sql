-- Job: ps_settings_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324214
-- Component ID: 4336345

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,excess_inventory_perc_of_target_inventory
    ,is_deduct_excess_distribution_from_prod_need
    ,day_of_week_int_line_capacity_set
    ,day_of_week_int_auto_schedule_set
    ,is_schedule_auto_sizes_to_non_auto_lines
    ,non_automated_proactive_minimum_production_run
from ${stg2_schema}.ps_settings