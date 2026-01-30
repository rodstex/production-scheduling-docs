-- Job: ps_staff_by_day_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324206
-- Component ID: 4335835

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,grouped_line_type
    ,day_of_week_name
    ,day_of_week_int
    ,count_of_lines
from ${stg2_schema}.ps_staff_by_day