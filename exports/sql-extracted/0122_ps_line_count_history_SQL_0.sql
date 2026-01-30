-- Job: ps_line_count_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324204
-- Component ID: 4335812

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,line_type
    ,count_of_lines
from ${stg2_schema}.ps_line_count