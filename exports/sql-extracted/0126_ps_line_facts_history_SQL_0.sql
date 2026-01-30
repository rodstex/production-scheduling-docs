-- Job: ps_line_facts_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4326599
-- Component ID: 4335911

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,line_type
    ,min_run_hrs_per_size
    ,max_prod_lines_per_size
    ,min_run_hrs_per_merv_rating
    ,changeover_hrs_per_size
from ${stg2_schema}.ps_line_facts