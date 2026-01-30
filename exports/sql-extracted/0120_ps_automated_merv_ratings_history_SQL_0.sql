-- Job: ps_automated_merv_ratings_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324202
-- Component ID: 4335691

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,merv_rating
from ${stg2_schema}.ps_automated_merv_ratings