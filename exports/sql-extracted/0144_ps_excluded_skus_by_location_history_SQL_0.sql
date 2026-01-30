-- Job: ps_excluded_skus_by_location_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 6315135
-- Component ID: 6315573

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,sku_without_merv_rating
    ,manufacturing_location
from ${stg2_schema}.ps_excluded_skus_by_location