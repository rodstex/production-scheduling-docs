-- Job: ps_filters_per_pallet_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324210
-- Component ID: 4336044

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,sku_without_merv_rating
    ,filters_per_pallet
from ${stg2_schema}.ps_filters_per_pallet