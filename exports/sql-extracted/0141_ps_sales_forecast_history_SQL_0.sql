-- Job: ps_sales_forecast_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324187
-- Component ID: 4336794

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,mapped_location_name
    ,sb_location_name
    ,psf.sku
    ,di.dim_item_id
    ,di.sku_without_merv_rating
    ,di.merv_rating
    ,di.filter_type
    ,dt
    ,quantity
    ,rolling_sum_quantity
    ,daily_sales_avg_rolling_28_day
    ,weekly_sales_avg_rolling_28_day
from ${stg2_schema}.ps_sales_forecast psf
    join ${stg2_schema}.dw_sku_to_dim_item_id dsdi on dsdi.sku = psf.sku
        and dsdi.rn = 1
    join ${dwh_schema}.dim_item di on di.dim_item_id = dsdi.dim_item_id