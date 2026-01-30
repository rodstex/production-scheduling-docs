-- Job: fps_excess
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/excess_production
-- Component: Initialize stg2.ps_excess_distribution_need
-- Type: SQL Query
-- Job ID: 4347057
-- Component ID: 4347127

with base_available_quantity as (
    select
        fmn.inserted_dt_utc
        ,fmn.location_name
        ,fmn.sku
        ,fmn.dim_item_id
        ,fmn.sku_without_merv_rating
        ,fmn.filter_type
        ,fmn.merv_rating
        ,fmn.quantity_in_stock
            - coalesce(fmn.quantity_on_order, 0)
            + coalesce(fmn.quantity_in_transit, 0) as quantity_available
        ,fmn.days_of_inventory_remaining
    from ${stg2_schema}.ps_fact_manufacturing_need_history fmn
    where fmn.is_selected_dt /* Is current record */
        and fmn.location_name not in ('slc','pittsburgh','pope','newberry') /* Is not manufacturing location */
)

, base_future_sales as (
    select
        psh.inserted_dt_utc
        ,psh.sb_location_name
        ,psh.dim_item_id
        ,psh.sku
        ,psh.sku_without_merv_rating
        ,psh.filter_type
        ,psh.merv_rating
        ,max(psh.rolling_sum_quantity) as target_quantity
    from ${stg2_schema}.ps_sales_forecast_history psh
        join ${stg2_schema}.ps_target_days_of_inventory_history tdih on tdih.is_selected_dt /* Is current record */
            and tdih.grouped_line_type = 'Excess Distribution' /* Truncate to excess distribution goal */
            and tdih.distribution_sb_alias = psh.sb_location_name
            and tdih.filter_type = psh.filter_type
    where psh.is_selected_dt /* Is current record */
        and psh.sb_location_name not in ('slc','pittsburgh','pope','newberry') /* Is not manufacturing location */
        and psh.dt <= dateadd(day, tdih.target_days_of_inventory, psh.inserted_dt_utc::timestamp)::date
    group by 1,2,3,4,5,6,7
)

, base_dimensions as (
    select
        location_name
        ,sku
        ,dim_item_id
        ,sku_without_merv_rating
        ,filter_type
        ,merv_rating
    from base_available_quantity
    union
    select
        sb_location_name
        ,sku
        ,dim_item_id
        ,sku_without_merv_rating
        ,filter_type
        ,merv_rating
    from base_future_sales
)

select
    bd.location_name
    ,bd.sku
    ,bd.dim_item_id
    ,bd.sku_without_merv_rating
    ,bd.filter_type
    ,bd.merv_rating
    ,coalesce(baq.days_of_inventory_remaining, 0)   as days_of_inventory_remaining
    ,coalesce(baq.quantity_available, 0)            as quantity_available
    ,coalesce(bfs.target_quantity, 0)               as target_quantity
    ,quantity_available - target_quantity           as excess_distribution_need
from base_dimensions bd
    left join base_available_quantity baq on baq.location_name = bd.location_name
        and baq.dim_item_id = bd.dim_item_id
    left join base_future_sales bfs on bfs.sb_location_name = bd.location_name
        and bfs.dim_item_id = bd.dim_item_id
where excess_distribution_need < 0