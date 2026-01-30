-- Job: ps_fact_manufacturing_need_future_trans0
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/future_production_need_calc
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4356474
-- Component ID: 4356534

select
    fmn.inserted_dt_utc as runtime_dt_utc
    ,fmn.inserted_dt_utc
    ,fmn.is_selected_dt
    ,fmn.location_name
    ,fmn.sku
    ,fmn.filter_type
    ,fmn.merv_rating
    ,fmn.sku_without_merv_rating
    ,fmn.quantity_in_stock
    ,fmn.quantity_in_transit
    ,fmn.quantity_on_order
    ,coalesce(psf.quantity, 0) as sales_on_dt
    ,fmn.daily_sales_avg_rolling_28_day
    ,fmn.weekly_sales_avg_rolling_28_day
    ,fmn.days_of_inventory_remaining
    ,fmn.planned_manufacturing_1_day
    ,fmn.planned_manufacturing_2_day
    ,fmn.planned_manufacturing_3_day
    ,fmn.planned_manufacturing_4_day
    ,fmn.planned_manufacturing_5_day
    ,fmn.planned_manufacturing_6_day
    ,fmn.planned_manufacturing_7_day
    ,fmn.planned_manufacturing_8_day
    ,fmn.planned_manufacturing_9_day
    ,fmn.planned_manufacturing_10_day
    ,fmn.planned_manufacturing_11_day
    ,fmn.planned_manufacturing_12_day
    ,fmn.planned_manufacturing_13_day
    ,fmn.planned_manufacturing_14_day
    ,fmn.planned_manufacturing_15_day
    ,fmn.planned_manufacturing_16_day
    ,fmn.planned_manufacturing_17_day
    ,fmn.planned_manufacturing_18_day
    ,fmn.planned_manufacturing_19_day
    ,fmn.planned_manufacturing_20_day
    ,fmn.planned_manufacturing_21_day
    ,fmn.planned_manufacturing_22_day
    ,fmn.planned_manufacturing_23_day
    ,fmn.planned_manufacturing_24_day
    ,fmn.planned_manufacturing_25_day
    ,fmn.planned_manufacturing_26_day
    ,fmn.planned_manufacturing_27_day
    ,fmn.planned_manufacturing_28_day
    ,fmn.planned_manufacturing_29_day
    ,fmn.planned_manufacturing_30_day
    ,fmn.is_custom
from ${stg2_schema}.ps_fact_manufacturing_need_history fmn
    left join ${stg2_schema}.ps_sales_forecast_history psf on psf.is_selected_dt
        and psf.sb_location_name = fmn.location_name
        and psf.sku = fmn.sku
        and psf.dt = fmn.inserted_dt_utc::date
where fmn.is_selected_dt