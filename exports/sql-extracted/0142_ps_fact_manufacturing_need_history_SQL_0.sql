-- Job: ps_fact_manufacturing_need_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324286
-- Component ID: 4324925

select
    '${inserted_dt_utc}'::timestamptz as inserted_dt_utc
    ,true as is_selected_dt
    ,location_name
    ,fmn.sku
    ,di.dim_item_id
    ,di.filter_type
    ,di.merv_rating
    ,di.sku_without_merv_rating
    ,quantity_in_stock
    ,quantity_in_transit
    ,quantity_on_order
    ,daily_sales_avg_rolling_28_day
    ,weekly_sales_avg_rolling_28_day
    ,days_of_inventory_remaining
    ,planned_manufacturing_1_day
    ,planned_manufacturing_2_day
    ,planned_manufacturing_3_day
    ,planned_manufacturing_4_day
    ,planned_manufacturing_5_day
    ,planned_manufacturing_6_day
    ,planned_manufacturing_7_day
    ,planned_manufacturing_8_day
    ,planned_manufacturing_9_day
    ,planned_manufacturing_10_day
    ,planned_manufacturing_11_day
    ,planned_manufacturing_12_day
    ,planned_manufacturing_13_day
    ,planned_manufacturing_14_day
    ,planned_manufacturing_15_day
    ,planned_manufacturing_16_day
    ,planned_manufacturing_17_day
    ,planned_manufacturing_18_day
    ,planned_manufacturing_19_day
    ,planned_manufacturing_20_day
    ,planned_manufacturing_21_day
    ,planned_manufacturing_22_day
    ,planned_manufacturing_23_day
    ,planned_manufacturing_24_day
    ,planned_manufacturing_25_day
    ,planned_manufacturing_26_day
    ,planned_manufacturing_27_day
    ,planned_manufacturing_28_day
    ,planned_manufacturing_29_day
    ,planned_manufacturing_30_day
    ,fmn.is_custom
from ${dwh_schema}.fact_manufacturing_need fmn
    join
        (select max(inserted_dt_utc) as max_dt
        from ${dwh_schema}.fact_manufacturing_need
        where location_name in (${locations_list})) t on t.max_dt = fmn.inserted_dt_utc
    join ${stg2_schema}.dw_sku_to_dim_item_id dsdi on dsdi.sku = fmn.sku
        and dsdi.rn = 1
    join ${dwh_schema}.dim_item di on di.dim_item_id = dsdi.dim_item_id
where location_name in (${locations_list})
	and not(fmn.is_custom)