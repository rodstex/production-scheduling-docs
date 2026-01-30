-- Job: ps_non_automated_reactive_ranking_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 6398032
-- Component ID: 6398035

/* Return production that has already been scheduled for the day in question.
 * Filter to within capacity only.
 */
with base_production as (
    select
        mdch.distribution_sb_alias
        ,fps.sku_without_merv_rating
        ,sum(fps.production_goal_merv_8)                    as production_goal_merv_8
        ,sum(fps.production_goal_merv_11)                   as production_goal_merv_11
        ,sum(fps.production_goal_merv_13)                   as production_goal_merv_13
        ,sum(fps.production_goal_merv_8_odor_eliminator)    as production_goal_merv_8_odor_eliminator
    from ${dwh_schema}.fact_production_schedule fps
        join
            (select distinct inserted_dt_utc
             from ${stg2_schema}.ps_fact_manufacturing_need_future
             where is_selected_dt) fmn on fmn.inserted_dt_utc = fps.inserted_dt_utc -- Truncate to the current day's production schedule
        join ${stg2_schema}.ps_manufacturing_to_distribution_center_history mdch on mdch.is_selected_dt
            and mdch.mapped_distribution_location = fps.distribution_location
    where fps.is_within_capacity
        and fps.line_type = 'Automated'
    group by 1,2
)

/* Return the sales forecast for the day in question and future dates.
 * Truncate to products that have production scheduled.
 */
, sales_forecast as (
    select
        fmn.inserted_dt_utc
        ,psf.dt
        ,fmn.location_name
        ,fmn.sku
        ,fmn.quantity_in_stock
        ,case when fmn.merv_rating = 'MERV 8'               then bp.production_goal_merv_8
            when fmn.merv_rating = 'MERV 11'                then bp.production_goal_merv_11
            when fmn.merv_rating = 'MERV 13'                then bp.production_goal_merv_13
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then bp.production_goal_merv_8_odor_eliminator end as production_goal
        ,fmn.quantity_in_stock + production_goal as quantity_in_stock_after_scheduled_production
        ,psf.quantity
        ,sum(psf.quantity) over (partition by psf.sb_location_name, psf.sku order by psf.dt asc rows unbounded preceding) as rolling_sum_quantity
    from ${stg2_schema}.ps_fact_manufacturing_need_future fmn
        join base_production bp on bp.distribution_sb_alias = fmn.location_name
            and bp.sku_without_merv_rating = fmn.sku_without_merv_rating
        join ${stg2_schema}.ps_sales_forecast_history psf on psf.is_selected_dt
            and psf.sb_location_name = fmn.location_name
            and psf.sku = fmn.sku
            and psf.dt >= fmn.inserted_dt_utc::date
    where fmn.is_selected_dt
        and production_goal > 0
)

/* Estimate days of inventory remaining using sales forecast and current inventory levels.
 * Assume that on order and in transit quantities impact inventory immediately.
 */
, days_of_inventory_remaining as (
    /* dw_sales_forecast includes a sales forecast for the next N days.
     * If the rolling sum of sales is ever greater than or equal to the new quantity in stock within that period, return the minimum date that occurs. */
    select
        location_name
        ,sku
        ,quantity_in_stock_after_scheduled_production
        ,min(dt) as next_out_of_stock_dt
        ,date_diff('day', inserted_dt_utc::date, next_out_of_stock_dt) as day_of_inventory_remaining_after_scheduled_production
    from sales_forecast
    where rolling_sum_quantity >= quantity_in_stock_after_scheduled_production /* Where sum of {sales forecast} is >= {quantity in stock} */
    group by 1,2,3, inserted_dt_utc
    union all
    /* If the rolling sum of sales is always less than the new quantity in stock, assume that the SKU's next out of stock date is the maximum date in dw_sales_forecast */
    select
        location_name
        ,sku
        ,quantity_in_stock_after_scheduled_production
        ,max(dt) as next_out_of_stock_dt
        ,date_diff('day', inserted_dt_utc::date, next_out_of_stock_dt) as day_of_inventory_remaining_after_scheduled_production
    from sales_forecast
    group by 1,2,3, inserted_dt_utc
    having quantity_in_stock_after_scheduled_production > max(rolling_sum_quantity)
)

/* Base records for non-automated manufacturing.
 * Filters:
 *      (1) Distribution location(s) of interest
 *      (2) Manufacturing location that feeds DC(s)
 *      (3) Manufacturing location with non-automated line(s)
 *      (4) SKU(s) that can be made at manufacturing location on non-automated lines
 *      (5) Non-automated line type is the efficiency rank of interest
 */
, base_records as (
    select
        fmn.inserted_dt_utc
        ,fmn.runtime_dt_utc
        ,mdch.mapped_distribution_location
        ----------------------------------------------
        -- manufacturing location & line type facts --
        ----------------------------------------------
        ,mdch.mapped_manufacturing_location
        ---------------
        -- sku facts --
        ---------------
        ,fmn.sku
        ,'${planned_manufacturing_days}'::int as target_days_of_inventory
        ,fmn.filter_type
        ,fmn.merv_rating
        ,fmn.sku_without_merv_rating
        ,fmn.days_of_inventory_remaining
        ,case when coalesce(doir.day_of_inventory_remaining_after_scheduled_production, fmn.days_of_inventory_remaining) <= 0 then 0
            else coalesce(doir.day_of_inventory_remaining_after_scheduled_production, fmn.days_of_inventory_remaining) end as days_of_inventory_remaining_after_scheduled_production
        ,case when mdch.mapped_manufacturing_location = mdch.mapped_distribution_location and days_of_inventory_remaining_after_scheduled_production = 0 then 1
            else 0 end as out_of_stock_at_manufacturing_location_flg
        ,case when mdch.mapped_manufacturing_location != mdch.mapped_distribution_location and days_of_inventory_remaining_after_scheduled_production = 0 then 1
            else 0 end as out_of_stock_at_distribution_center_flg
        ,fmn.planned_manufacturing_1_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_1_day
        ,fmn.planned_manufacturing_2_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_2_day
        ,fmn.planned_manufacturing_3_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_3_day
        ,fmn.planned_manufacturing_4_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_4_day
        ,fmn.planned_manufacturing_5_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_5_day
        ,fmn.planned_manufacturing_6_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_6_day
        ,fmn.planned_manufacturing_7_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_7_day
        ,fmn.planned_manufacturing_8_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_8_day
        ,fmn.planned_manufacturing_9_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_9_day
        ,fmn.planned_manufacturing_10_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_10_day
        ,fmn.planned_manufacturing_11_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_11_day
        ,fmn.planned_manufacturing_12_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_12_day
        ,fmn.planned_manufacturing_13_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_13_day
        ,fmn.planned_manufacturing_14_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_14_day
        ,fmn.planned_manufacturing_15_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_15_day
        ,fmn.planned_manufacturing_16_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_16_day
        ,fmn.planned_manufacturing_17_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_17_day
        ,fmn.planned_manufacturing_18_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_18_day
        ,fmn.planned_manufacturing_19_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_19_day
        ,fmn.planned_manufacturing_20_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_20_day
        ,fmn.planned_manufacturing_21_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_21_day
        ,fmn.planned_manufacturing_22_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_22_day
        ,fmn.planned_manufacturing_23_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_23_day
        ,fmn.planned_manufacturing_24_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_24_day
        ,fmn.planned_manufacturing_25_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_25_day
        ,fmn.planned_manufacturing_26_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_26_day
        ,fmn.planned_manufacturing_27_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_27_day
        ,fmn.planned_manufacturing_28_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_28_day
        ,fmn.planned_manufacturing_29_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_29_day
        ,fmn.planned_manufacturing_30_day - case
            when fmn.merv_rating = 'MERV 8' then coalesce(bp.production_goal_merv_8, 0)
            when fmn.merv_rating = 'MERV 11' then coalesce(bp.production_goal_merv_11, 0)
            when fmn.merv_rating = 'MERV 13' then coalesce(bp.production_goal_merv_13, 0)
            when fmn.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(bp.production_goal_merv_8_odor_eliminator, 0) end as planned_manufacturing_30_day
        ,fpph.filters_per_pallet
    from ${stg2_schema}.ps_fact_manufacturing_need_future fmn
        -- Identify manufacturing location(s) that send inventory to DC --
        join ${stg2_schema}.ps_manufacturing_to_distribution_center_history mdch on mdch.is_selected_dt
            and mdch.distribution_sb_alias = fmn.location_name
        -- Truncate to SKU(s) that can be made on non-automated lines at the manufacturing location --
        join
            (select distinct
                mapped_manufacturing_location
                ,sku_without_merv_rating
            from ${stg2_schema}.ps_capacity_by_sku_history
            where is_selected_dt
                and line_type in ('Double Loader', 'Single Loader', 'Manual')) cap on cap.mapped_manufacturing_location = mdch.mapped_manufacturing_location
                and cap.sku_without_merv_rating = fmn.sku_without_merv_rating
        -- Return facts about SKU --
        join ${stg2_schema}.ps_filters_per_pallet_history fpph on fpph.is_selected_dt
            and fpph.sku_without_merv_rating = fmn.sku_without_merv_rating
        -- Return production that has been scheduled (if any) --
        left join base_production bp on bp.distribution_sb_alias = mdch.distribution_sb_alias
            and bp.sku_without_merv_rating = fmn.sku_without_merv_rating
        -- Return days of inventory remaining after production that has been scheduled (if any) --
        left join days_of_inventory_remaining doir on doir.location_name = fmn.location_name
            and doir.sku = fmn.sku
        join ${stg2_schema}.ps_settings_history psh on psh.is_selected_dt
        -- Return sizes that can be made on automated lines at the location
        left join ${stg2_schema}.ps_capacity_by_sku_history cap_auto on cap_auto.is_selected_dt
            and case when cap_auto.mapped_manufacturing_location = mdch.mapped_manufacturing_location then true
                when cap_auto.mapped_manufacturing_location = 'Talladega, AL (TMS)' and mdch.mapped_manufacturing_location like 'Talladega%' then true
                else false end
            and cap_auto.line_type = 'Automated'
            and cap_auto.sku_without_merv_rating = fmn.sku_without_merv_rating
        -- Filter out excluded SKUs (DATA-505) --
        left join ${stg2_schema}.ps_excluded_skus_by_location_history excl on excl.is_selected_dt
            and excl.sku_without_merv_rating = fmn.sku_without_merv_rating
            and excl.manufacturing_location = mdch.mapped_manufacturing_location
    where fmn.is_selected_dt
        and days_of_inventory_remaining_after_scheduled_production >= '${min_days_of_inventory}'::int
        and days_of_inventory_remaining_after_scheduled_production < '${max_days_of_inventory}'::int
        and case when psh.is_schedule_auto_sizes_to_non_auto_lines then true
            when not(psh.is_schedule_auto_sizes_to_non_auto_lines) and cap_auto.sku_without_merv_rating is not null then false
            else true end
        and excl.sku_without_merv_rating is null /* SKU and manufacturing location is not an invalid combination DATA-505 */
        and not(fmn.is_custom) /* Is not custom SKU DATA-520 */
)

/* Transform base_records into 1 record per (1) Manufacturing location and (2) SKU.
 */
, production_need_by_day_sku as (
    select
        br.mapped_manufacturing_location
        ,br.sku
        ,br.sku_without_merv_rating
        ,br.merv_rating
        ,min(days_of_inventory_remaining_after_scheduled_production) as min_day_of_inventory_remaining
        ---------------------------
        -- production need facts --
        ---------------------------
        ,max(out_of_stock_at_manufacturing_location_flg) as out_of_stock_at_manufacturing_location_flg
        ,max(out_of_stock_at_distribution_center_flg) as out_of_stock_at_distribution_center_flg
        ,sum(case when br.target_days_of_inventory = 1 then br.planned_manufacturing_1_day
            when br.target_days_of_inventory = 2 then br.planned_manufacturing_2_day
            when br.target_days_of_inventory = 3 then br.planned_manufacturing_3_day
            when br.target_days_of_inventory = 4 then br.planned_manufacturing_4_day
            when br.target_days_of_inventory = 5 then br.planned_manufacturing_5_day
            when br.target_days_of_inventory = 6 then br.planned_manufacturing_6_day
            when br.target_days_of_inventory = 7 then br.planned_manufacturing_7_day
            when br.target_days_of_inventory = 8 then br.planned_manufacturing_8_day
            when br.target_days_of_inventory = 9 then br.planned_manufacturing_9_day
            when br.target_days_of_inventory = 10 then br.planned_manufacturing_10_day
            when br.target_days_of_inventory = 11 then br.planned_manufacturing_11_day
            when br.target_days_of_inventory = 12 then br.planned_manufacturing_12_day
            when br.target_days_of_inventory = 13 then br.planned_manufacturing_13_day
            when br.target_days_of_inventory = 14 then br.planned_manufacturing_14_day
            when br.target_days_of_inventory = 15 then br.planned_manufacturing_15_day
            when br.target_days_of_inventory = 16 then br.planned_manufacturing_16_day
            when br.target_days_of_inventory = 17 then br.planned_manufacturing_17_day
            when br.target_days_of_inventory = 18 then br.planned_manufacturing_18_day
            when br.target_days_of_inventory = 19 then br.planned_manufacturing_19_day
            when br.target_days_of_inventory = 20 then br.planned_manufacturing_20_day
            when br.target_days_of_inventory = 20 then br.planned_manufacturing_20_day
            when br.target_days_of_inventory = 21 then br.planned_manufacturing_21_day
            when br.target_days_of_inventory = 22 then br.planned_manufacturing_22_day
            when br.target_days_of_inventory = 23 then br.planned_manufacturing_23_day
            when br.target_days_of_inventory = 24 then br.planned_manufacturing_24_day
            when br.target_days_of_inventory = 25 then br.planned_manufacturing_25_day
            when br.target_days_of_inventory = 26 then br.planned_manufacturing_26_day
            when br.target_days_of_inventory = 27 then br.planned_manufacturing_27_day
            when br.target_days_of_inventory = 28 then br.planned_manufacturing_28_day
            when br.target_days_of_inventory = 29 then br.planned_manufacturing_29_day
            when br.target_days_of_inventory = 30 then br.planned_manufacturing_30_day
            end) as production_need
    from base_records br
    group by 1,2,3,4
    having production_need >= '${min_production_need}'
)

/* For each manufacturing location, rank SKU without MERV rating(s) by:
 *      (1) Out of stock at distribution center
 *      (2) Out of stock at manufacturing location
 *      (3) Days of inventory remaining (ascending)
 *      (4) Production need (descending)
 * #1-2 are evaluated as the minimum for any location and SKU without MERV rating.
 *      E.g., if a manufacturing location feeds 3 DCs and 1 of them is out of stock, #1 will be true for all records.
 * #3 is evaluated as the minimum for any location and SKU without MERV rating.
 *      E.g., if the MERV 8 version of a product has 0 days of inventory remaining and the MERV 11 version has 2 days of inventory remaining, #3 will be 0 for all products.
 * #4 is evaluated as the total for any location and SKU without MERV rating.
 */
, base_rankings as (
    select
        mapped_manufacturing_location
        ,sku_without_merv_rating
        ,min(min_day_of_inventory_remaining)                as min_day_of_inventory_remaining
        ,max(out_of_stock_at_manufacturing_location_flg)    as out_of_stock_at_manufacturing_location_flg
        ,min(out_of_stock_at_distribution_center_flg)       as out_of_stock_at_distribution_center_flg
        ,sum(production_need)                               as total_production_need
    from production_need_by_day_sku
    group by 1,2
)

, rankings as (
    select
        br.mapped_manufacturing_location
        ,br.sku_without_merv_rating
        ,br.min_day_of_inventory_remaining
        ,br.out_of_stock_at_distribution_center_flg
        ,br.out_of_stock_at_manufacturing_location_flg
        ,br.total_production_need
        ,row_number() over (partition by br.mapped_manufacturing_location order by
            br.out_of_stock_at_distribution_center_flg desc
            ,br.out_of_stock_at_manufacturing_location_flg desc
            ,br.min_day_of_inventory_remaining asc
            ,br.total_production_need desc) as ranking
    from base_rankings br
)

select
    br.inserted_dt_utc
    ,br.runtime_dt_utc
    ,br.mapped_distribution_location
    ----------------------------------------------
    -- manufacturing location & line type facts --
    ----------------------------------------------
    ,br.mapped_manufacturing_location
    ,case when pn.sku is null then true else false end as is_unscheduled_production
    ---------------
    -- sku facts --
    ---------------
    ,br.sku
    ,br.filter_type
    ,br.merv_rating
    ,br.sku_without_merv_rating
    ,br.days_of_inventory_remaining
    ,case when pn.sku is null then null
        else r.ranking + coalesce(mr.max_rank, 0) end as ranking
    ,br.filters_per_pallet
    ,(case when br.target_days_of_inventory = 1 then br.planned_manufacturing_1_day
        when br.target_days_of_inventory = 2 then br.planned_manufacturing_2_day
        when br.target_days_of_inventory = 3 then br.planned_manufacturing_3_day
        when br.target_days_of_inventory = 4 then br.planned_manufacturing_4_day
        when br.target_days_of_inventory = 5 then br.planned_manufacturing_5_day
        when br.target_days_of_inventory = 6 then br.planned_manufacturing_6_day
        when br.target_days_of_inventory = 7 then br.planned_manufacturing_7_day
        when br.target_days_of_inventory = 8 then br.planned_manufacturing_8_day
        when br.target_days_of_inventory = 9 then br.planned_manufacturing_9_day
        when br.target_days_of_inventory = 10 then br.planned_manufacturing_10_day
        when br.target_days_of_inventory = 11 then br.planned_manufacturing_11_day
        when br.target_days_of_inventory = 12 then br.planned_manufacturing_12_day
        when br.target_days_of_inventory = 13 then br.planned_manufacturing_13_day
        when br.target_days_of_inventory = 14 then br.planned_manufacturing_14_day
        when br.target_days_of_inventory = 15 then br.planned_manufacturing_15_day
        when br.target_days_of_inventory = 16 then br.planned_manufacturing_16_day
        when br.target_days_of_inventory = 17 then br.planned_manufacturing_17_day
        when br.target_days_of_inventory = 18 then br.planned_manufacturing_18_day
        when br.target_days_of_inventory = 19 then br.planned_manufacturing_19_day
        when br.target_days_of_inventory = 20 then br.planned_manufacturing_20_day
        when br.target_days_of_inventory = 21 then br.planned_manufacturing_21_day
        when br.target_days_of_inventory = 22 then br.planned_manufacturing_22_day
        when br.target_days_of_inventory = 23 then br.planned_manufacturing_23_day
        when br.target_days_of_inventory = 24 then br.planned_manufacturing_24_day
        when br.target_days_of_inventory = 25 then br.planned_manufacturing_25_day
        when br.target_days_of_inventory = 26 then br.planned_manufacturing_26_day
        when br.target_days_of_inventory = 27 then br.planned_manufacturing_27_day
        when br.target_days_of_inventory = 28 then br.planned_manufacturing_28_day
        when br.target_days_of_inventory = 29 then br.planned_manufacturing_29_day
        when br.target_days_of_inventory = 30 then br.planned_manufacturing_30_day
        end) as production_need_calculated
from base_records br
    left join rankings r on r.sku_without_merv_rating = br.sku_without_merv_rating
        and r.mapped_manufacturing_location = br.mapped_manufacturing_location
    left join production_need_by_day_sku pn on pn.sku = br.sku
        and pn.mapped_manufacturing_location = br.mapped_manufacturing_location
    left join ${stg2_schema}.ps_non_automated_max_ranking mr on mr.mapped_manufacturing_location = br.mapped_manufacturing_location
where production_need_calculated > 0