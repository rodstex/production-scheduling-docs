-- Job: ps_fact_manufacturing_need_future_trans1
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/future_production_need_calc
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4355520
-- Component ID: 4356745

/* Base records for excess inventory distribution from MFG location.
 * Returns by
 *      (1) manufacturing location the inventory is being removed from and
 *      (2) SKU without MERV rating
 * Filters:
 * (1) Scheduled for current record
 * (2) Is set to be removed from current inventory (i.e., within capacity)
 */
with base_excess_removal as (
    select
        mtd.manufacturing_sb_alias
        ,fps.sku_without_merv_rating
        ,sum(fps.production_goal_merv_8)                   as production_goal_merv_8
        ,sum(fps.production_goal_merv_11)                  as production_goal_merv_11
        ,sum(fps.production_goal_merv_13)                  as production_goal_merv_13
        ,sum(fps.production_goal_merv_8_odor_eliminator)   as production_goal_merv_8_odor_eliminator
    from ${dwh_schema}.fact_production_schedule fps
        join (select distinct inserted_dt_utc::date as dt /* Truncate to current record */
              from ${stg2_schema}.ps_fact_manufacturing_need_future_copy
              where is_selected_dt) fmn on fmn.dt = fps.inserted_dt_utc::date
        join (select distinct
                  mapped_manufacturing_location
                  ,manufacturing_sb_alias
              from ${stg2_schema}.ps_manufacturing_to_distribution_center_history
              where is_selected_dt) mtd on mtd.mapped_manufacturing_location = fps.manufacturing_location
    where fps.line_type = 'Excess Distribution'
        and fps.is_within_capacity
    group by 1,2
)

/* Transform excess inventory removal from a SKU without MERV rating level
 * to a SKU level. */
, excess_inventory_removal as (
    ------------
    -- MERV 8 --
    ------------
    select
        manufacturing_sb_alias
        ,sku_without_merv_rating || 'M8'    as sku
        ,production_goal_merv_8             as excess_inventory_removal
    from base_excess_removal
    where production_goal_merv_8 <> 0
    union all
    -------------
    -- MERV 11 --
    -------------
    select
        manufacturing_sb_alias
        ,sku_without_merv_rating || 'M11'    as sku
        ,production_goal_merv_11             as excess_inventory_removal
    from base_excess_removal
    where production_goal_merv_11 <> 0
    union all
    -------------
    -- MERV 13 --
    -------------
    select
        manufacturing_sb_alias
        ,sku_without_merv_rating || 'M13'    as sku
        ,production_goal_merv_13             as excess_inventory_removal
    from base_excess_removal
    where production_goal_merv_13 <> 0
    union all
    ----------------------------
    -- MERV 8 Odor Eliminator --
    ----------------------------
    select
        manufacturing_sb_alias
        ,sku_without_merv_rating || 'OE'        as sku
        ,production_goal_merv_8_odor_eliminator as excess_inventory_removal
    from base_excess_removal
    where production_goal_merv_8_odor_eliminator <> 0
)

/* Base records for production scheduled.
 * Includes excess distribution (if included in production scheduling #s) and production scheduled.
 * Returns by
 *      (1) distribution location the inventory is being added tp and
 *      (2) SKU without MERV rating
 * Filters:
 * (1) Scheduled for current record
 * (2) Is set to be added from current inventory (i.e., within capacity)
 */
, base_production_scheduled as (
    select
        mtd.distribution_sb_alias
        ,fps.sku_without_merv_rating
        ,sum(fps.production_goal_merv_8)                   as production_goal_merv_8
        ,sum(fps.production_goal_merv_11)                  as production_goal_merv_11
        ,sum(fps.production_goal_merv_13)                  as production_goal_merv_13
        ,sum(fps.production_goal_merv_8_odor_eliminator)   as production_goal_merv_8_odor_eliminator
    from ${dwh_schema}.fact_production_schedule fps
        join (select distinct inserted_dt_utc::date as dt /* Truncate to current record */
              from ${stg2_schema}.ps_fact_manufacturing_need_future_copy
              where is_selected_dt) fmn on fmn.dt = fps.inserted_dt_utc::date
        join (select distinct
                  mapped_distribution_location
                  ,distribution_sb_alias
              from ${stg2_schema}.ps_manufacturing_to_distribution_center_history
              where is_selected_dt) mtd on mtd.mapped_distribution_location = fps.distribution_location
    where fps.is_within_capacity
    group by 1,2
)

/* Transform production scheduled from a SKU without MERV rating level
 * to a SKU level. */
, production_scheduled as (
    ------------
    -- MERV 8 --
    ------------
    select
        distribution_sb_alias
        ,sku_without_merv_rating || 'M8'    as sku
        ,production_goal_merv_8             as production_scheduled
    from base_production_scheduled
    where production_goal_merv_8 <> 0
    union all
    -------------
    -- MERV 11 --
    -------------
    select
        distribution_sb_alias
        ,sku_without_merv_rating || 'M11'    as sku
        ,production_goal_merv_11             as production_scheduled
    from base_production_scheduled
    where production_goal_merv_11 <> 0
    union all
    -------------
    -- MERV 13 --
    -------------
    select
        distribution_sb_alias
        ,sku_without_merv_rating || 'M13'    as sku
        ,production_goal_merv_13             as production_scheduled
    from base_production_scheduled
    where production_goal_merv_13 <> 0
    union all
    ----------------------------
    -- MERV 8 Odor Eliminator --
    ----------------------------
    select
        distribution_sb_alias
        ,sku_without_merv_rating || 'OE'        as sku
        ,production_goal_merv_8_odor_eliminator as production_scheduled
    from base_production_scheduled
    where production_goal_merv_8_odor_eliminator <> 0
)

/* Return base dimensions, including:
 *      (1) inventory currently in stock
 *      (2) production scheduled to send to DCs
 *      (3) sales forecasted for yesterday
 */
, base_dimensions as (
    ------------------------
    -- Inventory in stock --
    ------------------------
    select
        inserted_dt_utc
        ,location_name
        ,sku
        ,sku_without_merv_rating
        ,filter_type
        ,merv_rating
    from ${stg2_schema}.ps_fact_manufacturing_need_future_copy
    where is_selected_dt
    union
    --------------------------
    -- Production scheduled --
    --------------------------
    select
        fmn.inserted_dt_utc
        ,ps.distribution_sb_alias as location_name
        ,ps.sku
        ,di.sku_without_merv_rating
        ,di.filter_type
        ,di.merv_rating
    from production_scheduled ps
        join (select distinct inserted_dt_utc
              from ${stg2_schema}.ps_fact_manufacturing_need_future_copy
              where is_selected_dt) fmn on true
        join ${stg2_schema}.dw_sku_to_dim_item_id dsdi on dsdi.sku = ps.sku
            and dsdi.rn = 1
        join ${dwh_schema}.dim_item di on di.dim_item_id = dsdi.dim_item_id
    union
    ----------------------
    -- Forecasted sales --
    ----------------------
    select
        f.inserted_dt_utc
        ,psf.sb_location_name as location_name
        ,psf.sku
        ,psf.sku_without_merv_rating
        ,psf.filter_type
        ,psf.merv_rating
    from ${stg2_schema}.ps_sales_forecast_history psf
        join (select distinct inserted_dt_utc, location_name
            from ${stg2_schema}.ps_fact_manufacturing_need_future_copy) f on f.inserted_dt_utc::date = psf.dt
                and f.location_name = psf.sb_location_name
    where psf.is_selected_dt

)

/* Base records table.
 * For each record that is (1) currently in stock or (2) scheduled to be produced or (3) both, calculate quantity in stock.
 * Assume that in-transit and on-order quantities impact inventory immediately.
 */
, base_records as (
    select
        f.runtime_dt_utc
        ,bd.inserted_dt_utc + interval '1 day' as inserted_dt_utc
        ,bd.location_name
        ,bd.sku
        ,bd.filter_type
        ,bd.merv_rating
        ,bd.sku_without_merv_rating
        ,coalesce(fmn.quantity_in_stock, 0)             /* Current quantity in stock */
            + coalesce(fmn.quantity_in_transit, 0)      /* Plus in-transit */
            - coalesce(fmn.quantity_on_order, 0)        /* Minus on-order */
            - coalesce(eir.excess_inventory_removal, 0) /* Minus excess inventory removal */
            + coalesce(ps.production_scheduled, 0)      /* Plus production/excess distribution scheduled */
            as available_quantity_at_start_of_prior_day
        ,coalesce(fmn.quantity_in_stock, 0)             /* Current quantity in stock */
            + coalesce(fmn.quantity_in_transit, 0)      /* Plus in-transit */
            - coalesce(fmn.quantity_on_order, 0)        /* Minus on-order */
            - coalesce(eir.excess_inventory_removal, 0) /* Minus excess inventory removal */
            + coalesce(ps.production_scheduled, 0)      /* Plus production/excess distribution scheduled */
            - coalesce(psf.quantity, 0)                 /* Minus sales for the prior day */
            as available_quantity_at_start_of_current_day
        ,0 as quantity_in_transit
        ,0 as quantity_on_order
        ,fmn.daily_sales_avg_rolling_28_day
        ,fmn.weekly_sales_avg_rolling_28_day
        ,fmn.is_custom
    from base_dimensions bd
        join (select distinct runtime_dt_utc
            from ${stg2_schema}.ps_fact_manufacturing_need_future_copy) f on true
        left join ${stg2_schema}.ps_fact_manufacturing_need_future_copy fmn on fmn.location_name = bd.location_name
            and fmn.sku = bd.sku
        left join excess_inventory_removal eir on eir.manufacturing_sb_alias = bd.location_name
            and eir.sku = bd.sku
        left join production_scheduled ps on ps.distribution_sb_alias = bd.location_name
            and ps.sku = bd.sku
        left join ${stg2_schema}.ps_sales_forecast_history psf on psf.is_selected_dt
            and psf.dt = bd.inserted_dt_utc::date
            and psf.sb_location_name = bd.location_name
            and psf.sku = bd.sku
    where fmn.is_selected_dt
)

/* Return the sales forecast for days 1..30 */
, sales_forecast as (
    select
        dsf.sb_location_name
        ,dsf.sku
        ,dsf.daily_sales_avg_rolling_28_day
        ,dsf.weekly_sales_avg_rolling_28_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '0 day') then dsf.quantity else 0 end) as sales_forecast_1_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '1 day') then dsf.quantity else 0 end) as sales_forecast_2_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '2 day') then dsf.quantity else 0 end) as sales_forecast_3_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '3 day') then dsf.quantity else 0 end) as sales_forecast_4_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '4 day') then dsf.quantity else 0 end) as sales_forecast_5_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '5 day') then dsf.quantity else 0 end) as sales_forecast_6_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '6 day') then dsf.quantity else 0 end) as sales_forecast_7_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '7 day') then dsf.quantity else 0 end) as sales_forecast_8_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '8 day') then dsf.quantity else 0 end) as sales_forecast_9_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '9 day') then dsf.quantity else 0 end) as sales_forecast_10_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '10 day') then dsf.quantity else 0 end) as sales_forecast_11_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '11 day') then dsf.quantity else 0 end) as sales_forecast_12_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '12 day') then dsf.quantity else 0 end) as sales_forecast_13_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '13 day') then dsf.quantity else 0 end) as sales_forecast_14_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '14 day') then dsf.quantity else 0 end) as sales_forecast_15_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '15 day') then dsf.quantity else 0 end) as sales_forecast_16_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '16 day') then dsf.quantity else 0 end) as sales_forecast_17_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '17 day') then dsf.quantity else 0 end) as sales_forecast_18_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '18 day') then dsf.quantity else 0 end) as sales_forecast_19_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '19 day') then dsf.quantity else 0 end) as sales_forecast_20_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '20 day') then dsf.quantity else 0 end) as sales_forecast_21_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '21 day') then dsf.quantity else 0 end) as sales_forecast_22_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '22 day') then dsf.quantity else 0 end) as sales_forecast_23_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '23 day') then dsf.quantity else 0 end) as sales_forecast_24_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '24 day') then dsf.quantity else 0 end) as sales_forecast_25_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '25 day') then dsf.quantity else 0 end) as sales_forecast_26_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '26 day') then dsf.quantity else 0 end) as sales_forecast_27_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '27 day') then dsf.quantity else 0 end) as sales_forecast_28_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '28 day') then dsf.quantity else 0 end) as sales_forecast_29_day
        ,sum(case when dsf.dt between fmn.inserted_dt_utc::date and (fmn.inserted_dt_utc::date + interval '29 day') then dsf.quantity else 0 end) as sales_forecast_30_day
    from ${stg2_schema}.ps_sales_forecast_future dsf
        join (select distinct inserted_dt_utc
              from ${stg2_schema}.ps_fact_manufacturing_need_future_copy
              where is_selected_dt) fmn on true
    group by 1,2,3,4
)

/* Estimate days of inventory remaining using sales forecast and current inventory levels.
 * Assume that on order and in transit quantities impact inventory immediately.
 */
, days_of_inventory_remaining as (
    /* dw_sales_forecast includes a sales forecast for the next N days.
     * If the rolling sum of sales is ever greater than or equal to the new quantity in stock within that period, return the minimum date that occurs. */
    select
        dsf.sb_location_name
        ,dsf.sku
        ,br.available_quantity_at_start_of_prior_day
        ,min(dsf.dt) as next_out_of_stock_dt
        ,date_diff('day', br.inserted_dt_utc::date, next_out_of_stock_dt) as days_of_inventory_remaining
    from ${stg2_schema}.ps_sales_forecast_future dsf
        join base_records br on br.location_name = dsf.sb_location_name
                and br.sku = dsf.sku
    where dsf.rolling_sum_quantity >= br.available_quantity_at_start_of_prior_day /* Where sum of {sales forecast} is >= {quantity in stock} */
    group by 1,2,3, br.inserted_dt_utc
    union all
    /* If the rolling sum of sales is always less than the new quantity in stock, assume that the SKU's next out of stock date is the maximum date in dw_sales_forecast */
    select
        dsf.sb_location_name
        ,dsf.sku
        ,br.available_quantity_at_start_of_prior_day
        ,max(dsf.dt) as next_out_of_stock_dt
        ,date_diff('day', br.inserted_dt_utc::date, next_out_of_stock_dt) as days_of_inventory_remaining
    from ${stg2_schema}.ps_sales_forecast_future dsf
        left join base_records br on br.location_name = dsf.sb_location_name
            and br.sku = dsf.sku
    group by 1,2,3, br.inserted_dt_utc
    having br.available_quantity_at_start_of_prior_day > max(dsf.rolling_sum_quantity)
)

select
    br.runtime_dt_utc
    ,br.inserted_dt_utc
    ,true as is_selected_dt
    ,br.location_name
    ,br.sku
    ,br.filter_type
    ,br.merv_rating
    ,br.sku_without_merv_rating
    ,br.available_quantity_at_start_of_current_day as quantity_in_stock
    ,br.quantity_in_transit
    ,br.quantity_on_order
    ,psf.quantity as sales_on_dt
    ,br.daily_sales_avg_rolling_28_day
    ,br.weekly_sales_avg_rolling_28_day
    ,case when doir.days_of_inventory_remaining < 0 then 0
        else doir.days_of_inventory_remaining end as days_of_inventory_remaining

    /* PLANNED MANUFACTURING
     * When {sales_forecast_n_days} - {available_quantity_at_start_of_current_day} is less than 0, return 0.
     * Else return {sales_forecast_n_days} - {available_quantity_at_start_of_current_day}.
     */
    ,case when (coalesce(sf.sales_forecast_1_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_1_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_1_day
    ,case when (coalesce(sf.sales_forecast_2_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_2_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_2_day
    ,case when (coalesce(sf.sales_forecast_3_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_3_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_3_day
    ,case when (coalesce(sf.sales_forecast_4_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_4_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_4_day
    ,case when (coalesce(sf.sales_forecast_5_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_5_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_5_day
    ,case when (coalesce(sf.sales_forecast_6_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_6_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_6_day
    ,case when (coalesce(sf.sales_forecast_7_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_7_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_7_day
    ,case when (coalesce(sf.sales_forecast_8_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_8_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_8_day
    ,case when (coalesce(sf.sales_forecast_9_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_9_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_9_day
    ,case when (coalesce(sf.sales_forecast_10_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_10_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_10_day
    ,case when (coalesce(sf.sales_forecast_11_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_11_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_11_day
    ,case when (coalesce(sf.sales_forecast_12_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_12_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_12_day
    ,case when (coalesce(sf.sales_forecast_13_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_13_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_13_day
    ,case when (coalesce(sf.sales_forecast_14_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_14_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_14_day
    ,case when (coalesce(sf.sales_forecast_15_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_15_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_15_day
    ,case when (coalesce(sf.sales_forecast_16_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_16_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_16_day
    ,case when (coalesce(sf.sales_forecast_17_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_17_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_17_day
    ,case when (coalesce(sf.sales_forecast_18_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_18_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_18_day
    ,case when (coalesce(sf.sales_forecast_19_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_19_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_19_day
    ,case when (coalesce(sf.sales_forecast_20_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_20_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_20_day
    ,case when (coalesce(sf.sales_forecast_21_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_21_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_21_day
    ,case when (coalesce(sf.sales_forecast_22_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_22_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_22_day
    ,case when (coalesce(sf.sales_forecast_23_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_23_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_23_day
    ,case when (coalesce(sf.sales_forecast_24_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_24_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_24_day
    ,case when (coalesce(sf.sales_forecast_25_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_25_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_25_day
    ,case when (coalesce(sf.sales_forecast_26_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_26_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_26_day
    ,case when (coalesce(sf.sales_forecast_27_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_27_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_27_day
    ,case when (coalesce(sf.sales_forecast_28_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_28_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_28_day
    ,case when (coalesce(sf.sales_forecast_29_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_29_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_29_day
    ,case when (coalesce(sf.sales_forecast_30_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0)) < 0 then 0
        else coalesce(sf.sales_forecast_30_day, 0) - coalesce(br.available_quantity_at_start_of_current_day, 0) end             as planned_manufacturing_30_day
    ,br.is_custom
from base_records br
    left join sales_forecast sf on sf.sb_location_name = br.location_name
        and sf.sku = br.sku
    left join days_of_inventory_remaining doir on doir.sb_location_name = br.location_name
        and doir.sku = br.sku
    left join ${stg2_schema}.ps_sales_forecast_future psf on psf.dt = br.inserted_dt_utc::date
        and psf.sb_location_name = br.location_name
        and psf.sku = br.sku