-- Job: fps_excess
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/excess_production
-- Component: Initialize stg2.ps_excess_inventory
-- Type: SQL Query
-- Job ID: 4347057
-- Component ID: 4347084

-------------------
-- NON-AUTOMATED --
-------------------
with non_auto_base_records as (
    select distinct
        fmn.inserted_dt_utc
        ,fmn.location_name
        ,fmn.sku
        ,fmn.dim_item_id
        ,fmn.filter_type
        ,fmn.sku_without_merv_rating
        ,fmn.merv_rating
        ,tdih.target_days_of_inventory
        ,fmn.quantity_in_stock
            - coalesce(fmn.quantity_on_order, 0)
            + coalesce(fmn.quantity_in_transit, 0) as quantity_available
    from ${stg2_schema}.ps_fact_manufacturing_need_history fmn
        join ${stg2_schema}.dw_sku_to_dim_item_id dsdi on dsdi.sku = fmn.sku
            and dsdi.rn = 1
        join ${dwh_schema}.dim_item di on di.dim_item_id = dsdi.dim_item_id
        join ${stg2_schema}.ps_target_days_of_inventory_history tdih on tdih.is_selected_dt /* Is current record */
            and tdih.grouped_line_type = 'Non-Automated' /* Excess inventory is based off of non-automated goals */
            and tdih.distribution_sb_alias = fmn.location_name
            and tdih.filter_type = di.filter_type
        join ${stg2_schema}.ps_manufacturing_to_distribution_center_history mdch on mdch.is_selected_dt
            and mdch.distribution_sb_alias = fmn.location_name
        join ${stg2_schema}.ps_line_count_history lch on lch.is_selected_dt
            and lch.mapped_manufacturing_location = mdch.mapped_manufacturing_location
            and lch.line_type not in ('Automated')
        join ${stg2_schema}.ps_capacity_by_sku_history caph on caph.is_selected_dt
            and caph.mapped_manufacturing_location = mdch.mapped_manufacturing_location
            and caph.sku_without_merv_rating = fmn.sku_without_merv_rating
            and caph.line_type = lch.line_type
    where fmn.is_selected_dt /* Is current record */
        and fmn.location_name in ('slc','pittsburgh','pope','newberry') /* Is manufacturing location */
        and (fmn.quantity_in_stock
            - coalesce(fmn.quantity_on_order, 0)
            + coalesce(fmn.quantity_in_transit, 0)) > 0 /* Has quantity in stock */
)

---------------------------
-- (Talladega, AL (Pope) --
---------------------------
, pope as ( /* DATA-488 */
    select distinct
        fmn.inserted_dt_utc
        ,fmn.location_name
        ,fmn.sku
        ,fmn.dim_item_id
        ,fmn.filter_type
        ,fmn.sku_without_merv_rating
        ,fmn.merv_rating
        ,tdih.target_days_of_inventory
        ,fmn.quantity_in_stock
            - coalesce(fmn.quantity_on_order, 0)
            + coalesce(fmn.quantity_in_transit, 0) as quantity_available
    from ${stg2_schema}.ps_fact_manufacturing_need_history fmn
        left join
            (select mapped_manufacturing_location
            from ${stg2_schema}.ps_manufacturing_to_distribution_center_history
            where is_selected_dt
                and mapped_manufacturing_location = 'Talladega, AL (Pope)') pope on true
        join ${stg2_schema}.dw_sku_to_dim_item_id dsdi on dsdi.sku = fmn.sku
            and dsdi.rn = 1
        join ${dwh_schema}.dim_item di on di.dim_item_id = dsdi.dim_item_id
        join ${stg2_schema}.ps_target_days_of_inventory_history tdih on tdih.is_selected_dt /* Is current record */
            and tdih.grouped_line_type = 'Non-Automated' /* Excess inventory is based off of non-automated goals */
            and tdih.distribution_sb_alias = fmn.location_name
            and tdih.filter_type = di.filter_type
    where case when pope.mapped_manufacturing_location is null then true
        else false end
        and fmn.location_name = 'pope' /* Only concerned with Talladega, AL (Pope) */
        and fmn.is_selected_dt /* Is current record */
        and (fmn.quantity_in_stock
            - coalesce(fmn.quantity_on_order, 0)
            + coalesce(fmn.quantity_in_transit, 0)) > 0 /* Has quantity in stock */
)

---------------
-- AUTOMATED --
---------------
, auto_base_records as (
    select distinct
        fmn.inserted_dt_utc
        ,fmn.location_name
        ,fmn.sku
        ,fmn.dim_item_id
        ,fmn.filter_type
        ,fmn.sku_without_merv_rating
        ,fmn.merv_rating
        ,tdih.target_days_of_inventory
        ,fmn.quantity_in_stock
            - coalesce(fmn.quantity_on_order, 0)
            + coalesce(fmn.quantity_in_transit, 0) as quantity_available
    from ${stg2_schema}.ps_fact_manufacturing_need_history fmn
        join ${stg2_schema}.dw_sku_to_dim_item_id dsdi on dsdi.sku = fmn.sku
            and dsdi.rn = 1
        join ${dwh_schema}.dim_item di on di.dim_item_id = dsdi.dim_item_id
        join ${stg2_schema}.ps_target_days_of_inventory_history tdih on tdih.is_selected_dt /* Is current record */
            and tdih.grouped_line_type = 'Non-Automated' /* Excess inventory is based off of non-automated goals */
            and tdih.distribution_sb_alias = fmn.location_name
            and tdih.filter_type = di.filter_type
        join ${stg2_schema}.ps_manufacturing_to_distribution_center_history mdch on mdch.is_selected_dt
            and mdch.distribution_sb_alias = fmn.location_name
        join ${stg2_schema}.ps_line_count_history lch on lch.is_selected_dt
            and lch.mapped_manufacturing_location = mdch.mapped_manufacturing_location
            and lch.line_type in ('Automated')
        join ${stg2_schema}.ps_capacity_by_sku_history caph on caph.is_selected_dt
            and caph.mapped_manufacturing_location = mdch.mapped_manufacturing_location
            and caph.sku_without_merv_rating = fmn.sku_without_merv_rating
            and caph.line_type = lch.line_type
    where fmn.is_selected_dt /* Is current record */
        and fmn.location_name in ('slc','pittsburgh','pope','newberry') /* Is manufacturing location */
        and (fmn.quantity_in_stock
            - coalesce(fmn.quantity_on_order, 0)
            + coalesce(fmn.quantity_in_transit, 0)) > 0 /* Has quantity in stock */
)

, base_records as (
    select *
    from non_auto_base_records
    union all
    select *
    from pope
    union all
    select *
    from auto_base_records
)

, sales_forecast as (
    select
        sfh.sb_location_name
        ,sfh.dim_item_id
        ,max(sfh.rolling_sum_quantity) as max_rolling_sum
    from ${stg2_schema}.ps_sales_forecast_history sfh
        join base_records fmn on fmn.dim_item_id = sfh.dim_item_id
            and fmn.location_name = sfh.sb_location_name
    where sfh.is_selected_dt
        and sfh.dt <= dateadd(day, fmn.target_days_of_inventory, fmn.inserted_dt_utc::timestamp)::date
    group by 1,2
)

select
    br.inserted_dt_utc
    ,br.location_name
    ,br.sku
    ,br.dim_item_id
    ,br.sku_without_merv_rating
    ,br.filter_type
    ,br.merv_rating
    ,br.target_days_of_inventory
    ,br.quantity_available
    ,psh.excess_inventory_perc_of_target_inventory
    ,max(sfh.max_rolling_sum)                                                                  as target_quantity
    ,round(max(sfh.max_rolling_sum)::float * psh.excess_inventory_perc_of_target_inventory)    as target_quantity_plus_safety_stock
    ,br.quantity_available - target_quantity_plus_safety_stock                                 as excess_quantity
from base_records br
    join sales_forecast sfh on sfh.sb_location_name = br.location_name
        and sfh.dim_item_id = br.dim_item_id
    join ${stg2_schema}.ps_settings_history psh on psh.is_selected_dt
group by 1,2,3,4,5,6,7,8,9,10
having excess_quantity > 0