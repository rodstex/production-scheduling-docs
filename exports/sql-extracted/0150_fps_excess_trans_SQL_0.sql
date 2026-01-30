-- Job: fps_excess_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/excess_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4347575
-- Component ID: 4347623

with base_wo_pope as (
    select
        pei.inserted_dt_utc
        ,mdc.mapped_manufacturing_location
        ,mdc.mapped_distribution_location
        ,pei.sku
        ,pei.sku_without_merv_rating
        ,pei.merv_rating
        ,pei.excess_quantity
        ,ped.excess_distribution_need
        ,ped.days_of_inventory_remaining
        ,sum(ped.excess_distribution_need) over (partition by mdc.mapped_manufacturing_location, pei.sku)   as excess_distribution_need_over_mfg_sku
        ,ped.excess_distribution_need::float / excess_distribution_need_over_mfg_sku::float                 as perc_distribution_need
        ,(perc_distribution_need * pei.excess_quantity::float)::int                                         as excess_distribution
        ,psh.is_deduct_excess_distribution_from_prod_need                                                   as is_within_capacity
    from ${stg2_schema}.ps_excess_inventory pei
        join ${stg2_schema}.ps_excess_distribution_need ped on pei.dim_item_id = ped.dim_item_id
        join ${stg2_schema}.ps_manufacturing_to_distribution_center_history mdc on mdc.is_selected_dt /* Truncate to current record */
            and mdc.manufacturing_sb_alias = pei.location_name
            and mdc.distribution_sb_alias = ped.location_name
        join ${stg2_schema}.ps_settings_history psh on psh.is_selected_dt
)

, pope as ( /* DATA-488 */
    select
        pei.inserted_dt_utc
        ,'Talladega, AL (Pope)'::text as mapped_manufacturing_location
        ,case when ped.location_name = 'dallas' then 'Dallas, TX'
            when ped.location_name = 'chicago' then 'Elgin, IL'
            when ped.location_name = 'orlando' then 'Orlando, FL'
            when ped.location_name = 'newberry' then 'Talladega, AL (Newberry)' end as mapped_distribution_location
        ,pei.sku
        ,pei.sku_without_merv_rating
        ,pei.merv_rating
        ,pei.excess_quantity
        ,ped.excess_distribution_need
        ,ped.days_of_inventory_remaining
        ,sum(ped.excess_distribution_need) over (partition by pei.sku)                      as excess_distribution_need_over_mfg_sku
        ,ped.excess_distribution_need::float / excess_distribution_need_over_mfg_sku::float as perc_distribution_need
        ,(perc_distribution_need * pei.excess_quantity::float)::int                         as excess_distribution
        ,psh.is_deduct_excess_distribution_from_prod_need                                   as is_within_capacity
    from ${stg2_schema}.ps_excess_inventory pei
        left join
            (select mapped_manufacturing_location
            from ${stg2_schema}.ps_manufacturing_to_distribution_center_history
            where is_selected_dt
                and mapped_manufacturing_location = 'Talladega, AL (Pope)') pope on true
        join ${stg2_schema}.ps_excess_distribution_need ped on pei.dim_item_id = ped.dim_item_id
        join ${stg2_schema}.ps_settings_history psh on psh.is_selected_dt
    where ped.location_name in ('dallas', 'chicago', 'orlando', 'newberry')
        and case when pope.mapped_manufacturing_location is null then true
            else false end
)

, base_records as (
    select *
    from base_wo_pope
    union all
    select *
    from pope
)

select
    br.inserted_dt_utc
    ,br.mapped_manufacturing_location
    ,br.mapped_distribution_location
    ,'Excess Distribution' as line_type
    ,br.sku_without_merv_rating
    ,fpp.filters_per_pallet                                                                             as filters_per_pallet_over_sku_without_merv_rating
    ,sum(case when br.merv_rating = 'MERV 8' then br.excess_distribution else 0 end)                    as production_goal_merv_8
    ,sum(case when br.merv_rating = 'MERV 11' then br.excess_distribution else 0 end)                   as production_goal_merv_11
    ,sum(case when br.merv_rating = 'MERV 13' then br.excess_distribution else 0 end)                   as production_goal_merv_13
    ,sum(case when br.merv_rating = 'MERV 8 Odor Eliminator' then br.excess_distribution else 0 end)    as production_goal_merv_8_odor_eliminator
    ,sum(br.excess_distribution)                                                                        as production_goal_total
    ,max(case when br.merv_rating = 'MERV 8' then br.days_of_inventory_remaining end)                   as current_days_of_inventory_merv_8
    ,max(case when br.merv_rating = 'MERV 11' then br.days_of_inventory_remaining end)                  as current_days_of_inventory_merv_11
    ,max(case when br.merv_rating = 'MERV 13' then br.days_of_inventory_remaining end)                  as current_days_of_inventory_merv_13
    ,max(case when br.merv_rating = 'MERV 8 Odor Eliminator' then br.days_of_inventory_remaining end)   as current_days_of_inventory_merv_8_odor_eliminator
    ,br.is_within_capacity
    ,true as is_current_production_schedule
    ,false as is_tomorrow_production_schedule
    ,false as is_future_production_schedule
from base_records br
    join ${stg2_schema}.ps_filters_per_pallet_history fpp on fpp.is_selected_dt
        and fpp.sku_without_merv_rating = br.sku_without_merv_rating
group by 1,2,3,4,5,6, br.is_within_capacity