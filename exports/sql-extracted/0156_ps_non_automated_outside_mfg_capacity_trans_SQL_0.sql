-- Job: ps_non_automated_outside_mfg_capacity_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4436182
-- Component ID: 4436202

/* Identify all the records that have a greater production need than the manufacturing location has staff for non-automated lines.
 * Identify the 1st record that fell outside of capacity.
 */
with base_records as (
    select
        mapped_manufacturing_location
        ,running_sum_staffing_over_mfg
        ,row_number() over (partition by mapped_manufacturing_location order by running_sum_staffing_over_mfg asc) as rn
    from ${stg2_schema}.ps_non_automated_production_schedule_staging
    where not(is_copied_fps)
        and running_sum_staffing_over_mfg > count_of_staffing_over_mfg
)

/* Factor the 1st record that was greater than the number of production lines allowed to make it within any remaining capacity.
 * For example:
 * Woodland has staff for 3 production lines
 * 20x20x1 needs 1.0 lines
 * 16x24x2 needs 0.5 lines
 * 20x30x2 needs 2.5 lines
 * Factor 20x30x2 demand to 1.5 lines.
 */
, factored as (
    select
        t.mapped_manufacturing_location
        ,t.mapped_distribution_location
        ,t.sku_without_merv_rating
        ,t.sku
        ,t.line_type
        ,t.merv_rating
        ,t.production_capacity
        ,t.production_need
        ,t.count_of_staffing_over_mfg
        ,t.running_sum_staffing_over_mfg
        ,case when t.running_sum_staffing_over_mfg > t.count_of_staffing_over_mfg then true else false end as is_outside_goal
        ,coalesce(t.count_of_staffing_over_mfg::float - lag(t.running_sum_staffing_over_mfg) over (partition by t.mapped_manufacturing_location order by t.running_sum_staffing_over_mfg asc)
            ,t.count_of_staffing_over_mfg::float) as prior_staff_remaining
        ,case when t.running_sum_staffing_over_mfg = br.running_sum_staffing_over_mfg then true else false end as is_record_to_factor
        ,case when is_record_to_factor then round(t.production_capacity::float * prior_staff_remaining::float) end as production_need_within_capacity
        ,case when t.production_need > production_need_within_capacity then t.production_need - production_need_within_capacity end as production_need_outside_capacity
    from ${stg2_schema}.ps_non_automated_production_schedule_staging t
        join base_records br on br.rn = 1
            and t.mapped_manufacturing_location = br.mapped_manufacturing_location
)

, transformed as (
    ----------------------------
    -- Factor production need --
    ----------------------------
    select
        t.inserted_dt_utc
        ,f.mapped_manufacturing_location                                                                                        as manufacturing_location
        ,f.mapped_distribution_location                                                                                         as distribution_location
        ,f.line_type
        ,f.count_of_staffing_over_mfg                                                                                           as lines_available_over_manufacturing_location_line_type
        ,t.ranking                                                                                                              as rank_over_manufacturing_location_line_type
        ,f.sku_without_merv_rating
        ,t.filter_per_pallet                                                                                                    as filters_per_pallet_over_sku_without_merv_rating
        ,max(case when t.merv_rating = 'MERV 8'                 then f.production_need_outside_capacity else 0 end)             as production_goal_merv_8
        ,max(case when t.merv_rating = 'MERV 11'                then f.production_need_outside_capacity else 0 end)             as production_goal_merv_11
        ,max(case when t.merv_rating = 'MERV 13'                then f.production_need_outside_capacity else 0 end)             as production_goal_merv_13
        ,max(case when t.merv_rating = 'MERV 8 Odor Eliminator' then f.production_need_outside_capacity else 0 end)             as production_goal_merv_8_odor_eliminator
        ,production_goal_merv_8 + production_goal_merv_11 + production_goal_merv_13 + production_goal_merv_8_odor_eliminator    as production_goal_total
        ,(production_goal_total::float / t.production_capacity::float)::decimal(10,2)                                           as production_lines
        ,max(case when t.merv_rating = 'MERV 8'                 then t.days_of_inventory_remaining end)                         as current_days_of_inventory_merv_8
        ,max(case when t.merv_rating = 'MERV 11'                then t.days_of_inventory_remaining end)                         as current_days_of_inventory_merv_11
        ,max(case when t.merv_rating = 'MERV 13'                then t.days_of_inventory_remaining end)                         as current_days_of_inventory_merv_13
        ,max(case when t.merv_rating = 'MERV 8 Odor Eliminator' then t.days_of_inventory_remaining end)                         as current_days_of_inventory_merv_8_odor_eliminator
        ,false                                                                                                                  as is_current_production_schedule
        ,false                                                                                                                  as is_tomorrow_production_schedule
        ,true                                                                                                                   as is_future_production_schedule
        ,false                                                                                                                  as is_within_capacity
        ,t.runtime_dt_utc
        ,t.changeover_production_lines_per_size
    from factored f
        join ${stg2_schema}.ps_non_automated_production_schedule_staging t on t.mapped_manufacturing_location = f.mapped_manufacturing_location
            and t.mapped_distribution_location = f.mapped_distribution_location
            and t.sku = f.sku
            and t.running_sum_staffing_over_mfg = f.running_sum_staffing_over_mfg
    where f.is_record_to_factor
        and f.production_need_outside_capacity > 0
    group by 1,2,3,4,5,6,7,8
        ,t.runtime_dt_utc,t.production_capacity,t.changeover_production_lines_per_size
    union all

    ------------------------------------
    -- Production need over threshold --
    ------------------------------------
    select
        t.inserted_dt_utc
        ,t.mapped_manufacturing_location                                                                                        as manufacturing_location
        ,t.mapped_distribution_location                                                                                         as distribution_location
        ,t.line_type
        ,t.count_of_staffing_over_mfg                                                                                           as lines_available_over_manufacturing_location_line_type
        ,t.ranking                                                                                                              as rank_over_manufacturing_location_line_type
        ,t.sku_without_merv_rating
        ,t.filter_per_pallet                                                                                                    as filters_per_pallet_over_sku_without_merv_rating
        ,max(case when t.merv_rating = 'MERV 8'                 then t.production_need else 0 end)                              as production_goal_merv_8
        ,max(case when t.merv_rating = 'MERV 11'                then t.production_need else 0 end)                              as production_goal_merv_11
        ,max(case when t.merv_rating = 'MERV 13'                then t.production_need else 0 end)                              as production_goal_merv_13
        ,max(case when t.merv_rating = 'MERV 8 Odor Eliminator' then t.production_need else 0 end)                              as production_goal_merv_8_odor_eliminator
        ,production_goal_merv_8 + production_goal_merv_11 + production_goal_merv_13 + production_goal_merv_8_odor_eliminator    as production_goal_total
        ,(production_goal_total::float / t.production_capacity::float)::decimal(10,2)                                           as production_lines
        ,max(case when t.merv_rating = 'MERV 8'                 then t.days_of_inventory_remaining end)                         as current_days_of_inventory_merv_8
        ,max(case when t.merv_rating = 'MERV 11'                then t.days_of_inventory_remaining end)                         as current_days_of_inventory_merv_11
        ,max(case when t.merv_rating = 'MERV 13'                then t.days_of_inventory_remaining end)                         as current_days_of_inventory_merv_13
        ,max(case when t.merv_rating = 'MERV 8 Odor Eliminator' then t.days_of_inventory_remaining end)                         as current_days_of_inventory_merv_8_odor_eliminator
        ,false                                                                                                                  as is_current_production_schedule
        ,false                                                                                                                  as is_tomorrow_production_schedule
        ,true                                                                                                                   as is_future_production_schedule
        ,false                                                                                                                  as is_within_capacity
        ,t.runtime_dt_utc
        ,t.changeover_production_lines_per_size
    from ${stg2_schema}.ps_non_automated_production_schedule_staging t
        left join base_records br on br.rn = 1
            and br.mapped_manufacturing_location = t.mapped_manufacturing_location
            and br.running_sum_staffing_over_mfg = t.running_sum_staffing_over_mfg
    where not(is_copied_fps)
        and t.running_sum_staffing_over_mfg > count_of_staffing_over_mfg
        and br.rn is null
    group by 1,2,3,4,5,6,7,8
        ,t.runtime_dt_utc, t.production_capacity,t.changeover_production_lines_per_size
)

, final as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,distribution_location
        ,line_type
        ,lines_available_over_manufacturing_location_line_type
        ,rank_over_manufacturing_location_line_type
        ,sku_without_merv_rating
        ,filters_per_pallet_over_sku_without_merv_rating
        ,sum(production_goal_merv_8) as production_goal_merv_8
        ,sum(production_goal_merv_11) as production_goal_merv_11
        ,sum(production_goal_merv_13) as production_goal_merv_13
        ,sum(production_goal_merv_8_odor_eliminator) as production_goal_merv_8_odor_eliminator
        ,sum(production_goal_total) as production_goal_total
        ,sum(production_lines) as production_lines
        ,current_days_of_inventory_merv_8
        ,current_days_of_inventory_merv_11
        ,current_days_of_inventory_merv_13
        ,current_days_of_inventory_merv_8_odor_eliminator
        ,is_current_production_schedule
        ,is_tomorrow_production_schedule
        ,is_future_production_schedule
        ,is_within_capacity
        ,runtime_dt_utc
        ,'Manufacturing Location Staffing Limit' as outside_capacity_logic_type
        ,changeover_production_lines_per_size
    from transformed t
    group by 1,2,3,4,5,6,7,8
        ,15,16,17,18,19,20,21,22,23,24,25
)

-- , prior_sku as (
--     select
--         t1.mapped_manufacturing_location
--         ,t1.line_type
--         ,t1.sku_without_merv_rating
--     from
--         (select
--             mapped_manufacturing_location
--             ,running_sum_staffing_over_mfg
--             ,row_number() over (partition by mapped_manufacturing_location order by running_sum_staffing_over_mfg desc) as rn
--         from ${stg2_schema}.ps_non_automated_production_schedule_staging
--         where not(is_copied_fps)
--             and running_sum_staffing_over_mfg <= count_of_staffing_over_mfg) t0
--         join ${stg2_schema}.ps_non_automated_production_schedule_staging t1 on not(t1.is_copied_fps)
--             and t1.running_sum_staffing_over_mfg <= t1.count_of_staffing_over_mfg
--             and t1.mapped_manufacturing_location = t0.mapped_manufacturing_location
--             and t1.running_sum_staffing_over_mfg = t0.running_sum_staffing_over_mfg
--     where t0.rn = 1
-- )

select
    f.inserted_dt_utc
    ,f.manufacturing_location
    ,f.distribution_location
    ,f.line_type
    ,f.lines_available_over_manufacturing_location_line_type
    ,f.rank_over_manufacturing_location_line_type
    ,f.sku_without_merv_rating
    ,f.filters_per_pallet_over_sku_without_merv_rating
    ,f.production_goal_merv_8
    ,f.production_goal_merv_11
    ,f.production_goal_merv_13
    ,f.production_goal_merv_8_odor_eliminator
    ,f.production_goal_total
    ,f.production_lines
    ,f.current_days_of_inventory_merv_8
    ,f.current_days_of_inventory_merv_11
    ,f.current_days_of_inventory_merv_13
    ,f.current_days_of_inventory_merv_8_odor_eliminator
    ,f.is_current_production_schedule
    ,f.is_tomorrow_production_schedule
    ,f.is_future_production_schedule
    ,f.is_within_capacity
    ,f.runtime_dt_utc
    ,'Manufacturing Location Staffing Limit' as outside_capacity_logic_type
--     ,coalesce(lag(f.sku_without_merv_rating) over (partition by f.manufacturing_location, f.line_type order by f.rank_over_manufacturing_location_line_type)
--         ,ps.sku_without_merv_rating) as prior_sku_without_merv_rating
--     ,case when prior_sku_without_merv_rating is null then 0::decimal
--         when f.sku_without_merv_rating = prior_sku_without_merv_rating then 0::decimal
--         else f.changeover_production_lines_per_size end as changeover_production_lines
from final f
--     left join prior_sku ps on ps.mapped_manufacturing_location = f.manufacturing_location
--         and ps.line_type = f.line_type