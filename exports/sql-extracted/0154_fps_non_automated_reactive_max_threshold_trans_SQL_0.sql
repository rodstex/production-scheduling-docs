-- Job: fps_non_automated_reactive_max_threshold_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4433896
-- Component ID: 4433914

/* Identify all the SKU without MERV rating(s) that have a greater number of production lines than allowed on non-automated lines.
 * Identify the 1st record that fell outside of capacity.
 */
with base_records as (
    select
        mapped_manufacturing_location
        ,sku_without_merv_rating
        ,running_sum_production_lines_per_size
        ,row_number() over (partition by mapped_manufacturing_location, sku_without_merv_rating order by running_sum_production_lines_per_size asc) as rn
    from ${stg2_schema}.ps_non_automated_production_schedule_staging
    where not(is_copied_fps)
        and running_sum_production_lines_per_size > max_production_lines_per_size
)

/* Factor the 1st record that was greater than the number of production lines allowed to make it within any remaining capacity.
 * For example:
 * Any size can have 2 non-automated lines.
 * 20x20x1 needs 1.5 lines to go to Dallas.
 * 20x20x1 needs 1 line to go to Orlando.
 * Factor Orlando's demand to 0.5 lines.
 * Add 0.5 lines of Orlando's demand to 'Non-Automated Threshold Exceeded'.
 */
, factored as (
    select
        t.mapped_manufacturing_location
        ,t.mapped_distribution_location
        ,t.sku_without_merv_rating
        ,t.sku
        ,t.merv_rating
        ,t.production_capacity
        ,t.production_need
        ,t.max_production_lines_per_size
        ,t.running_sum_production_lines_per_size
        ,coalesce(t.max_production_lines_per_size - lag(t.running_sum_production_lines_per_size) over (partition by t.mapped_manufacturing_location, t.sku_without_merv_rating order by t.running_sum_production_lines_per_size asc)
            ,t.max_production_lines_per_size) as prior_lines_remaining
        ,case when t.running_sum_production_lines_per_size = br.running_sum_production_lines_per_size then true else false end as is_record_to_factor
        ,case when is_record_to_factor then round(t.production_capacity::float * prior_lines_remaining::float) end as production_need_within_capacity
        ,case when t.production_need > production_need_within_capacity then t.production_need - production_need_within_capacity else 0 end as production_need_outside_capacity
    from ${stg2_schema}.ps_non_automated_production_schedule_staging t
        join base_records br on br.rn = 1
            and br.mapped_manufacturing_location = t.mapped_manufacturing_location
            and br.sku_without_merv_rating = t.sku_without_merv_rating
            and br.running_sum_production_lines_per_size >= t.running_sum_production_lines_per_size
)

, transformed as (
    ----------------------------
    -- Factor production need --
    ----------------------------
    select
        t.inserted_dt_utc
        ,f.mapped_manufacturing_location                                                                                        as manufacturing_location
        ,f.mapped_distribution_location                                                                                         as distribution_location
        ,'Non-Automated Production Threshold Exceeded'                                                                          as line_type
        ,null::decimal(10,2)                                                                                                    as lines_available_over_manufacturing_location_line_type
        ,null::int                                                                                                              as rank_over_manufacturing_location_line_type
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
        ,false                                                                                                                  as is_within_capacity
        ,t.runtime_dt_utc
    from factored f
        join ${stg2_schema}.ps_non_automated_production_schedule_staging t on t.mapped_manufacturing_location = f.mapped_manufacturing_location
            and t.mapped_distribution_location = f.mapped_distribution_location
            and t.sku = f.sku
            and t.running_sum_production_lines_per_size = f.running_sum_production_lines_per_size
    where f.is_record_to_factor
    group by 1,2,3,4,5,6,7,8
        ,t.runtime_dt_utc,t.production_capacity
    union all

    ------------------------------------
    -- Production need over threshold --
    ------------------------------------
    select
        t.inserted_dt_utc
        ,t.mapped_manufacturing_location                                                                                        as manufacturing_location
        ,t.mapped_distribution_location                                                                                         as distribution_location
        ,'Non-Automated Production Threshold Exceeded'                                                                          as line_type
        ,null::decimal(10,2)                                                                                                    as lines_available_over_manufacturing_location_line_type
        ,null::int                                                                                                              as rank_over_manufacturing_location_line_type
        ,br.sku_without_merv_rating
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
        ,false                                                                                                                  as is_within_capacity
        ,t.runtime_dt_utc
    from base_records br
        join ${stg2_schema}.ps_non_automated_production_schedule_staging t on t.mapped_manufacturing_location = br.mapped_manufacturing_location
            and t.sku_without_merv_rating = br.sku_without_merv_rating
            and t.running_sum_production_lines_per_size = br.running_sum_production_lines_per_size
    where br.rn > 1
    group by 1,2,3,4,5,6,7,8
        ,t.runtime_dt_utc, t.production_capacity
)

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
    ,false                                                              as is_current_production_schedule
    ,case when '${iteration_num0}'::int = 2 then true else false end    as is_tomorrow_production_schedule
    ,case when '${iteration_num0}'::int = 2 then false else true end    as is_future_production_schedule
    ,is_within_capacity
    ,runtime_dt_utc
    ,'${non_automated_logic_type}' as non_automated_logic_type
from transformed t
group by 1,2,3,4,5,6,7,8
    ,15,16,17,18,19,20,21,22,23,24