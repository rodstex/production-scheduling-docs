-- Job: fps_non_automated_reactive_min_threshold_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4424252
-- Component ID: 4424283

select
    runtime_dt_utc
    ,inserted_dt_utc
    ,mapped_manufacturing_location
    ,mapped_distribution_location
    ,'Non-Automated Reactive Threshold Not Meet | Iteration ' || '${iteration_num2}' as line_type
    ,null::int                                      as lines_available_over_manufacturing_location_line_type
    ,null::int                                      as rank_over_manufacturing_location_line_type
    ,sku_without_merv_rating
    ,filters_per_pallet                             as filters_per_pallet_over_sku_without_merv_rating
    ,sum(case when merv_rating = 'MERV 8'                   then production_need else 0 end) as production_goal_merv_8
    ,sum(case when merv_rating = 'MERV 11'                  then production_need else 0 end) as production_goal_merv_11
    ,sum(case when merv_rating = 'MERV 13'                  then production_need else 0 end) as production_goal_merv_13
    ,sum(case when merv_rating = 'MERV 8 Odor Eliminator'   then production_need else 0 end) as production_goal_merv_8_odor_eliminator
    ,sum(production_need) as production_goal_total
    ,null::decimal(10,2) as production_lines
    ,null::decimal(10,2) as changeover_production_lines
    ,min(case when merv_rating = 'MERV 8'                   then days_of_inventory_remaining end) as current_days_of_inventory_merv_8
    ,min(case when merv_rating = 'MERV 11'                  then days_of_inventory_remaining end) as current_days_of_inventory_merv_11
    ,min(case when merv_rating = 'MERV 13'                  then days_of_inventory_remaining end) as current_days_of_inventory_merv_13
    ,min(case when merv_rating = 'MERV 8 Odor Eliminator'   then days_of_inventory_remaining end) as current_days_of_inventory_merv_8_odor_eliminator
    ,false                                                              as is_current_production_schedule
    ,case when '${iteration_num0}'::int = 2 then true else false end    as is_tomorrow_production_schedule
    ,case when '${iteration_num0}'::int = 2 then false else true end    as is_future_production_schedule
    ,false                                                              as is_within_capacity
    ,'Reactive'                                                         as non_automated_logic_type
from ${stg2_schema}.ps_non_automated_ranking
where is_unscheduled_production
group by 1,2,3,4,5,6,7,8,9