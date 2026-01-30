-- Job: fps_non_auto_line_rounding_trans2
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_line_rounding
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4886971
-- Component ID: 4887021

with max_rank as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,line_type
        ,max(rank_over_manufacturing_location_line_type) as max_rank
    from ${dwh_schema}.fact_production_schedule
    group by 1,2,3
)

select
    s.inserted_dt_utc
    ,s.manufacturing_location
    ,s.distribution_location
    ,s.line_type
    ,par.adjusted_total_lines as lines_available_over_manufacturing_location_line_type
    ,row_number() over (partition by s.inserted_dt_utc, s.manufacturing_location, s.line_type
                        order by s.rank_over_manufacturing_location_line_type)
        + coalesce(mr.max_rank, 0) as rank_over_manufacturing_location_line_type
    ,s.sku_without_merv_rating
    ,s.filters_per_pallet_over_sku_without_merv_rating
    ,case when s.production_goal_merv_8 >= coalesce(fps.production_goal_merv_8, 0)
        then s.production_goal_merv_8 - coalesce(fps.production_goal_merv_8, 0) else 0 end as production_goal_merv_8_calc
    ,case when s.production_goal_merv_11 >= coalesce(fps.production_goal_merv_11, 0)
        then s.production_goal_merv_11 - coalesce(fps.production_goal_merv_11, 0) else 0 end as production_goal_merv_11_calc
    ,case when s.production_goal_merv_13 >= coalesce(fps.production_goal_merv_13, 0)
        then s.production_goal_merv_13 - coalesce(fps.production_goal_merv_13, 0) else 0 end as production_goal_merv_13_calc
    ,case when s.production_goal_merv_8_odor_eliminator >= coalesce(fps.production_goal_merv_8_odor_eliminator, 0)
        then s.production_goal_merv_8_odor_eliminator - coalesce(fps.production_goal_merv_8_odor_eliminator, 0) else 0 end as production_goal_merv_8_odor_eliminator_calc
    ,production_goal_merv_8_calc
        + production_goal_merv_11_calc
        + production_goal_merv_13_calc
        + production_goal_merv_8_odor_eliminator_calc as production_goal_total
    ,s.production_lines
    ,s.current_days_of_inventory_merv_8
    ,s.current_days_of_inventory_merv_11
    ,s.current_days_of_inventory_merv_13
    ,s.current_days_of_inventory_merv_8_odor_eliminator
    ,s.is_current_production_schedule
    ,s.is_future_production_schedule
    ,false as is_within_capacity
    ,s.is_tomorrow_production_schedule
    ,s.runtime_dt_utc
    ,s.non_automated_logic_type
    ,s.changeover_production_lines
    ,s.automated_original_inserted_dt_utc
    ,s.non_automated_efficiency_rank
    ,s.reassigned_automated_production_lines_over_dt_manufacturing_location
    ,s.rank_over_manufacturing_location_line_type               as prior_to_rounding_rank_over_manufacturing_location_line_type
    ,s.is_within_capacity                                       as prior_to_rounding_within_capacity
    ,s.lines_available_over_manufacturing_location_line_type    as prior_to_rounding_lines_available_over_manufacturing_location_line_type
    ,s.non_automated_reactive_logic_iteration
from ${stg2_schema}.ps_non_automated_rounding_staging s
    left join ${dwh_schema}.fact_production_schedule fps
         on s.inserted_dt_utc = fps.inserted_dt_utc
        and s.manufacturing_location = fps.manufacturing_location
        and s.distribution_location = fps.distribution_location
        and s.line_type = fps.line_type
        and s.rank_over_manufacturing_location_line_type = fps.prior_to_rounding_rank_over_manufacturing_location_line_type
    left join max_rank mr on mr.inserted_dt_utc = s.inserted_dt_utc
        and mr.manufacturing_location = s.manufacturing_location
        and mr.line_type = s.line_type
    join ${stg2_schema}.ps_non_automated_line_rounding par on par.inserted_dt_utc = s.inserted_dt_utc
        and par.manufacturing_location = s.manufacturing_location
        and par.line_type = s.line_type
where (s.production_goal_total - coalesce(fps.production_goal_total, 0)) > 0