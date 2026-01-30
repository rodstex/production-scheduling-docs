-- Job: fps_reassignment_trans2
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4877362
-- Component ID: 4878792

with max_rank as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,line_type
        ,max(rank_over_manufacturing_location_line_type) as max_rank_over_manufacturing_location_line_type
    from ${dwh_schema}.fact_production_schedule
    where (is_tomorrow_production_schedule or is_future_production_schedule)
        and is_within_capacity
        and line_type in ('Single Loader', 'Double Loader', 'Manual')
    group by 1,2,3
)

select
    prs.inserted_dt_utc
    ,prs.manufacturing_location
    ,prs.distribution_location
    ,prs.line_type
    ,prs.lines_available_over_manufacturing_location_line_type
    ,row_number() over (partition by prs.inserted_dt_utc, prs.manufacturing_location, prs.line_type
                        order by prs.rank_over_manufacturing_location_line_type)
     + coalesce(mr.max_rank_over_manufacturing_location_line_type, 0) as rank_over_manufacturing_location_line_type
    ,prs.sku_without_merv_rating
    ,prs.filters_per_pallet_over_sku_without_merv_rating
    ,case when prs.production_goal_merv_8 >= coalesce(fps.production_goal_merv_8, 0)
        then prs.production_goal_merv_8 - coalesce(fps.production_goal_merv_8, 0) 
        else 0 end as production_goal_merv_8_calc
    ,case when prs.production_goal_merv_11 >= coalesce(fps.production_goal_merv_11, 0)
        then prs.production_goal_merv_11 - coalesce(fps.production_goal_merv_11, 0) 
        else 0 end as production_goal_merv_11_calc
    ,case when prs.production_goal_merv_13 >= coalesce(fps.production_goal_merv_13, 0)
        then prs.production_goal_merv_13 - coalesce(fps.production_goal_merv_13, 0) 
        else 0 end as production_goal_merv_13_calc
    ,case when prs.production_goal_merv_8_odor_eliminator >= coalesce(fps.production_goal_merv_8_odor_eliminator, 0)
        then prs.production_goal_merv_8_odor_eliminator - coalesce(fps.production_goal_merv_8_odor_eliminator, 0) 
        else 0 end as production_goal_merv_8_odor_eliminator_calc
    ,production_goal_merv_8_calc
        + production_goal_merv_11_calc
        + production_goal_merv_13_calc
        + production_goal_merv_8_odor_eliminator_calc as production_goal_total
    ,((prs.production_goal_merv_8 - coalesce(fps.production_goal_merv_8, 0)
        + prs.production_goal_merv_11 - coalesce(fps.production_goal_merv_11, 0)
        + prs.production_goal_merv_13 - coalesce(fps.production_goal_merv_13, 0)
        + prs.production_goal_merv_8_odor_eliminator - coalesce(fps.production_goal_merv_8_odor_eliminator, 0))::float
        / caph.production_capacity::float)::decimal(10,2) as production_lines
    ,prs.current_days_of_inventory_merv_8
    ,prs.current_days_of_inventory_merv_11
    ,prs.current_days_of_inventory_merv_13
    ,prs.current_days_of_inventory_merv_8_odor_eliminator
    ,prs.is_current_production_schedule
    ,prs.is_future_production_schedule
    ,false as is_within_capacity
    ,prs.is_tomorrow_production_schedule
    ,prs.runtime_dt_utc
    ,prs.non_automated_logic_type
    ,prs.changeover_production_lines
    ,prs.automated_original_inserted_dt_utc
    ,prs.non_automated_efficiency_rank
    ,prs.automated_production_lines_available_to_reassign as reassigned_automated_production_lines_over_dt_manufacturing_location
from ${stg2_schema}.ps_reassignment_staging prs
    left join ${dwh_schema}.fact_production_schedule fps on fps.inserted_dt_utc = prs.inserted_dt_utc
        and fps.manufacturing_location = prs.manufacturing_location
        and fps.distribution_location = prs.distribution_location
        and fps.reassigned_automated_production_lines_over_dt_manufacturing_location is not null
    join ${stg2_schema}.ps_capacity_by_sku_history caph on caph.is_selected_dt
        and caph.mapped_manufacturing_location = prs.manufacturing_location
        and caph.line_type = prs.line_type
        and caph.sku_without_merv_rating = prs.sku_without_merv_rating
    left join max_rank mr on mr.inserted_dt_utc = prs.inserted_dt_utc
        and mr.manufacturing_location = prs.manufacturing_location
        and mr.line_type = prs.line_type
where
     (production_goal_merv_8_calc
        + production_goal_merv_11_calc
        + production_goal_merv_13_calc
        + production_goal_merv_8_odor_eliminator_calc) > 0