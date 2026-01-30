-- Job: fps_non_automated_line_rounding
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_line_rounding
-- Component: Initialize stg2.ps_non_automated_rounding_staging
-- Type: SQL Query
-- Job ID: 4881076
-- Component ID: 4882556

select
    fps.inserted_dt_utc
    ,fps.manufacturing_location
    ,fps.distribution_location
    ,fps.line_type
    ,fps.lines_available_over_manufacturing_location_line_type
    ,fps.rank_over_manufacturing_location_line_type
    ,fps.sku_without_merv_rating
    ,fps.filters_per_pallet_over_sku_without_merv_rating
    ,fps.production_goal_merv_8
    ,fps.production_goal_merv_11
    ,fps.production_goal_merv_13
    ,fps.production_goal_merv_8_odor_eliminator
    ,fps.production_goal_total
    ,fps.production_lines
    ,fps.current_days_of_inventory_merv_8
    ,fps.current_days_of_inventory_merv_11
    ,fps.current_days_of_inventory_merv_13
    ,fps.current_days_of_inventory_merv_8_odor_eliminator
    ,fps.is_current_production_schedule
    ,fps.is_future_production_schedule
    ,fps.is_within_capacity
    ,fps.is_tomorrow_production_schedule
    ,fps.runtime_dt_utc
    ,fps.non_automated_logic_type
    ,fps.changeover_production_lines
    ,fps.automated_original_inserted_dt_utc
    ,fps.non_automated_efficiency_rank
    ,fps.reassigned_automated_production_lines_over_dt_manufacturing_location
    ,fps.non_automated_reactive_logic_iteration
from ${dwh_schema}.fact_production_schedule fps
    join ${stg2_schema}.ps_non_automated_line_rounding pr on pr.inserted_dt_utc = fps.inserted_dt_utc
        and pr.manufacturing_location = fps.manufacturing_location
        and pr.line_type = fps.line_type