-- Job: run_production_schedule_v4
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: COPY fact_production_schedule 00
-- Type: SQL Query
-- Job ID: 4308379
-- Component ID: 4925604

select
    current_timestamp as copied_dt_utc
    ,inserted_dt_utc
    ,manufacturing_location
    ,distribution_location
    ,line_type
    ,lines_available_over_manufacturing_location_line_type
    ,rank_over_manufacturing_location_line_type
    ,sku_without_merv_rating
    ,filters_per_pallet_over_sku_without_merv_rating
    ,production_goal_merv_8
    ,production_goal_merv_11
    ,production_goal_merv_13
    ,production_goal_merv_8_odor_eliminator
    ,production_goal_total
    ,production_lines
    ,changeover_production_lines
    ,current_days_of_inventory_merv_8
    ,current_days_of_inventory_merv_11
    ,current_days_of_inventory_merv_13
    ,current_days_of_inventory_merv_8_odor_eliminator
    ,ending_days_of_inventory_merv_8
    ,ending_days_of_inventory_merv_11
    ,ending_days_of_inventory_merv_13
    ,ending_days_of_inventory_merv_8_odor_eliminator
    ,is_current_production_schedule
    ,is_future_production_schedule
    ,is_within_capacity
    ,is_tomorrow_production_schedule
    ,runtime_dt_utc
    ,non_automated_logic_type
    ,automated_original_inserted_dt_utc
    ,non_automated_efficiency_rank
    ,reassigned_automated_production_lines_over_dt_manufacturing_location
    ,prior_to_rounding_rank_over_manufacturing_location_line_type
    ,prior_to_rounding_is_within_capacity
    ,prior_to_rounding_lines_available_over_manufacturing_location_line_type
from ${dwh_schema}.fact_production_schedule