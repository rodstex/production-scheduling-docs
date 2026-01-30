-- Job: fps_automated_reschedule_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4608977
-- Component ID: 4609006

with rescheduled as (
    select * from ${stg2_schema}.ps_auto_day_one union all
    select * from ${stg2_schema}.ps_auto_day_two union all
    select * from ${stg2_schema}.ps_auto_day_three union all
    select * from ${stg2_schema}.ps_auto_day_four union all
    select * from ${stg2_schema}.ps_auto_day_five union all
    select * from ${stg2_schema}.ps_auto_day_six union all
    select * from ${stg2_schema}.ps_auto_day_seven
)

select
    rs.new_scheduled_dt as inserted_dt_utc
    ,fps.manufacturing_location
    ,fps.distribution_location
    ,fps.line_type
    ,rs.new_lines_available_over_manufacturing_location_line_type as lines_available_over_manufacturing_location_line_type
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
    ,fps.inserted_dt_utc as automated_original_inserted_dt_utc
from ${stg2_schema}.ps_automated_production_schedule_staging fps
    join rescheduled rs on rs.mapped_manufacturing_location = fps.manufacturing_location
        and rs.sku_without_merv_rating = fps.sku_without_merv_rating
        and rs.original_scheduled_dt = fps.inserted_dt_utc
where fps.line_type = 'Automated'
    and fps.is_within_capacity
    and fps.manufacturing_location in (${manufacturing_locations})
    and date_trunc('week', fps.inserted_dt_utc) = date_trunc('week', '${auto_reschedule_week_dt}'::date)