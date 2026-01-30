-- Job: ps_automated_production_schedule_staging_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4608766
-- Component ID: 4608764

select
    runtime_dt_utc
    ,inserted_dt_utc
    ,manufacturing_location
    ,distribution_location
    ,line_type
    ,sku_without_merv_rating
    ,lines_available_over_manufacturing_location_line_type
    ,rank_over_manufacturing_location_line_type
    ,filters_per_pallet_over_sku_without_merv_rating
    ,production_goal_merv_8
    ,production_goal_merv_11
    ,production_goal_merv_13
    ,production_goal_merv_8_odor_eliminator
    ,production_goal_total
    ,production_lines
    ,changeover_production_lines
    ,is_current_production_schedule
    ,is_tomorrow_production_schedule
    ,is_future_production_schedule
    ,is_within_capacity
    ,current_days_of_inventory_merv_8
    ,current_days_of_inventory_merv_11
    ,current_days_of_inventory_merv_13
    ,current_days_of_inventory_merv_8_odor_eliminator
from ${dwh_schema}.fact_production_schedule
where (is_tomorrow_production_schedule or is_future_production_schedule)
	and manufacturing_location in (${manufacturing_locations})
    and line_type = 'Automated'
    and is_within_capacity
    and case when '${is_sat_sun_flg}'::int = 1 then
        /* If the current date is Saturday or Sunday then don't select dates that are immutable (i.e., this Saturday, Sunday or Monday) */
            case when
                inserted_dt_utc::date not in
                    ((date_trunc('week', current_timestamp at time zone '${timezone}') + interval '5 day')::date /* This Saturday*/
                    ,(date_trunc('week', current_timestamp at time zone '${timezone}') + interval '6 day')::date /* This Sunday */
                    ,date_trunc('week', current_timestamp at time zone '${timezone}' + interval '7 day')::date /* Next Monday*/
                    ) then true
            else false end
        /* If the current date is not Saturday or Sunday, then return all dates */
        else true end