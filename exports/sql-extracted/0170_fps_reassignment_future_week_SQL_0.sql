-- Job: fps_reassignment_future_week
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4839861
-- Component ID: 4840174

/* Return all lines scheduled and within capacity.
 * Truncate to weeks where the line type is abstracted to 'Non-Automated'.
 */
with scheduled_lines as (
    select
        fps.inserted_dt_utc::date                               as dt
        ,fps.manufacturing_location                             as mapped_manufacturing_location
        ,fps.line_type
        ,sum(fps.production_lines)
            + sum(coalesce(fps.changeover_production_lines, 0)) as production_and_changeover_lines
    from ${stg2_schema}.ps_reassignment_staging fps
    where fps.line_type in ('Non-Automated')
        and fps.is_within_capacity
    group by 1,2,3
)

/* Calculate the number of non-automated lines available by date and manufacturing location.
 * Filter to records where lines remain available.
 */
, available_lines as (
    select
        sl.dt
        ,sl.mapped_manufacturing_location
        ,sl.line_type
        ,sl.production_and_changeover_lines
        ,sum(lch.count_of_lines)                            as total_lines
        ,total_lines - sl.production_and_changeover_lines   as lines_available
    from scheduled_lines sl
        join ${stg2_schema}.ps_line_count_history lch on lch.is_selected_dt
            and lch.mapped_manufacturing_location = sl.mapped_manufacturing_location
            and lch.line_type in ('Single Loader', 'Double Loader', 'Manual')
    group by 1,2,3,4
    having lines_available > 0
)

/* Talladega, AL (TMS) may have excess capacity but that location does not have non-automated production lines.
 * Thus, excess automated staff from that location needs to be redistributed to other Talladega locations.
 * To simplify management, any excess automated staff is reassigned to a single Talladega location.
 * Excess automated staff is reassigned to the location with the greatest number of lines available.
 */
, talladega_ranking as (
    select
        dt
        ,mapped_manufacturing_location
        ,sum(lines_available)                                                       as total_available_lines
        ,row_number() over (partition by dt order by total_available_lines desc)    as mapped_manufacturing_location_rank
    from available_lines
    where mapped_manufacturing_location like 'Talladega, AL%'
    group by 1,2
)

/* Add reassigned automated staff to the abstracted 'Non-Automated' line.
 * Truncate to records where an adjustment needs to occur.
 */
------------------------------------
-- New Kensington, PA & Ogden, UT --
------------------------------------
select
    fps.inserted_dt_utc
    ,fps.manufacturing_location
    ,fps.distribution_location
    ,fps.line_type
    ,fps.lines_available_over_manufacturing_location_line_type +
        case when pasr.adjustment > al.lines_available then al.lines_available
            else pasr.adjustment end as lines_available_over_manufacturing_location_line_type
    ,fps.rank_over_manufacturing_location_line_type
    ,fps.sku_without_merv_rating
    ,fps.filters_per_pallet_over_sku_without_merv_rating
    ,fps.production_goal_merv_8
    ,fps.production_goal_merv_11
    ,fps.production_goal_merv_13
    ,fps.production_goal_merv_8_odor_eliminator
    ,fps.production_goal_total
    ,fps.production_lines +
        case when pasr.adjustment > al.lines_available then al.lines_available
            else pasr.adjustment end as production_lines
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
    ,case when pasr.adjustment > al.lines_available then al.lines_available
        else pasr.adjustment end  as reassigned_automated_production_lines_over_dt_manufacturing_location
from ${stg2_schema}.ps_automated_staffing_reassignment pasr
    join ${stg2_schema}.ps_reassignment_staging fps on fps.inserted_dt_utc::date = pasr.dt::date  /* Truncate to date(s) where adjustments need to occur */
        and fps.manufacturing_location = pasr.mapped_manufacturing_location                 /* Truncate to location(s) where adjustments need to occur */
        and fps.line_type in ('Non-Automated')                                              /* Truncate to abstracted non-automated line */
        and fps.is_within_capacity                                                          /* Truncate to products within capacity */
    /* Truncate to the Talladega manufacturing location with the greatest number of lines available */
    join available_lines al on al.dt = pasr.dt::date
        and al.mapped_manufacturing_location = fps.manufacturing_location
where pasr.mapped_manufacturing_location not in ('Talladega, AL (TMS)')
union all

-------------------
-- Talladega, AL --
-------------------
select
    fps.inserted_dt_utc
    ,fps.manufacturing_location
    ,fps.distribution_location
    ,fps.line_type
    ,fps.lines_available_over_manufacturing_location_line_type +
        case when pasr.adjustment > tr.total_available_lines then tr.total_available_lines
            else pasr.adjustment end as lines_available_over_manufacturing_location_line_type
    ,fps.rank_over_manufacturing_location_line_type
    ,fps.sku_without_merv_rating
    ,fps.filters_per_pallet_over_sku_without_merv_rating
    ,fps.production_goal_merv_8
    ,fps.production_goal_merv_11
    ,fps.production_goal_merv_13
    ,fps.production_goal_merv_8_odor_eliminator
    ,fps.production_goal_total
    ,fps.production_lines +
        case when pasr.adjustment > tr.total_available_lines then tr.total_available_lines
            else pasr.adjustment end as production_lines
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
    ,case when pasr.adjustment > tr.total_available_lines then tr.total_available_lines
        else pasr.adjustment end  as reassigned_automated_production_lines_over_dt_manufacturing_location
from ${stg2_schema}.ps_automated_staffing_reassignment pasr
    join ${stg2_schema}.ps_reassignment_staging fps on fps.inserted_dt_utc::date = pasr.dt::date    /* Truncate to date(s) where adjustments need to occur */
        and fps.manufacturing_location like 'Talladega, AL%'                                        /* Truncate to location(s) where adjustments need to occur */
        and fps.line_type in ('Non-Automated')                                                      /* Truncate to abstracted non-automated line */
        and fps.is_within_capacity                                                                  /* Truncate to products within capacity */
    /* Truncate to the Talladega manufacturing location with the greatest number of lines available */
    join talladega_ranking tr on tr.dt = pasr.dt::date
        and tr.mapped_manufacturing_location = fps.manufacturing_location
        and tr.mapped_manufacturing_location_rank = 1
where pasr.mapped_manufacturing_location in ('Talladega, AL (TMS)')