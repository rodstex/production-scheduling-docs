-- Job: fps_automated_staffing_reassignment
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: Initialize stg2.ps_reassignment_staging 01
-- Type: SQL Query
-- Job ID: 4826146
-- Component ID: 4840532

/* Return all lines scheduled and within capacity.
 * Truncate to weeks where the line type is not is abstracted to 'Non-Automated'.
 */
with scheduled_lines as (
    select
        fps.inserted_dt_utc::date                               as dt
        ,fps.manufacturing_location                             as mapped_manufacturing_location
        ,fps.line_type
        ,sum(fps.production_lines)
            + sum(coalesce(fps.changeover_production_lines, 0)) as production_and_changeover_lines
    from ${dwh_schema}.fact_production_schedule fps
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and fps.is_within_capacity
        and (fps.is_tomorrow_production_schedule or fps.is_future_production_schedule)
    group by 1,2,3
)

/* Calculate the number of non-automated lines available by date and manufacturing location.
 * Filter to records where lines remain available.
 */
, available_lines as (
    select
        dt
        ,mapped_manufacturing_location
        ,line_type
        ,lines_available
        ,sum(lines_available) over (partition by dt, mapped_manufacturing_location) as total_lines_available
    from
        (select
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
        having lines_available > 0) t
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

------------------------------------
-- New Kensington, PA & Ogden, UT --
------------------------------------
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
    /* Return the automated staff to reassign or the total number of available lines, whichever is fewer. */
    ,case when pasr.adjustment <= al.total_lines_available then pasr.adjustment
        else al.total_lines_available end as automated_production_lines_available_to_reassign
from ${stg2_schema}.ps_automated_staffing_reassignment pasr
    join ${dwh_schema}.fact_production_schedule fps on fps.inserted_dt_utc::date = pasr.dt::date  /* Truncate to date(s) where adjustments need to occur */
        and fps.manufacturing_location = pasr.mapped_manufacturing_location                 /* Truncate to location(s) where adjustments need to occur */
        and fps.line_type in ('Single Loader', 'Double Loader', 'Manual')                   /* Truncate to abstracted non-automated line */
        and not(fps.is_within_capacity)                                                     /* Truncate to products not within capacity */
    /* Join to return the number of lines available */
    join available_lines al on al.dt = pasr.dt::date
        and al.mapped_manufacturing_location = fps.manufacturing_location
        and al.line_type = fps.line_type
    left join
        (select distinct date_trunc('week', inserted_dt_utc)::date week_dt
        from ${dwh_schema}.fact_production_schedule
        where line_type in ('Non-Automated')) esc on esc.week_dt = date_trunc('week', pasr.dt)::date
where esc.week_dt is null                                                   /* Exclude weeks where the abstract 'Non-Automated' line type is used */
    and pasr.mapped_manufacturing_location not in ('Talladega, AL (TMS)')   /* Only New Kensington, PA & Ogden, UT */
union all

-------------------
-- Talladega, AL --
-------------------
select distinct
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
    /* Return the automated staff to reassign or the total number of available lines, whichever is fewer. */
    ,case when pasr.adjustment <= tr.total_available_lines then pasr.adjustment
        else tr.total_available_lines end as automated_production_lines_available_to_reassign
from ${stg2_schema}.ps_automated_staffing_reassignment pasr
    join ${dwh_schema}.fact_production_schedule fps on fps.inserted_dt_utc::date = pasr.dt::date  /* Truncate to date(s) where adjustments need to occur */
        and fps.manufacturing_location like 'Talladega%'                                    /* Truncate to location(s) where adjustments need to occur */
        and fps.line_type in ('Single Loader', 'Double Loader', 'Manual')                   /* Truncate to abstracted non-automated line */
        and not(fps.is_within_capacity)                                                     /* Truncate to products not within capacity */
    /* Join to return the number of lines available */
    join talladega_ranking tr on tr.dt = pasr.dt::date
        and tr.mapped_manufacturing_location = fps.manufacturing_location
        and tr.mapped_manufacturing_location_rank = 1
    left join
        (select distinct date_trunc('week', inserted_dt_utc)::date week_dt
        from ${dwh_schema}.fact_production_schedule
        where line_type in ('Non-Automated')) esc on esc.week_dt = date_trunc('week', pasr.dt)::date
where esc.week_dt is null                                               /* Exclude weeks where the abstract 'Non-Automated' line type is used */
    and pasr.mapped_manufacturing_location in ('Talladega, AL (TMS)')   /* Only Talladega, AL */