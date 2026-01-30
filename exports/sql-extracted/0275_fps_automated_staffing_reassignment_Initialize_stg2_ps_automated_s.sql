-- Job: fps_automated_staffing_reassignment
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: Initialize stg2.ps_automated_staffing_reassignment
-- Type: SQL Query
-- Job ID: 4826146
-- Component ID: 4826380

/* Return the number of automated lines scheduled (if any) for every date in the current until the end of scheduled products */
with base_records as (
    select
        date_trunc('week', dd.dt)::date as week_dt
        ,case when dd.day_of_week_name in ('Monday', 'Tuesday', 'Wednesday', 'Thursday') then 'Mon-Thu'
            when dd.day_of_week_name in ('Friday', 'Saturday', 'Sunday') then 'Fri-Sun' end as shift
        ,dd.dt
        ,case when dd.dt >= (current_timestamp at time zone 'America/Chicago' + interval '1 day')::date then true
            else false end as is_future_dt
        ,lch.mapped_manufacturing_location
        ,round(sum(coalesce(fps.production_lines, 0))
            + sum(coalesce(fps.changeover_production_lines, 0)), 0) as total_auto_lines
    from ${dwh_schema}.dim_date dd
        join ${stg2_schema}.ps_line_count_history lch on lch.is_selected_dt
            and lch.line_type = 'Automated' /* Truncate to automated lines */
            and lch.mapped_manufacturing_location in (${manufacturing_locations})
        left join ${dwh_schema}.fact_production_schedule fps on fps.inserted_dt_utc::date = dd.dt
            and fps.is_within_capacity
            and fps.line_type = lch.line_type
            and fps.manufacturing_location = lch.mapped_manufacturing_location
        join (select max(inserted_dt_utc)::date as max_dt from ${dwh_schema}.fact_production_schedule) md on true
    where date_trunc('week', dd.dt)::date >= date_trunc('week', current_timestamp at time zone '${timezone}')::date
        and date_trunc('week', dd.dt)::date <= date_trunc('week', md.max_dt)::date
    group by 1,2,3,4,5
)

/* For each manufacturing location, week, shift, and (if applicable) staff available, calculate the maximum number of lines.
 * E.g., Mon = 2 lines, Tue = 3 lines, Wed = 3 lines. Max lines = 3.
 * If the (1) date is after tomorrow and (2) the number of scheduled lines is less than the maximum, identify the delta.
 */
, adjustment as (
    select
        br.week_dt
        ,br.shift
        ,sbd.count_of_lines as staff_available
        ,br.dt
        ,br.is_future_dt
        ,br.mapped_manufacturing_location
        ,br.total_auto_lines
        ,max(br.total_auto_lines) over (partition by br.mapped_manufacturing_location, br.week_dt, br.shift, sbd.count_of_lines)    as max_total_lines_over_week_shift_mfg
        ,case when br.is_future_dt then max_total_lines_over_week_shift_mfg - total_auto_lines end                                  as adjustment
        ,case when br.is_future_dt and adjustment != 0 then true else false end                                                     as is_adjustment_needed
    from base_records br
        join ${stg2_schema}.ps_staff_by_day_history sbd on sbd.is_selected_dt
            and sbd.day_of_week_int = date_part(dow, br.dt)
            and sbd.grouped_line_type = 'Automated'
            and sbd.mapped_manufacturing_location = br.mapped_manufacturing_location
)

, adjustment_needed as (
    select
        dt::date as dt
        ,mapped_manufacturing_location
        ,case when mapped_manufacturing_location = 'Talladega, AL (TMS)' then null
            else mapped_manufacturing_location end as target_mapped_manufacturing_location
        ,adjustment
    from adjustment
    where is_adjustment_needed
         and is_future_dt
)

, talladega_assignment as (
    select
        dt
        ,mapped_manufacturing_location as target_manufacturing_location
        ,row_number() over (partition by dt order by lines_available desc) as rn
    from
        (select
            t.dt
            ,t.mapped_manufacturing_location
            ,t.existing_lines
            ,sum(coalesce(fps.production_lines, 0)) + sum(coalesce(fps.changeover_production_lines, 0)) as lines_scheduled
            ,t.existing_lines - lines_scheduled as lines_available
        from
            (select
                br.dt
                ,lc.mapped_manufacturing_location
                ,sum(lc.count_of_lines) as existing_lines
            from adjustment_needed br
                join ${stg2_schema}.ps_line_count lc on lc.mapped_manufacturing_location like 'Talladega%'
                    and lc.line_type not like 'Automated'
            where br.mapped_manufacturing_location = 'Talladega, AL (TMS)'
            group by 1,2) t
            left join ${dwh_schema}.fact_production_schedule fps on fps.is_within_capacity
                and fps.inserted_dt_utc::date = t.dt
                and fps.manufacturing_location = t.mapped_manufacturing_location
                and fps.line_type in ('Single Loader', 'Double Loader', 'Manual', 'Non-Automated')
        group by 1,2,3) t
)

select
    an.dt
    ,an.mapped_manufacturing_location
    ,coalesce(an.target_mapped_manufacturing_location, ta.target_manufacturing_location) as target_mapped_manufacturing_location
    ,an.adjustment
from adjustment_needed an
    left join talladega_assignment ta on an.mapped_manufacturing_location = 'Talladega, AL (TMS)'
        and ta.dt = an.dt
        and ta.rn = 1
