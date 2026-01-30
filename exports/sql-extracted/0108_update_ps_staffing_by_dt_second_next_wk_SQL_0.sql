-- Job: update_ps_staffing_by_dt_second_next_wk
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 5034651
-- Component ID: 5034644

/* Calculate the number of production and changeover lines used at each:
 *      (1) Date
 *      (2) MFG location
 *      (3) Grouped line type
 */
with base_records as (
    select
        dd.dt
        ,dd.day_of_week_name
        ,fps.manufacturing_location
        ,case when fps.line_type in ('Single Loader', 'Double Loader', 'Manual') then 'Non-Automated'
            when fps.line_type in ('Automated') then 'Automated' end as grouped_line_type
        ,sum(fps.production_lines + coalesce(fps.changeover_production_lines, 0)) as production_lines
    from ${dwh_schema}.dim_date dd
        join ${dwh_schema}.fact_production_schedule fps on fps.inserted_dt_utc::date = dd.dt
            and fps.is_within_capacity
            and fps.line_type in ('Double Loader', 'Single Loader', 'Manual')
            and date_trunc('week', fps.inserted_dt_utc)::date = (date_trunc('week', current_timestamp at time zone '${timezone}') + interval '14 day')::date
  	where manufacturing_location in (${manufacturing_locations})
    group by 1,2,3,4
)

/* Calculate the average number of production and changeover lines used at each:
 *      (1) Week
 *      (2) Shift
 *      (3) Staffing available
 *      (4) Manufacturing location
 *      (5) Grouped line type
 * Grouping by #3 ensures that a shift with inconsistent staffing levels will have the correct production capacity generated.
 * E.g., locations frequently have higher production capacity on Friday's due to overtime availability.
 */
, transformed as (
    select
        date_trunc('week', br.dt)::date as week_dt
        ,case when br.day_of_week_name in ('Monday', 'Tuesday', 'Wednesday', 'Thursday') then 'Mon-Thu'
            when br.day_of_week_name in ('Friday', 'Saturday', 'Sunday') then 'Fri-Sun' end as shift
        ,br.manufacturing_location
        ,br.grouped_line_type
        ,sbd.count_of_lines as staffing_available
        ,sum(br.production_lines) as scheduled_production_lines
        ,count(distinct br.dt) as count_of_dts
        ,sum(br.production_lines)::float
            / count(distinct br.dt)::float as avg_production_lines
        ,case when round(avg_production_lines) > sbd.count_of_lines then sbd.count_of_lines
            else round(avg_production_lines) end as staffing_available_calc
        ,min(br.dt) as start_dt
        ,max(br.dt) as end_dt
    from base_records br
        join ${stg2_schema}.ps_staff_by_day_history sbd on sbd.is_selected_dt
            and sbd.day_of_week_int = date_part(dow, br.dt)
            and sbd.grouped_line_type = br.grouped_line_type
            and sbd.mapped_manufacturing_location = br.manufacturing_location
    group by 1,2,3,4,5
)

select
    dd.dt
    ,t.manufacturing_location as mapped_manufacturing_location
    ,t.grouped_line_type
    ,t.staffing_available_calc as staffing_available
from transformed t
    join ${dwh_schema}.dim_date dd on dd.dt between t.start_dt and t.end_dt
order by 1