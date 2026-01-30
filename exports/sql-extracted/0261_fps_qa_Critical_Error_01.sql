-- Job: fps_qa
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Critical Error 01
-- Type: SQL Query
-- Job ID: 4496732
-- Component ID: 4500872

with base_records as (
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,fps.line_type
        ,lch.count_of_lines
        ,sum(fps.production_lines)
            + sum(coalesce(fps.changeover_production_lines, 0)) as production_and_changeover_lines
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_line_count_history lch on lch.is_selected_dt
            and lch.line_type = fps.line_type
            and lch.mapped_manufacturing_location = fps.manufacturing_location
    where fps.is_within_capacity
        and fps.line_type in ('Automated', 'Single Loader', 'Double Loader', 'Manual')
  		and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
    group by 1,2,3,4
)

select
    'Production and changeover lines > lines available by MFG location and line type'   as error_type
    ,true                                                                               as is_critical_error
    ,inserted_dt_utc::date                                                              as dt
    ,manufacturing_location
    ,count_of_lines::text || ' lines available on ' || line_type || ' lines. '
    || production_and_changeover_lines::text || ' production and changeover lines are scheduled.' as error_message
from base_records
where production_and_changeover_lines > (count_of_lines + 0.2) -- Adding buffer