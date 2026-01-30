-- Job: fps_qa
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Critical Error 00
-- Type: SQL Query
-- Job ID: 4496732
-- Component ID: 4496801

with base_records as (
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,case when fps.line_type in ('Automated') then 'Automated'
            else 'Non-Automated' end                                as grouped_line_type
        ,ps.staffing_available  as staffing_available
        ,sum(fps.production_lines)
            + sum(coalesce(fps.changeover_production_lines, 0))     as production_and_changeover_lines
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_staffing_by_dt ps on ps.dt = fps.inserted_dt_utc::date
            and ps.mapped_manufacturing_location = fps.manufacturing_location
            and case when ps.grouped_line_type = 'Non-Automated' and fps.line_type in ('Single Loader', 'Double Loader', 'Manual') then true
                when ps.grouped_line_type = fps.line_type then true
                else false end
    where fps.is_within_capacity
        and fps.line_type in ('Automated', 'Single Loader', 'Double Loader', 'Manual')
  		and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
    group by 1,2,3,4
)

select
    'Production and changeover lines > staffing available by MFG location'  as error_type
    ,true                                                                   as is_critical_error
    ,inserted_dt_utc::date                                                  as dt
    ,manufacturing_location
    ,staffing_available::text || ' staffing available. '
    || production_and_changeover_lines::text || ' production and changeover lines are scheduled.' as error_message
from base_records
where production_and_changeover_lines > (staffing_available + 0.2) -- Adding buffer capacity