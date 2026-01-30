-- Job: fps_non_automated_staffing_by_dt
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4687563
-- Component ID: 4688133

select
    ps.dt                               as inserted_dt_utc
    ,ps.mapped_manufacturing_location   as manufacturing_location
    ,'Non-Automated'                    as line_type
    ,'Placeholder'                      as sku_without_merv_rating
    ,ps.staffing_available              as lines_available_over_manufacturing_location_line_type
    ,ps.staffing_available              as production_lines
    ,true                               as is_within_capacity
    ,false                              as is_current_production_schedule
    ,false                              as is_tomorrow_production_schedule
    ,true                               as is_future_production_schedule
    ,fmn.runtime_dt_utc
from ${stg2_schema}.ps_staffing_by_dt ps
    join (select distinct runtime_dt_utc from ${stg2_schema}.ps_fact_manufacturing_need_future where is_selected_dt) fmn on true
where ps.grouped_line_type = 'Non-Automated'
    and ps.mapped_manufacturing_location in (${manufacturing_locations})
    and date_trunc('week', ps.dt)::date > (date_trunc('week', current_timestamp at time zone '${timezone}'))::date