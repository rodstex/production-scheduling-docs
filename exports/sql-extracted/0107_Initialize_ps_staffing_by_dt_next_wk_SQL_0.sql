-- Job: Initialize ps_staffing_by_dt_next_wk
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 5031290
-- Component ID: 5031291

select
    dd.dt
    ,sbd.mapped_manufacturing_location
    ,sbd.grouped_line_type
    ,sbd.count_of_lines as staffing_available
from ${dwh_schema}.dim_date dd
    left join ${stg2_schema}.ps_staff_by_day_history sbd on sbd.is_selected_dt
        and sbd.day_of_week_int = date_part(dow, dd.dt)
where date_trunc('week', dd.dt) = (date_trunc('week', current_timestamp at time zone '${timezone}') + interval '7 day')::date
    and sbd.mapped_manufacturing_location in (${manufacturing_locations})