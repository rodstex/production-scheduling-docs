-- Job: fps_automated_staffing_reassignment
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: Initialize stg2.ps_is_set_production_lines_dt_flg
-- Type: SQL Query
-- Job ID: 4826146
-- Component ID: 4839162

select
    true is_set_production_line_dt
from ${stg2_schema}.ps_settings_history
where is_selected_dt
    and day_of_week_int_line_capacity_set = date_part('dow', current_timestamp at time zone '${timezone}')