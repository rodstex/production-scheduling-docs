-- Job: run_production_schedule_v4
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: COPY ps_staffing_by_dt 00
-- Type: SQL Query
-- Job ID: 4308379
-- Component ID: 5124342

select
    '${orchestration_start_dt}'::timestamptz as copied_dt_utc
    ,dt
    ,mapped_manufacturing_location
    ,grouped_line_type
    ,staffing_available
from ${stg2_schema}.ps_staffing_by_dt