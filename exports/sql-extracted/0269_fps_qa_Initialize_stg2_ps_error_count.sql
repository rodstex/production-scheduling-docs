-- Job: fps_qa
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Initialize stg2.ps_error_count 00
-- Type: SQL Query
-- Job ID: 4496732
-- Component ID: 4517266

select
    is_critical_error
    ,count(*) as row_count
from ${stg2_schema}.ps_fps_errors
where is_critical_error
group by 1