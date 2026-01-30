-- Job: fps_non_automated_line_type_iteration
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: Copy stg2.ps_non_automated_production_schedule_staging 01
-- Type: SQL Query
-- Job ID: 4429390
-- Component ID: 4436821

select *
from ${stg2_schema}.ps_non_automated_production_schedule_staging
where not(is_copied_fps)