-- Job: run_production_schedule_v4
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: Initialize stg2.ps_max_runtime
-- Type: SQL Query
-- Job ID: 4308379
-- Component ID: 9367071

select
    manufacturing_location
    ,max(runtime_dt_utc) as max_runtime_dt
from ${dwh_schema}.fact_production_schedule
where runtime_dt_utc::date = current_date
group by 1