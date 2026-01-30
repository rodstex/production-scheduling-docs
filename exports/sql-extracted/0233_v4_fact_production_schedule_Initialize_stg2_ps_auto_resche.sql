-- Job: v4 fact_production_schedule
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: Initialize stg2.ps_auto_reschedule_iterations
-- Type: SQL Query
-- Job ID: 5026643
-- Component ID: 5033349

select
    week_dt
    ,row_number() over (order by week_dt asc) as rn
from
    (select distinct
        date_trunc('week', inserted_dt_utc)::date as week_dt
    from ${stg2_schema}.ps_automated_production_schedule_staging) t