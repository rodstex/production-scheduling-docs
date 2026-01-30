-- Job: run_production_schedule_v4
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: Initialize stg2.ps_fact_manufacturing_need_max_dt
-- Type: SQL Query
-- Job ID: 4308379
-- Component ID: 4311363

select
    max(inserted_dt_utc) as max_inserted_dt
from ${dwh_schema}.fact_manufacturing_need
where location_name in (${locations_list})
    and (inserted_dt_utc at time zone '${timezone}')::date >= (current_timestamp at time zone '${timezone}')::date
having max(inserted_dt_utc) is not null