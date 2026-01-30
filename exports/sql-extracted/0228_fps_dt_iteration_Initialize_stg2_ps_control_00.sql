-- Job: fps_dt_iteration
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: Initialize stg2.ps_control 00
-- Type: SQL Query
-- Job ID: 4349408
-- Component ID: 5865840

select distinct
    inserted_dt_utc::date
from ${stg2_schema}.ps_fact_manufacturing_need_future
where is_selected_dt /* Date is date in question */