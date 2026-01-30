-- Job: ps_fact_manufacturing_need_future
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/future_production_need_calc
-- Component: Copy stg2.ps_fact_manufacturing_need_future
-- Type: SQL Query
-- Job ID: 4355882
-- Component ID: 4358976

select
    *
from ${stg2_schema}.ps_fact_manufacturing_need_future
where is_selected_dt