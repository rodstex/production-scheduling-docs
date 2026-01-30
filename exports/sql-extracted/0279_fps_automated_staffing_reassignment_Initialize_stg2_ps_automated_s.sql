-- Job: fps_automated_staffing_reassignment
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: Initialize stg2.ps_automated_staffing_reassignment_control 00
-- Type: SQL Query
-- Job ID: 4826146
-- Component ID: 5092703

select distinct
    pasr.dt
    ,pasr.target_mapped_manufacturing_location
    ,pasr.adjustment
from ${stg2_schema}.ps_automated_staffing_reassignment pasr
    join ${dwh_schema}.fact_production_schedule fps on fps.inserted_dt_utc::date = pasr.dt
        and fps.manufacturing_location = pasr.target_mapped_manufacturing_location
        and fps.line_type in ('Non-Automated')
        and fps.is_within_capacity