-- Job: fps_dt_iteration
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: Initialize stg2.ps_control 01
-- Type: SQL Query
-- Job ID: 4349408
-- Component ID: 5186493

select distinct
    inserted_dt_utc::date
from ${stg2_schema}.ps_fact_manufacturing_need_future
where is_selected_dt /* Date is date in question */
    and inserted_dt_utc::date in
        ((date_trunc('week', current_timestamp at time zone '${timezone}') + interval '5 day')::date /* This Saturday */
        ,(date_trunc('week', current_timestamp at time zone '${timezone}') + interval '6 day')::date /* This Sunday */
        ,date_trunc('week', current_timestamp at time zone '${timezone}' + interval '7 day')::date /* Next Monday */
        )
    and date_part(dow, current_timestamp at time zone 'America/Chicago') in (6,0) /* Current date is Saturday or Sunday */