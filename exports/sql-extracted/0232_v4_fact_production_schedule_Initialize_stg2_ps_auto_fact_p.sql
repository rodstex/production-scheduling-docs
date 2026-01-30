-- Job: v4 fact_production_schedule
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: Initialize stg2.ps_auto_fact_production_schedule_copy
-- Type: SQL Query
-- Job ID: 5026643
-- Component ID: 5026783

select
	*
from ${dwh_schema}.fact_production_schedule
where line_type = 'Automated'
	and (is_future_production_schedule or is_tomorrow_production_schedule)
    and manufacturing_location in (${manufacturing_locations})