-- Job: fps_non_automated_reactive_iteration
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: Initialize stg2.ps_non_automated_max_ranking
-- Type: SQL Query
-- Job ID: 4398556
-- Component ID: 4400472

select
    mapped_manufacturing_location
    ,max(ranking) as max_rank
from ${stg2_schema}.ps_non_automated_ranking
where not(is_unscheduled_production)
group by 1