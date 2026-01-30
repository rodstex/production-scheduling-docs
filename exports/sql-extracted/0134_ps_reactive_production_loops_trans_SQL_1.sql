-- Job: ps_reactive_production_loops_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 1
-- Type: SQL Query
-- Job ID: 4319798
-- Component ID: 4401233

select
    iteration
    ,coalesce(lag(max_days_of_inventory) over (order by iteration asc), 0) as min_days_of_inventory
    ,max_days_of_inventory
    ,planned_manufacturing_days
    ,min_production_need
from ($T{Filter 0})