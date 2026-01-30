-- Job: ps_reactive_production_loops_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4319798
-- Component ID: 4319840

with base_records as (
    select
        "loop order"::int                           as iteration
        ,"days of inventory less than"::int         as max_days_of_inventory
        ,"build inventory to n days"::int           as planned_manufacturing_days
        ,"exclude production need less than (for sku's demand)"::int   as min_production_need
    from ${stg1_schema}.ps_reactive_production_loops
    where "days of inventory less than"::int > 0
        and "build inventory to n days"::int > 0
        and "exclude production need less than (for sku's demand)"::int >= 0
)

select
    iteration
    ,max_days_of_inventory
    ,planned_manufacturing_days
    ,min_production_need
    ,row_number() over (partition by iteration order by max_days_of_inventory asc) as rn
from base_records