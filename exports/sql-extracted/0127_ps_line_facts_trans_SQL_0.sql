-- Job: ps_line_facts_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4317850
-- Component ID: 4317894

with base_records as (
    select
        case when trim(lower("line type")) = 'automated'    then 'Automated'
            when trim(lower("line type")) = 'non-automated' then 'Non-Automated'
            when trim(lower("line type")) = 'single loader' then 'Single Loader'
            when trim(lower("line type")) = 'double loader' then 'Double Loader'
            when trim(lower("line type")) = 'manual'        then 'Manual' end                               as line_type
        ,case when "minimum production runtime hours (per size)" = '#N/A' then null::decimal(10,2)
            else "minimum production runtime hours (per size)"::decimal(10,2) end                           as min_run_hrs_per_size
        ,case when "maximum production lines (per size)" = '#N/A' then null::decimal(10,2)
            else "maximum production lines (per size)"::decimal(10,2) end                                   as max_prod_lines_per_size
        ,case when "minimum production runtime hours (per merv rating)" = '#N/A' then null::decimal(10,2)
            else "minimum production runtime hours (per merv rating)"::decimal(10,2) end                    as min_run_hrs_per_merv_rating
        ,case when "changeover hours (per size)" = '#N/A' then null::decimal(10,2)
            else "changeover hours (per size)"::decimal(10,2) end                                           as changeover_hrs_per_size
    from ${stg1_schema}.ps_line_facts
    where trim(lower("line type")) in ('automated','non-automated','double loader','single loader','manual')
)

select
    line_type
    ,min_run_hrs_per_size
    ,max_prod_lines_per_size
    ,min_run_hrs_per_merv_rating
    ,changeover_hrs_per_size
    ,row_number() over (partition by line_type order by min_run_hrs_per_size asc) as rn
from base_records
where (min_run_hrs_per_size is null
    or min_run_hrs_per_size >= min_run_hrs_per_merv_rating)