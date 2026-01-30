-- Job: ps_filter_types_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4341912
-- Component ID: 4341965

select distinct
    case when trim(lower("filter type")) = '1" pleated' then '1" Pleated'
        when trim(lower("filter type")) = '2" pleated' then '2" Pleated'
        when trim(lower("filter type")) = '4" pleated' then '4" Pleated'
        when trim(lower("filter type")) = '6" rigid cell' then '6" Rigid Cell'
        when trim(lower("filter type")) = '12" rigid cell' then '12" Rigid Cell'
        when trim(lower("filter type")) = 'whole-house' then 'Whole-House' end as filter_type
from ${stg1_schema}.ps_filter_types
where trim(lower("filter type")) in ('1" pleated', '2" pleated', '4" pleated', 'whole-house', '12" rigid cell', '6" rigid cell')