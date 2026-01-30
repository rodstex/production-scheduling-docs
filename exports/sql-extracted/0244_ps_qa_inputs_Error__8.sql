-- Job: ps_qa_inputs
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: Error #8
-- Type: SQL Query
-- Job ID: 4320593
-- Component ID: 4322907

with base_line_types as (
    select 'Automated' as line_type union all
    select 'Single Loader' union all
    select 'Double Loader' union all
    select 'Manual'
)

select
    blt.line_type || ' line type does not have line facts' as error_type
from base_line_types blt
    left join ${stg2_schema}.ps_line_facts plf on plf.line_type = blt.line_type
group by 1
having count(distinct plf.line_type) = 0