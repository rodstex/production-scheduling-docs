-- Job: ps_qa_inputs
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: Error #1
-- Type: SQL Query
-- Job ID: 4320593
-- Component ID: 4321742

with base_distribution_centers as (
    select 'New Kensington, PA' as mapped_distribution_location union all
    select 'Ogden, UT' union all
    select 'Fresno, CA' union all
    select 'Talladega, AL (Pope)' union all
    select 'Talladega, AL (Newberry)' union all
    select 'Dallas, TX' union all
    select 'Orlando, FL' union all
    select 'Elgin, IL'
)

select
    bdc.mapped_distribution_location || ' distribution center not mapped to any manufacturing locations' as error_type
from base_distribution_centers bdc
    left join ${stg2_schema}.ps_manufacturing_to_distribution_center ps on ps.mapped_distribution_location = bdc.mapped_distribution_location
group by 1
having count(distinct ps.mapped_manufacturing_location) = 0