-- Job: ps_qa_inputs
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: Error #7
-- Type: SQL Query
-- Job ID: 4320593
-- Component ID: 4322813

select
    qsl.mapped_manufacturing_location || ' manufacturing location does not have staff capacities for Automated line types' as error_type
from ${stg2_schema}.ps_line_count qsl
    left join ${stg2_schema}.ps_staff_by_day ps on ps.mapped_manufacturing_location = qsl.mapped_manufacturing_location
        and ps.grouped_line_type = 'Automated'
where qsl.line_type = 'Automated'
group by qsl.mapped_manufacturing_location, qsl.line_type
having max(ps.count_of_lines) = 0
union all
select
    qsl.mapped_manufacturing_location || ' manufacturing location does not have staff capacities for Non-Automated line types' as error_type
from ${stg2_schema}.ps_line_count qsl
    left join ${stg2_schema}.ps_staff_by_day ps on ps.mapped_manufacturing_location = qsl.mapped_manufacturing_location
        and ps.grouped_line_type = 'Non-Automated'
where qsl.line_type != 'Automated'
group by qsl.mapped_manufacturing_location, qsl.line_type
having max(ps.count_of_lines) = 0