-- Job: ps_qa_inputs
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: Error #5
-- Type: SQL Query
-- Job ID: 4320593
-- Component ID: 4322430

select
    qsl.mapped_manufacturing_location || ' manufacturing location does not have MERV ratings for automated lines' as error_type
from ${stg2_schema}.ps_line_count qsl
    left join ${stg2_schema}.ps_automated_merv_ratings amr on amr.mapped_manufacturing_location = qsl.mapped_manufacturing_location
where qsl.line_type = 'Automated'
group by 1
having count(distinct amr.merv_rating) = 0