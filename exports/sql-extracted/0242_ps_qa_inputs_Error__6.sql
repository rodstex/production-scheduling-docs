-- Job: ps_qa_inputs
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: Error #6
-- Type: SQL Query
-- Job ID: 4320593
-- Component ID: 4322674

select
    qsl.mapped_manufacturing_location || ' manufacturing location does not have SKU capacities for line type ' || qsl.line_type as error_type
from ${stg2_schema}.ps_line_count qsl
    left join ${stg2_schema}.ps_capacity_by_sku ps on ps.mapped_manufacturing_location = qsl.mapped_manufacturing_location
        and ps.line_type = qsl.line_type
group by qsl.mapped_manufacturing_location, qsl.line_type
having count(distinct ps.sku_without_merv_rating) = 0