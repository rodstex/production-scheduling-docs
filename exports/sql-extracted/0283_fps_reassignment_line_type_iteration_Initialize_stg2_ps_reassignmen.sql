-- Job: fps_reassignment_line_type_iteration
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: Initialize stg2.ps_reassignment_remaining_capacity
-- Type: SQL Query
-- Job ID: 4856308
-- Component ID: 4875136

select
    pasr.inserted_dt_utc
    ,pasr.manufacturing_location
    ,pasr.automated_production_lines_available_to_reassign
    ,sum(coalesce(fps.production_lines, 0))
        + sum(coalesce(fps.changeover_production_lines, 0)) as reassigned_production_changeover_lines
from
    (select distinct
        inserted_dt_utc
        ,manufacturing_location
        ,automated_production_lines_available_to_reassign
    from ${stg2_schema}.ps_reassignment_staging) pasr
    left join ${dwh_schema}.fact_production_schedule fps on fps.inserted_dt_utc = pasr.inserted_dt_utc
        and fps.manufacturing_location = pasr.manufacturing_location
        and fps.is_within_capacity
        and fps.reassigned_automated_production_lines_over_dt_manufacturing_location is not null
group by 1,2,3
having pasr.automated_production_lines_available_to_reassign > reassigned_production_changeover_lines