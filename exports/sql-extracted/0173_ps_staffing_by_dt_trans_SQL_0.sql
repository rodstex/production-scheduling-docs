-- Job: ps_staffing_by_dt_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4880348
-- Component ID: 4880355

select
    ps.dt
    ,ps.mapped_manufacturing_location
    ,ps.grouped_line_type
    ,(ps.staffing_available + fps.reassigned_automated_production_lines_over_dt_manufacturing_location)::int as staffing_available
from
    (select distinct
        inserted_dt_utc::date as dt
        ,manufacturing_location
        ,reassigned_automated_production_lines_over_dt_manufacturing_location
    from ${dwh_schema}.fact_production_schedule
    where reassigned_automated_production_lines_over_dt_manufacturing_location is not null) fps
    join ${stg2_schema}.ps_staffing_by_dt ps on ps.dt = fps.dt
        and ps.mapped_manufacturing_location = fps.manufacturing_location
        and ps.grouped_line_type = 'Non-Automated'