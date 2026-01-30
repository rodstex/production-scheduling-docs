-- Job: fps_qa
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Critical Error 03
-- Type: SQL Query
-- Job ID: 4496732
-- Component ID: 4500927

with base_records as (
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,fps.sku_without_merv_rating
        ,cap.auto_tooling_sets
        ,sum(fps.production_lines)
            + sum(coalesce(fps.changeover_production_lines, 0)) as production_and_changeover_lines
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = fps.manufacturing_location
            and cap.line_type = fps.line_type
  			and cap.sku_without_merv_rating = fps.sku_without_merv_rating
    where fps.is_within_capacity
        and fps.line_type in ('Automated')
  		and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
    group by 1,2,3,4
)

select
    'Production lines for SKU without MERV rating is > auto tooling sets available' as error_type
    ,true                                                                           as is_critical_error
    ,inserted_dt_utc                                                                as dt
    ,manufacturing_location
    ,sku_without_merv_rating || ' has ' || production_and_changeover_lines::text || ' lines scheduled on automated lines.'
    || 'The tooling for that location and SKU without MERV rating is ' || auto_tooling_sets::text as error_message
from base_records
where production_and_changeover_lines > (auto_tooling_sets + 0.2) -- adding buffer