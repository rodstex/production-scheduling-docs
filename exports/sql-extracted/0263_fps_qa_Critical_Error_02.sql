-- Job: fps_qa
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Critical Error 02
-- Type: SQL Query
-- Job ID: 4496732
-- Component ID: 4500933

with base_records as (
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,lfh.line_type as grouped_line_type
        ,fps.sku_without_merv_rating
        ,lfh.max_prod_lines_per_size
        ,sum(fps.production_lines)
            + sum(coalesce(fps.changeover_production_lines, 0)) as production_and_changeover_lines
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_line_facts_history lfh on lfh.is_selected_dt
            and case when fps.line_type = 'Automated' and lfh.line_type = 'Automated' then true
                when fps.line_type in ('Single Loader', 'Double Loader', 'Manual') and lfh.line_type = 'Non-Automated' then true
                else false end
    where fps.is_within_capacity
        and fps.line_type in ('Automated', 'Single Loader', 'Double Loader', 'Manual')
  		and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
    group by 1,2,3,4,5
)

select
    'Production lines > allowed lines for a SKU without MERV rating on a grouped line type' as error_type
    ,true                                                                                   as is_critical_error
    ,inserted_dt_utc                                                                        as dt
    ,manufacturing_location
    ,sku_without_merv_rating || ' has ' || production_and_changeover_lines::text || ' lines scheduled on ' || grouped_line_type || ' lines. '
    || 'The maximum lines allowed are ' || max_prod_lines_per_size::text as error_message
from base_records
where production_and_changeover_lines > (max_prod_lines_per_size + 0.2) /* Adding buffer */