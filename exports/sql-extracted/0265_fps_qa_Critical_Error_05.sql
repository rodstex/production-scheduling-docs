-- Job: fps_qa
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Critical Error 05
-- Type: SQL Query
-- Job ID: 4496732
-- Component ID: 4512139

with base_records as (
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,fps.line_type
        ,fps.rank_over_manufacturing_location_line_type
        ,count(distinct sku_without_merv_rating) as row_count
    from ${dwh_schema}.fact_production_schedule fps
    where fps.line_type in ('Automated', 'Single Loader', 'Double Loader', 'Manual')
  		and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
    group by 1,2,3,4
    having count(distinct sku_without_merv_rating) > 1
)

select
    'Manufacturing location, line type, and rank has more than one distinct size.'    as error_type
    ,true                               as is_critical_error
    ,inserted_dt_utc::date              as dt
    ,manufacturing_location
    ,line_type || ', and ' || rank_over_manufacturing_location_line_type::text
    || ' rank has ' || row_count::text || ' distinct SKU without MERV ratings.' as error_message
from base_records