-- Job: fps_qa
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Critical Error 07
-- Type: SQL Query
-- Job ID: 4496732
-- Component ID: 5448190

with base_records as (
    select
        fps.inserted_dt_utc::date as dt
        ,fps.manufacturing_location
        ,fps.line_type
        ,fps.rank_over_manufacturing_location_line_type
        ,fps.sku_without_merv_rating
        ,fps.non_automated_efficiency_rank
        ,fps.non_automated_logic_type
        ,round(lfh.changeover_hrs_per_size::float / 10::float, 2) as changeover_production_lines_per_size
        ,sum(fps.changeover_production_lines) as changeover_production_lines
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_line_facts_history lfh on lfh.inserted_dt_utc = fps.runtime_dt_utc
            and fps.line_type = lfh.line_type
    where true
        and fps.inserted_dt_utc::date >= '2025-04-11'
        and fps.line_type in ('Single Loader', 'Double Loader','Manual')
    group by 1,2,3,4,5,6,7,8
)

, error_detection as (
    select
        dt
        ,manufacturing_location
        ,line_type
        ,rank_over_manufacturing_location_line_type
        ,sku_without_merv_rating
        ,lag(sku_without_merv_rating) over (partition by dt, manufacturing_location, line_type order by rank_over_manufacturing_location_line_type asc) as prior_sku_without_merv_rating
        ,changeover_production_lines
        ,non_automated_efficiency_rank
        ,non_automated_logic_type
        ,case
            when changeover_production_lines_per_size = 0 then 0
            when prior_sku_without_merv_rating is null and changeover_production_lines = 0 then 0
            when prior_sku_without_merv_rating is null and changeover_production_lines != 0 then 1
            when sku_without_merv_rating = prior_sku_without_merv_rating and changeover_production_lines = 0 then 0
            when sku_without_merv_rating = prior_sku_without_merv_rating and changeover_production_lines != 0 then 1
            when sku_without_merv_rating != prior_sku_without_merv_rating and changeover_production_lines = 0 then 1
            when sku_without_merv_rating != prior_sku_without_merv_rating and changeover_production_lines != 0 then 0
        end as is_error
        ,case
            when changeover_production_lines_per_size = 0 then null
            when prior_sku_without_merv_rating is null and changeover_production_lines = 0 then null
            when prior_sku_without_merv_rating is null and changeover_production_lines != 0 then 'Changeover when first scheduled size'
            when sku_without_merv_rating = prior_sku_without_merv_rating and changeover_production_lines = 0 then null
            when sku_without_merv_rating = prior_sku_without_merv_rating and changeover_production_lines != 0 then 'Changeover when prior size is the current size'
            when sku_without_merv_rating != prior_sku_without_merv_rating and changeover_production_lines = 0 then 'No changeover when prior size is not the current size'
            when sku_without_merv_rating != prior_sku_without_merv_rating and changeover_production_lines != 0 then null
        end as error_description
    from base_records
)

select
    error_description as error_type
    ,true as is_critical_error
    ,dt
    ,manufacturing_location
    ,line_type || ' has incorrect changeover logic' as error_message
from error_detection
where is_error = 1