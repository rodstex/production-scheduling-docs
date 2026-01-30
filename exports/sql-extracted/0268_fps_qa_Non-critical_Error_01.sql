-- Job: fps_qa
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Non-critical Error 01
-- Type: SQL Query
-- Job ID: 4496732
-- Component ID: 4516492

with base_records as (
    ------------
    -- MERV 8 --
    ------------
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,fps.line_type
        ,fps.sku_without_merv_rating || 'M8'                                            as sku
        ,fps.is_within_capacity
        ,round((cap.production_capacity::float / 10::float)
            * lfh.min_run_hrs_per_merv_rating::float)               as min_production_run_per_sku
        ,sum(fps.production_goal_merv_8)                                                as production
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_line_facts_history lfh on lfh.is_selected_dt
            and lfh.line_type = fps.line_type
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = fps.manufacturing_location
            and cap.line_type = fps.line_type
            and cap.sku_without_merv_rating = fps.sku_without_merv_rating
    where fps.line_type in ('Automated', 'Single Loader', 'Double Loader', 'Manual')
  		and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
    group by 1,2,3,4,5,6
    having production > 0
        and production < min_production_run_per_sku
    union all

    -------------
    -- MERV 11 --
    -------------
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,fps.line_type
        ,fps.sku_without_merv_rating || 'M11'                                           as sku
        ,fps.is_within_capacity
        ,round((cap.production_capacity::float / 10::float)
            * lfh.min_run_hrs_per_merv_rating::float)               as min_production_run_per_sku
        ,sum(fps.production_goal_merv_11)                                               as production
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_line_facts_history lfh on lfh.is_selected_dt
            and lfh.line_type = fps.line_type
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = fps.manufacturing_location
            and cap.line_type = fps.line_type
            and cap.sku_without_merv_rating = fps.sku_without_merv_rating
    where fps.line_type in ('Automated', 'Single Loader', 'Double Loader', 'Manual')
        and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
    group by 1,2,3,4,5,6
    having production > 0
        and production < min_production_run_per_sku
    union all

    -------------
    -- MERV 13 --
    -------------
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,fps.line_type
        ,fps.sku_without_merv_rating || 'M13'                                           as sku
        ,fps.is_within_capacity
        ,round((cap.production_capacity::float / 10::float)
            * lfh.min_run_hrs_per_merv_rating::float)               as min_production_run_per_sku
        ,sum(fps.production_goal_merv_13)                                               as production
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_line_facts_history lfh on lfh.is_selected_dt
            and lfh.line_type = fps.line_type
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = fps.manufacturing_location
            and cap.line_type = fps.line_type
            and cap.sku_without_merv_rating = fps.sku_without_merv_rating
    where fps.line_type in ('Automated', 'Single Loader', 'Double Loader', 'Manual')
  		and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
    group by 1,2,3,4,5,6
    having production > 0
        and production < min_production_run_per_sku
    union all

    ----------------------------
    -- MERV 8 Odor Eliminator --
    ----------------------------
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,fps.line_type
        ,fps.sku_without_merv_rating || 'OE'                                            as sku
        ,fps.is_within_capacity
        ,round((cap.production_capacity::float / 10::float)
            * lfh.min_run_hrs_per_merv_rating::float)               as min_production_run_per_sku
        ,sum(fps.production_goal_merv_8_odor_eliminator)                                as production
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_line_facts_history lfh on lfh.is_selected_dt
            and lfh.line_type = fps.line_type
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = fps.manufacturing_location
            and cap.line_type = fps.line_type
            and cap.sku_without_merv_rating = fps.sku_without_merv_rating
    where fps.line_type in ('Automated', 'Single Loader', 'Double Loader', 'Manual')
  		and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
    group by 1,2,3,4,5,6
    having production > 0
        and production < min_production_run_per_sku
)

select
    'Production goal for SKU is less than minimum production run'   as error_type
    ,false                                                          as is_critical_error
    ,inserted_dt_utc::date                                          as dt
    ,manufacturing_location
    ,sku || case when is_within_capacity then ', is within capacity, and '
                                    else ', not within capacity, and ' end
    || line_type || ' line has minimum production run of ' || min_production_run_per_sku::text || '. '
    || 'Production run scheduled is ' || production::text error_message
from base_records