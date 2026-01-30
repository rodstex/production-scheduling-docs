-- Job: fps_qa
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Critical Error 06
-- Type: SQL Query
-- Job ID: 4496732
-- Component ID: 4512547

with base_records as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,distribution_location
        ,line_type
        ,rank_over_manufacturing_location_line_type
        ,case when min_merv8 < 0 then min_merv8
            when min_merv11 < 0 then min_merv11
            when min_merv13 < 0 then min_merv13
            when min_merv11 < 0 then min_merv11
            when min_oe < 0 then min_oe end as min_production_goal
        ,case when min_lines < 0 then min_lines end as min_production_lines
    from
        (select
            fps.inserted_dt_utc
            ,fps.manufacturing_location
            ,fps.distribution_location
            ,fps.line_type
            ,fps.rank_over_manufacturing_location_line_type
            ,min(production_goal_merv_8) as min_merv8
            ,min(production_goal_merv_11) as min_merv11
            ,min(production_goal_merv_13) as min_merv13
            ,min(production_goal_merv_8_odor_eliminator) as min_oe
            ,min(production_lines) as min_lines
        from ${dwh_schema}.fact_production_schedule fps
        where fps.line_type in ('Automated', 'Single Loader', 'Double Loader', 'Manual')
  			and fps.inserted_dt_utc::date >= (current_timestamp at time zone 'America/New_York')::date
        group by 1,2,3,4,5) t
)

select
    'Minimum production goal is less than zero.'    as error_type
    ,true                               			as is_critical_error
    ,inserted_dt_utc::date              			as dt
    ,manufacturing_location
    ,distribution_location || ' DC and ' || line_type || ', and ' || rank_over_manufacturing_location_line_type::text
    || ' rank has a minimum production goal of ' || min_production_goal::text as error_message
from base_records
where min_production_goal is not null
union all

select
    'Minimum production lines is less than zero.'   as error_type
    ,true                               			as is_critical_error
    ,inserted_dt_utc::date              			as dt
    ,manufacturing_location
    ,distribution_location || ' DC and ' || line_type || ', and ' || rank_over_manufacturing_location_line_type::text
    || ' rank has a minimum production lines of ' || min_production_goal::text as error_message
from base_records
where min_production_lines is not null