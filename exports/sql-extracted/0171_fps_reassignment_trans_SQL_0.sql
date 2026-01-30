-- Job: fps_reassignment_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4862240
-- Component ID: 4864840

with base_records as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,distribution_location
        ,line_type
        ,sku_without_merv_rating
        ,rank_over_manufacturing_location_line_type                             as original_rank_over_manufacturing_location_line_type
        ,sum(case when sku like '%M8' then production_need_calc else 0 end)     as production_goal_merv_8
        ,sum(case when sku like '%M11' then production_need_calc else 0 end)    as production_goal_merv_11
        ,sum(case when sku like '%M13' then production_need_calc else 0 end)    as production_goal_merv_13
        ,sum(case when sku like '%OE' then production_need_calc else 0 end)     as production_goal_merv_8_odor_eliminator
        ,sum(production_need_calc)                                              as production_goal_total
        ,sum(production_lines_calc)::decimal(10,2)                              as production_lines
        ,sum(changeover_lines_calc)::decimal(10,2)                              as changeover_production_lines
    from ${stg2_schema}.ps_reassignment_within_capacity
    group by 1,2,3,4,5,6
)

, max_rank as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,line_type
        ,max(rank_over_manufacturing_location_line_type) as max_rank_over_manufacturing_location_line_type
    from ${dwh_schema}.fact_production_schedule
    where (is_tomorrow_production_schedule or is_future_production_schedule)
        and is_within_capacity
        and line_type in ('Single Loader', 'Double Loader', 'Manual')
    group by 1,2,3
)

select
    br.inserted_dt_utc
    ,br.manufacturing_location
    ,br.distribution_location
    ,br.line_type
    ,fps.lines_available_over_manufacturing_location_line_type
    ,row_number() over (partition by br.inserted_dt_utc, br.manufacturing_location, br.line_type
                        order by br.original_rank_over_manufacturing_location_line_type)
     + coalesce(mr.max_rank_over_manufacturing_location_line_type, 0) as rank_over_manufacturing_location_line_type
    ,br.sku_without_merv_rating
    ,fps.filters_per_pallet_over_sku_without_merv_rating
    ,br.production_goal_merv_8
    ,br.production_goal_merv_11
    ,br.production_goal_merv_13
    ,br.production_goal_merv_8_odor_eliminator
    ,br.production_goal_total
    ,br.production_lines
    ,fps.current_days_of_inventory_merv_8
    ,fps.current_days_of_inventory_merv_11
    ,fps.current_days_of_inventory_merv_13
    ,fps.current_days_of_inventory_merv_8_odor_eliminator
    ,fps.is_current_production_schedule
    ,fps.is_future_production_schedule
    ,true as is_within_capacity
    ,fps.is_tomorrow_production_schedule
    ,fps.runtime_dt_utc
    ,fps.non_automated_logic_type
    ,br.changeover_production_lines
    ,null::timestamptz as automated_original_inserted_dt_utc
    ,fps.non_automated_efficiency_rank
    ,pasr.automated_production_lines_available_to_reassign::decimal(10,2) as reassigned_automated_production_lines_over_dt_manufacturing_location
from base_records br
    left join max_rank mr on mr.inserted_dt_utc = br.inserted_dt_utc
        and mr.manufacturing_location = br.manufacturing_location
        and mr.line_type = br.line_type
    join ${stg2_schema}.ps_reassignment_staging fps on fps.inserted_dt_utc = br.inserted_dt_utc
        and fps.manufacturing_location = br.manufacturing_location
        and fps.distribution_location = br.distribution_location
        and fps.line_type = br.line_type
        and fps.rank_over_manufacturing_location_line_type = br.original_rank_over_manufacturing_location_line_type
    join 
        (select distinct
            inserted_dt_utc
            ,manufacturing_location
            ,automated_production_lines_available_to_reassign
        from ${stg2_schema}.ps_reassignment_staging) pasr on pasr.inserted_dt_utc = br.inserted_dt_utc
            and pasr.manufacturing_location = br.manufacturing_location