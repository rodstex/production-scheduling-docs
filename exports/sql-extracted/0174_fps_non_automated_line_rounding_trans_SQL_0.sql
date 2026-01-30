-- Job: fps_non_automated_line_rounding_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_line_rounding
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4884457
-- Component ID: 4885065

with max_rank as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,line_type
        ,max(rank_over_manufacturing_location_line_type) as max_rank
    from ${dwh_schema}.fact_production_schedule
    where line_type in ('Single Loader', 'Double Loader', 'Manual')
    group by 1,2,3
)

, base_records as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,distribution_location
        ,new_lines_available_over_manufacturing_location_line_type
        ,line_type
        ,sku_without_merv_rating
        ,rank_over_manufacturing_location_line_type                     as og_rank_over_manufacturing_location_line_type
        ,sum(case when sku like '%M8' then production_need else 0 end)  as production_goal_merv_8
        ,sum(case when sku like '%M11' then production_need else 0 end) as production_goal_merv_11
        ,sum(case when sku like '%M13' then production_need else 0 end) as production_goal_merv_13
        ,sum(case when sku like '%OE' then production_need else 0 end)  as production_goal_merv_8_odor_eliminator
        ,sum(production_need)                                           as production_goal_total
        ,sum(production_lines_calc)                                     as production_lines
        ,sum(changeover_lines_calc)                                     as changeover_production_lines
    from ${stg2_schema}.ps_non_automated_rounding_within_capacity
    group by 1,2,3,4,5,6,7
)

, rankings as (
    select
        t.inserted_dt_utc
        ,t.manufacturing_location
        ,t.line_type
        ,og_rank_over_manufacturing_location_line_type
        ,row_number() over (partition by t.inserted_dt_utc, t.manufacturing_location, t.line_type
                            order by og_rank_over_manufacturing_location_line_type asc)
            + coalesce(mr.max_rank, 0) as rank_over_manufacturing_location_line_type
    from
        (select distinct
            inserted_dt_utc
            ,manufacturing_location
            ,line_type
            ,og_rank_over_manufacturing_location_line_type
        from base_records) t
        left join max_rank mr on mr.inserted_dt_utc = t.inserted_dt_utc
            and mr.manufacturing_location = t.manufacturing_location
            and mr.line_type = t.line_type
)

select
    br.inserted_dt_utc
    ,br.manufacturing_location
    ,br.distribution_location
    ,br.line_type
    ,par.adjusted_total_lines as lines_available_over_manufacturing_location_line_type
    ,r.rank_over_manufacturing_location_line_type
    ,br.sku_without_merv_rating
    ,ps.filters_per_pallet_over_sku_without_merv_rating
    ,br.production_goal_merv_8
    ,br.production_goal_merv_11
    ,br.production_goal_merv_13
    ,br.production_goal_merv_8_odor_eliminator
    ,br.production_goal_total
    ,br.production_lines
    ,ps.current_days_of_inventory_merv_8
    ,ps.current_days_of_inventory_merv_11
    ,ps.current_days_of_inventory_merv_13
    ,ps.current_days_of_inventory_merv_8_odor_eliminator
    ,ps.is_current_production_schedule
    ,ps.is_future_production_schedule
    ,true as is_within_capacity
    ,ps.is_tomorrow_production_schedule
    ,ps.runtime_dt_utc
    ,ps.non_automated_logic_type
    ,br.changeover_production_lines
    ,ps.automated_original_inserted_dt_utc
    ,ps.non_automated_efficiency_rank
    ,ps.reassigned_automated_production_lines_over_dt_manufacturing_location
    ,ps.rank_over_manufacturing_location_line_type                              as prior_to_rounding_rank_over_manufacturing_location_line_type
    ,ps.is_within_capacity                                                      as prior_to_rounding_is_within_capacity
    ,ps.lines_available_over_manufacturing_location_line_type                   as prior_to_rounding_lines_available_over_manufacturing_location_line_type
    ,ps.non_automated_reactive_logic_iteration
from base_records br
    join ${stg2_schema}.ps_non_automated_rounding_staging ps on ps.inserted_dt_utc = br.inserted_dt_utc
        and ps.manufacturing_location = br.manufacturing_location
        and ps.distribution_location = br.distribution_location
        and ps.line_type = br.line_type
        and ps.rank_over_manufacturing_location_line_type = br.og_rank_over_manufacturing_location_line_type
    join rankings r on r.inserted_dt_utc = br.inserted_dt_utc
        and r.manufacturing_location = br.manufacturing_location
        and r.line_type = br.line_type
        and r.og_rank_over_manufacturing_location_line_type = br.og_rank_over_manufacturing_location_line_type
    join ${stg2_schema}.ps_non_automated_line_rounding par on par.inserted_dt_utc = br.inserted_dt_utc
        and par.manufacturing_location = br.manufacturing_location
        and par.line_type = br.line_type