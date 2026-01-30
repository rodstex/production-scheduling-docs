-- Job: fps_automated_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4417452
-- Component ID: 4418731

/* Base records table */
with base_records as (
    select
        par.runtime_dt_utc
        ,par.inserted_dt_utc
        ,par.mapped_manufacturing_location
        ,par.mapped_distribution_location
        ,par.sku
        ,par.filter_type
        ,par.merv_rating
        ,par.sku_without_merv_rating
        ,par.days_of_inventory_remaining
        ,par.ranking
        ,par.production_goal_within_max_lines
        ,par.production_goal_outside_max_lines
        ,ps.staffing_available
        ,lch.count_of_lines
        ,fpp.filters_per_pallet
        ,par.production_capacity
        ,par.auto_tooling_sets
    from ${stg2_schema}.ps_automated_ranking par
        join ${stg2_schema}.ps_staffing_by_dt ps on ps.dt = par.inserted_dt_utc::date
            and par.mapped_manufacturing_location = ps.mapped_manufacturing_location
            and ps.grouped_line_type = 'Automated'
        join ${stg2_schema}.ps_line_count_history lch on lch.is_selected_dt
            and lch.mapped_manufacturing_location = par.mapped_manufacturing_location
            and lch.line_type = 'Automated'
        join ${stg2_schema}.ps_filters_per_pallet_history fpp on fpp.is_selected_dt
            and fpp.sku_without_merv_rating = par.sku_without_merv_rating
)

/* Transform from a SKU to a SKU without MERV rating basis.
 * Automated production is limited by:
 *      (1) Staffing
 *      (2) Maximum number of automated lines to run a SKU without MERV rating on
 *      (3) Tooling sets for the SKU without MERV rating
 * #1 will be considered in later CTEs.
 * #2 is considered in a prior ETL job (i.e., production_goal_within_max_lines and production_goal_outside_max_lines).
 * #3 is considered here.
 * Determine the factoring value for (1) within maximum lines and (2) outside maximum lines.
 */
, factoring as (
    select
        mapped_manufacturing_location
        ,sku_without_merv_rating
        /* Factor production goals that are within the maximum number of lines allowed (#2 above).
         * If the number of lines needed is less than or equal to the auto tooling sets, don't factor the production goal.
         * If the number of lines needed is greater than the auto tooling sets, factor the production goal down.
         */
        ,sum(production_goal_within_max_lines)                                                          as within_lines_total
        ,(sum(production_goal_within_max_lines)::decimal(10,4) / production_capacity::decimal(10,4))::decimal(10,2)     as within_lines_production_lines
        ,case when auto_tooling_sets >= within_lines_production_lines then 1
            else auto_tooling_sets::float / within_lines_production_lines end                           as within_lines_factor
        /* Identify the number of tooling sets (if any) that are still available after the within maximum number of lines production goal is satisfied */
        ,case when auto_tooling_sets >= within_lines_production_lines
            then auto_tooling_sets - within_lines_production_lines
            else 0 end                                                                                  as outside_lines_tooling_sets
        /* Factor production goals that are outside the maximum number of lines allowed (#2 above).
         * If the number of lines needed is less than or equal to the number of remaining auto tooling sets, don't factor the production goal.
         * If the number of lines needed is greater than the number of remaining auto tooling sets, factor the production goal down.
         */
        ,sum(production_goal_outside_max_lines)                                                         as outside_lines_total
        ,(sum(production_goal_outside_max_lines)::decimal(10,4) / production_capacity::decimal(10,4))::decimal(10,2)    as outside_lines_production_lines
        ,case when outside_lines_tooling_sets >= outside_lines_production_lines then 1
            else outside_lines_tooling_sets::float / outside_lines_production_lines end                 as outside_lines_factor
    from base_records
    group by 1,2
        ,production_capacity,auto_tooling_sets
)

, automated_production as (
    select
        br.runtime_dt_utc
        ,br.inserted_dt_utc
        ,br.mapped_manufacturing_location
        ,br.mapped_distribution_location
        ,'Automated' as line_type
        ,br.sku_without_merv_rating
        ,br.staffing_available as lines_available_over_manufacturing_location_line_type
        ,br.ranking as rank_over_manufacturing_location_line_type
        ,br.filters_per_pallet as filters_per_pallet_over_sku_without_merv_rating
        ,round(sum(case when br.merv_rating = 'MERV 8' then br.production_goal_within_max_lines else 0 end)::float
            * f.within_lines_factor) as production_goal_merv_8
        ,round(sum(case when br.merv_rating = 'MERV 11' then br.production_goal_within_max_lines else 0 end)::float
            * f.within_lines_factor) as production_goal_merv_11
        ,round(sum(case when br.merv_rating = 'MERV 13' then br.production_goal_within_max_lines else 0 end)::float
            * f.within_lines_factor) as production_goal_merv_13
        ,round(sum(case when br.merv_rating = 'MERV 8 Odor Eliminator' then br.production_goal_within_max_lines else 0 end)::float
            * f.within_lines_factor) as production_goal_merv_8_odor_eliminator
        ,production_goal_merv_8 + production_goal_merv_11 + production_goal_merv_13 + production_goal_merv_8_odor_eliminator as production_goal_total
        ,(production_goal_total::decimal(10,4) / br.production_capacity::decimal(10,4))::decimal(10,2) as production_lines
        ,0::decimal(10,2)   as changeover_production_lines
        /* Fix: Use decimal instead of float to avoid floating-point precision errors,
         * and add 0.01 tolerance to handle edge cases where cumulative sum slightly exceeds threshold */
        ,case when sum(production_goal_total::decimal(10,4) / br.production_capacity::decimal(10,4)) over (partition by br.mapped_manufacturing_location order by br.ranking asc rows unbounded preceding) <= (br.staffing_available + 0.01) then true
            else false end as is_within_capacity
        ,max(case when br.merv_rating = 'MERV 8' then br.days_of_inventory_remaining end) as current_days_of_inventory_merv_8
        ,max(case when br.merv_rating = 'MERV 11' then br.days_of_inventory_remaining end) as current_days_of_inventory_merv_11
        ,max(case when br.merv_rating = 'MERV 13' then br.days_of_inventory_remaining end) as current_days_of_inventory_merv_13
        ,max(case when br.merv_rating = 'MERV 8 Odor Eliminator' then br.days_of_inventory_remaining end) as current_days_of_inventory_merv_8_odor_eliminator
    from base_records br
        join factoring f on f.mapped_manufacturing_location = br.mapped_manufacturing_location
            and f.sku_without_merv_rating = br.sku_without_merv_rating
    group by 1,2,3,4,5,6,7,8,9
        ,br.production_capacity, f.within_lines_factor
)

-------------------------------------------
-- Automated Production Threshold Exceeded --
-------------------------------------------
select
    br.runtime_dt_utc
    ,br.inserted_dt_utc
    ,br.mapped_manufacturing_location
    ,br.mapped_distribution_location
    ,'Automated Production Threshold Exceeded' as line_type
    ,br.sku_without_merv_rating
    ,null::int                  as lines_available_over_manufacturing_location_line_type
    ,null::int                  as rank_over_manufacturing_location_line_type
    ,br.filters_per_pallet      as filters_per_pallet_over_sku_without_merv_rating
    ,round(sum(case when br.merv_rating = 'MERV 8' then br.production_goal_outside_max_lines else 0 end)::float
        * f.outside_lines_factor) as production_goal_merv_8
    ,round(sum(case when br.merv_rating = 'MERV 11' then br.production_goal_outside_max_lines else 0 end)::float
        * f.outside_lines_factor) as production_goal_merv_11
    ,round(sum(case when br.merv_rating = 'MERV 13' then br.production_goal_outside_max_lines else 0 end)::float
        * f.outside_lines_factor)  as production_goal_merv_13
    ,round(sum(case when br.merv_rating = 'MERV 8 Odor Eliminator' then br.production_goal_outside_max_lines else 0 end)::float
        * f.outside_lines_factor)  as production_goal_merv_8_odor_eliminator
    ,production_goal_merv_8 + production_goal_merv_11 + production_goal_merv_13 + production_goal_merv_8_odor_eliminator as production_goal_total
    ,(production_goal_total::decimal(10,4) / br.production_capacity::decimal(10,4))::decimal(10,2) as production_lines
    ,0::decimal(10,2)                                               as changeover_production_lines
    ,false                                                          as is_current_production_schedule
    ,case when '${iteration_num}'::int = 2 then true else false end as is_tomorrow_production_schedule
    ,case when '${iteration_num}'::int = 2 then false else true end as is_future_production_schedule
    ,false                                                          as is_within_capacity
    ,max(case when br.merv_rating = 'MERV 8' then br.days_of_inventory_remaining end) as current_days_of_inventory_merv_8
    ,max(case when br.merv_rating = 'MERV 11' then br.days_of_inventory_remaining end) as current_days_of_inventory_merv_11
    ,max(case when br.merv_rating = 'MERV 13' then br.days_of_inventory_remaining end) as current_days_of_inventory_merv_13
    ,max(case when br.merv_rating = 'MERV 8 Odor Eliminator' then br.days_of_inventory_remaining end) as current_days_of_inventory_merv_8_odor_eliminator
from base_records br
    join factoring f on f.mapped_manufacturing_location = br.mapped_manufacturing_location
        and f.sku_without_merv_rating = br.sku_without_merv_rating
where f.outside_lines_factor > 0
group by 1,2,3,4,5,6,7,8,9
    ,br.production_capacity, f.outside_lines_factor
having sum(br.production_goal_outside_max_lines) > 0
union all

--------------------------
-- Automated Production --
--------------------------
select
    ap0.runtime_dt_utc
    ,ap0.inserted_dt_utc
    ,ap0.mapped_manufacturing_location
    ,ap0.mapped_distribution_location
    ,ap0.line_type
    ,ap0.sku_without_merv_rating
    ,ap0.lines_available_over_manufacturing_location_line_type
    ,ap0.rank_over_manufacturing_location_line_type
    ,ap0.filters_per_pallet_over_sku_without_merv_rating
    ,ap0.production_goal_merv_8
    ,ap0.production_goal_merv_11
    ,ap0.production_goal_merv_13
    ,ap0.production_goal_merv_8_odor_eliminator
    ,ap0.production_goal_total
    ,ap0.production_lines
    ,ap0.changeover_production_lines
    ,false                                                          as is_current_production_schedule
    ,case when '${iteration_num}'::int = 2 then true else false end as is_tomorrow_production_schedule
    ,case when '${iteration_num}'::int = 2 then false else true end as is_future_production_schedule
    ,case when ap1.is_within_capacity = 0 then false
        when ap1.is_within_capacity = 1 then true end as is_within_capacity
    ,ap0.current_days_of_inventory_merv_8
    ,ap0.current_days_of_inventory_merv_11
    ,ap0.current_days_of_inventory_merv_13
    ,ap0.current_days_of_inventory_merv_8_odor_eliminator
from automated_production ap0
    join /* Subquery used to ensure all ranked products for a manufacturing location have the same is within capacity, despite rounding */
        (select
            mapped_manufacturing_location
            ,rank_over_manufacturing_location_line_type
            ,min(is_within_capacity::int) as is_within_capacity
        from automated_production
        group by 1,2) ap1 on ap1.mapped_manufacturing_location = ap0.mapped_manufacturing_location
            and ap1.rank_over_manufacturing_location_line_type = ap0.rank_over_manufacturing_location_line_type