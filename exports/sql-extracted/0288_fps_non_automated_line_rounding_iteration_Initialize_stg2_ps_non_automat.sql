-- Job: fps_non_automated_line_rounding_iteration
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_line_rounding
-- Component: Initialize stg2.ps_non_automated_rounding_staging_iteration
-- Type: SQL Query
-- Job ID: 4883578
-- Component ID: 4883607

/* Products that are outside of capacity are added to all production lines that it can be manufactured on.
 * E.g., SKU_A can be manufactured on Single, Double, and Manual lines but the manufacturing location has no capacity remaining.
 *      SKU_A will be scheduled on Single, Double, and Manual lines as outside of capacity.
 * I.e., out of capacity SKUs will be repeated on other line types.
 * To prevent double-scheduling, check for products that were originally out of capacity but are now within capacity
 * using the rounded production capacities.
 */
with previously_scheduled_skus as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,distribution_location
        ,sku
        ,sum(production_need) as previously_scheduled_production_need
    from ${stg2_schema}.ps_non_automated_rounding_within_capacity
    where not(og_is_within_capacity)
    group by 1,2,3,4
)
    
, previously_scheduled_lines as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,line_type
        ,sum(production_lines) + sum(coalesce(changeover_production_lines, 0)) as total_lines_scheduled
    from ${dwh_schema}.fact_production_schedule
    where line_type in ('Single Loader', 'Double Loader', 'Manual')
    group by 1,2,3
)

/* Return all records that are the efficiency rank of concern.
 */
, base_records as (
    select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,fps.distribution_location
        ,fps.line_type
        ,r.adjusted_total_lines::float
            - coalesce(psl.total_lines_scheduled, 0) as new_lines_available_over_manufacturing_location_line_type
        ,fps.rank_over_manufacturing_location_line_type
        ,fps.sku_without_merv_rating
        ,fps.filters_per_pallet_over_sku_without_merv_rating
        ,fps.production_goal_merv_8
        ,fps.production_goal_merv_11
        ,fps.production_goal_merv_13
        ,fps.production_goal_merv_8_odor_eliminator
        ,fps.production_goal_total
        ,fps.production_lines
        ,fps.current_days_of_inventory_merv_8
        ,fps.current_days_of_inventory_merv_11
        ,fps.current_days_of_inventory_merv_13
        ,fps.current_days_of_inventory_merv_8_odor_eliminator
        ,fps.is_current_production_schedule
        ,fps.is_future_production_schedule
        ,fps.is_within_capacity as og_is_within_capacity
        ,fps.is_tomorrow_production_schedule
        ,fps.runtime_dt_utc
        ,fps.non_automated_logic_type
        ,fps.changeover_production_lines
        ,fps.automated_original_inserted_dt_utc
        ,fps.non_automated_efficiency_rank
        ,fps.reassigned_automated_production_lines_over_dt_manufacturing_location
    from ${stg2_schema}.ps_non_automated_rounding_staging fps
        join ${stg2_schema}.ps_non_automated_line_rounding r on r.inserted_dt_utc = fps.inserted_dt_utc
            and r.manufacturing_location = fps.manufacturing_location
            and r.line_type = fps.line_type
        left join previously_scheduled_lines psl on psl.inserted_dt_utc = fps.inserted_dt_utc
            and psl.manufacturing_location = fps.manufacturing_location
            and psl.line_type = fps.line_type
    where fps.non_automated_efficiency_rank = '${iteration_num}'::int
        and new_lines_available_over_manufacturing_location_line_type > 0
)

/* Transform production schedule from SKU without MERV rating to SKU.
 * Assign changeover lines (if any) to the SKU with the largest production need.
 */
, fact_production_schedule_transformed as (
    select
        t.inserted_dt_utc
        ,t.manufacturing_location
        ,t.distribution_location
        ,t.line_type
        ,t.new_lines_available_over_manufacturing_location_line_type
        ,t.rank_over_manufacturing_location_line_type
        ,t.sku_without_merv_rating
        ,t.sku
        ,t.og_is_within_capacity
        ,case when t.og_is_within_capacity and ps.previously_scheduled_production_need is not null then t.production_need - ps.previously_scheduled_production_need
            else t.production_need end as production_need_calc
        ,t.production_need::float / caph.production_capacity::float as production_lines_calc
        ,caph.production_capacity
        ,row_number() over (partition by t.inserted_dt_utc, t.manufacturing_location, t.line_type, t.sku_without_merv_rating order by t.production_need desc, t.sku asc) as sku_rn
        ,case when sku_rn = 1 then t.changeover_production_lines else 0 end as changeover_lines_calc
    from
        (------------
        -- MERV 8 --
        ------------
        select
            br.inserted_dt_utc
            ,br.manufacturing_location
            ,br.distribution_location
            ,br.line_type
            ,br.new_lines_available_over_manufacturing_location_line_type
            ,br.rank_over_manufacturing_location_line_type
            ,br.sku_without_merv_rating
            ,br.sku_without_merv_rating || 'M8' as sku
            ,br.production_goal_merv_8 as production_need
            ,br.changeover_production_lines
            ,br.og_is_within_capacity
        from base_records br
        where br.production_goal_merv_8 > 0
        union all

        -------------
        -- MERV 11 --
        -------------
        select
            br.inserted_dt_utc
            ,br.manufacturing_location
            ,br.distribution_location
            ,br.line_type
            ,br.new_lines_available_over_manufacturing_location_line_type
            ,br.rank_over_manufacturing_location_line_type
            ,br.sku_without_merv_rating
            ,br.sku_without_merv_rating || 'M11' as sku
            ,br.production_goal_merv_11 as production_need
            ,br.changeover_production_lines
            ,br.og_is_within_capacity
        from base_records br
        where br.production_goal_merv_11 > 0
        union all

        -------------
        -- MERV 13 --
        -------------
        select
            br.inserted_dt_utc
            ,br.manufacturing_location
            ,br.distribution_location
            ,br.line_type
            ,br.new_lines_available_over_manufacturing_location_line_type
            ,br.rank_over_manufacturing_location_line_type
            ,br.sku_without_merv_rating
            ,br.sku_without_merv_rating || 'M13' as sku
            ,br.production_goal_merv_13 as production_need
            ,br.changeover_production_lines
            ,br.og_is_within_capacity
        from base_records br
        where br.production_goal_merv_13 > 0
        union all

        ---------------------
        -- ODOR ELIMINATOR --
        ---------------------
        select
            br.inserted_dt_utc
            ,br.manufacturing_location
            ,br.distribution_location
            ,br.line_type
            ,br.new_lines_available_over_manufacturing_location_line_type
            ,br.rank_over_manufacturing_location_line_type
            ,br.sku_without_merv_rating
            ,br.sku_without_merv_rating || 'OE' as sku
            ,br.production_goal_merv_8_odor_eliminator as production_need
            ,br.changeover_production_lines
            ,br.og_is_within_capacity
        from base_records br
        where br.production_goal_merv_8_odor_eliminator > 0) t
        join ${stg2_schema}.ps_capacity_by_sku_history caph on caph.is_selected_dt
            and caph.mapped_manufacturing_location = t.manufacturing_location
            and caph.line_type = t.line_type
            and caph.sku_without_merv_rating = t.sku_without_merv_rating
        left join previously_scheduled_skus ps on ps.inserted_dt_utc = t.inserted_dt_utc
            and ps.manufacturing_location = t.manufacturing_location
            and ps.distribution_location = t.distribution_location
            and ps.sku = t.sku
    where production_need_calc > 0 
)

/* Calculate a date and manufacturing location level ranking.
 * Order by (1) line type (ascending), (2) original rank over location & line type, and (3) SKU rank.
 * Ordering by line type ensures that double loader lines have excess automated staff reassigned first, followed by single loader, followed by manual.
 */
, ranking as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,distribution_location
        ,line_type
        ,sku_without_merv_rating
        ,rank_over_manufacturing_location_line_type
        ,sku
        ,production_need_calc as production_need
        ,production_capacity
        ,production_lines_calc
        ,changeover_lines_calc
        ,new_lines_available_over_manufacturing_location_line_type
        ,row_number() over (partition by inserted_dt_utc, manufacturing_location, line_type
            order by rank_over_manufacturing_location_line_type asc, sku_rn asc) as ranking

        ,og_is_within_capacity
    from fact_production_schedule_transformed
)

select
    inserted_dt_utc
    ,manufacturing_location
    ,distribution_location
    ,line_type
    ,sku_without_merv_rating
    ,rank_over_manufacturing_location_line_type
    ,sku
    ,production_need
    ,production_capacity
    ,production_lines_calc
    ,changeover_lines_calc
    ,new_lines_available_over_manufacturing_location_line_type
    ,ranking
    ,og_is_within_capacity
    -- Running Sums --
    ,sum(production_lines_calc) over (partition by inserted_dt_utc, manufacturing_location, line_type
                                    order by ranking asc rows unbounded preceding)
    + sum(changeover_lines_calc) over (partition by inserted_dt_utc, manufacturing_location, line_type
                                    order by ranking asc rows unbounded preceding) as running_sum_staffing_over_mfg_line_type
from ranking