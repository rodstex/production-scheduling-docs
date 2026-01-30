-- Job: fps_reassignment_line_type_iteration
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: Initialize stg2.ps_reassignment_staging_iteration
-- Type: SQL Query
-- Job ID: 4856308
-- Component ID: 4856958

/* Return the amount of production and changeover lines that were changed to within capacity using reassigned staff from automated lines.
 */
with previously_reassigned_staff as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,reassigned_automated_production_lines_over_dt_manufacturing_location
        ,sum(production_lines) + sum(coalesce(changeover_production_lines, 0)) as reassigned_production_and_changeover_lines
    from ${dwh_schema}.fact_production_schedule
    where (is_tomorrow_production_schedule or is_future_production_schedule)
        and is_within_capacity
        and reassigned_automated_production_lines_over_dt_manufacturing_location is not null
    group by 1,2,3
)

/* Outside of capacity SKUs have their demand repeated across line types.
 * E.g., if SKU_A is outside of capacity for Single Loader, it is added to Single Loader.
 *      If SKU_A is also outside of capacity for Double Loader, it is added to Double Loader.
 * Thus, previously-reassigned production need needs to account for this.
 */
, previously_reassigned_skus as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,sku_without_merv_rating
        ,sum(production_goal_merv_8) as reassigned_merv_8
        ,sum(production_goal_merv_11) as reassigned_merv_11
        ,sum(production_goal_merv_13) as reassigned_merv_13
        ,sum(production_goal_merv_8_odor_eliminator) as reassigned_merv_8_odor_eliminator
    from ${dwh_schema}.fact_production_schedule
    where (is_tomorrow_production_schedule or is_future_production_schedule)
        and is_within_capacity
        and reassigned_automated_production_lines_over_dt_manufacturing_location is not null
    group by 1,2,3

)

/* Transform fact_production_schedule from SKU without MERV Rating to SKU level.
 * If the record has changeover lines, assign changeover lines to the SKU with the greatest production need.
 * Filter to the line efficiency of concern.
 */
, fact_production_schedule_transformed as (
    select
        prs.inserted_dt_utc
        ,prs.manufacturing_location
        ,t.distribution_location
        ,t.line_type
        ,t.sku_without_merv_rating
        ,t.rank_over_manufacturing_location_line_type
        ,t.sku
        ,t.production_need
        ,row_number() over (partition by t.inserted_dt_utc, t.manufacturing_location, t.line_type, t.rank_over_manufacturing_location_line_type
                            order by production_need desc) as rn_over_sku
        ,case when rn_over_sku = 1 then t.changeover_production_lines else 0::decimal(10,2) end as changeover_lines_calc
        ,prs.automated_production_lines_available_to_reassign - coalesce(p.reassigned_production_and_changeover_lines, 0) as automated_production_lines_available_to_reassign
    from
        (select distinct
            inserted_dt_utc
            ,manufacturing_location
            ,automated_production_lines_available_to_reassign
        from ${stg2_schema}.ps_reassignment_staging) prs
        left join previously_reassigned_staff p on p.inserted_dt_utc = prs.inserted_dt_utc
            and p.manufacturing_location = prs.manufacturing_location
        join
            (------------
            -- MERV 8 --
            ------------
            select
                prs.inserted_dt_utc
                ,prs.manufacturing_location
                ,prs.distribution_location
                ,prs.line_type
                ,prs.sku_without_merv_rating
                ,prs.rank_over_manufacturing_location_line_type
                ,prs.sku_without_merv_rating || 'M8'                    as sku
                ,prs.production_goal_merv_8
                    - coalesce(p.reassigned_merv_8, 0)                  as production_need
                ,prs.changeover_production_lines
            from ${stg2_schema}.ps_reassignment_staging prs
                left join previously_reassigned_skus p on p.inserted_dt_utc = prs.inserted_dt_utc
                    and p.manufacturing_location = prs.manufacturing_location
                    and p.sku_without_merv_rating = prs.sku_without_merv_rating
            where (prs.production_goal_merv_8 - coalesce(p.reassigned_merv_8, 0)) > 0
                and prs.non_automated_efficiency_rank = '${iteration_num}'::int
            union all

            -------------
            -- MERV 11 --
            -------------
            select
                prs.inserted_dt_utc
                ,prs.manufacturing_location
                ,prs.distribution_location
                ,prs.line_type
                ,prs.sku_without_merv_rating
                ,prs.rank_over_manufacturing_location_line_type
                ,prs.sku_without_merv_rating || 'M11'                   as sku
                ,prs.production_goal_merv_11
                    - coalesce(p.reassigned_merv_11, 0)                  as production_need
                ,prs.changeover_production_lines
            from ${stg2_schema}.ps_reassignment_staging prs
                left join previously_reassigned_skus p on p.inserted_dt_utc = prs.inserted_dt_utc
                    and p.manufacturing_location = prs.manufacturing_location
                    and p.sku_without_merv_rating = prs.sku_without_merv_rating
            where (prs.production_goal_merv_11 - coalesce(p.reassigned_merv_11, 0)) > 0
                and prs.non_automated_efficiency_rank = '${iteration_num}'::int
            union all

            -------------
            -- MERV 13 --
            -------------
            select
                prs.inserted_dt_utc
                ,prs.manufacturing_location
                ,prs.distribution_location
                ,prs.line_type
                ,prs.sku_without_merv_rating
                ,prs.rank_over_manufacturing_location_line_type
                ,prs.sku_without_merv_rating || 'M13'                   as sku
                ,prs.production_goal_merv_13
                    - coalesce(p.reassigned_merv_13, 0)                  as production_need
                ,prs.changeover_production_lines
            from ${stg2_schema}.ps_reassignment_staging prs
                left join previously_reassigned_skus p on p.inserted_dt_utc = prs.inserted_dt_utc
                    and p.manufacturing_location = prs.manufacturing_location
                    and p.sku_without_merv_rating = prs.sku_without_merv_rating
            where (prs.production_goal_merv_13 - coalesce(p.reassigned_merv_13, 0)) > 0
                and prs.non_automated_efficiency_rank = '${iteration_num}'::int
            union all

            ---------------------
            -- Odor Eliminator --
            ---------------------
            select
                prs.inserted_dt_utc
                ,prs.manufacturing_location
                ,prs.distribution_location
                ,prs.line_type
                ,prs.sku_without_merv_rating
                ,prs.rank_over_manufacturing_location_line_type
                ,prs.sku_without_merv_rating || 'OE'                    as sku
                ,prs.production_goal_merv_8_odor_eliminator
                    - coalesce(p.reassigned_merv_8_odor_eliminator, 0)  as production_need
                ,prs.changeover_production_lines
            from ${stg2_schema}.ps_reassignment_staging prs
                left join previously_reassigned_skus p on p.inserted_dt_utc = prs.inserted_dt_utc
                    and p.manufacturing_location = prs.manufacturing_location
                    and p.sku_without_merv_rating = prs.sku_without_merv_rating
            where (prs.production_goal_merv_8_odor_eliminator - coalesce(p.reassigned_merv_8_odor_eliminator, 0)) > 0
                and prs.non_automated_efficiency_rank = '${iteration_num}'::int) t on t.inserted_dt_utc = prs.inserted_dt_utc
                    and t.manufacturing_location = prs.manufacturing_location
    where (prs.automated_production_lines_available_to_reassign - coalesce(p.reassigned_production_and_changeover_lines, 0)) > 0
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
        ,production_need
        ,changeover_lines_calc
        ,automated_production_lines_available_to_reassign
        ,row_number() over (partition by inserted_dt_utc, manufacturing_location
            order by line_type asc, rank_over_manufacturing_location_line_type asc, rn_over_sku asc) as ranking
    from fact_production_schedule_transformed
)

select
    r.inserted_dt_utc
    ,r.manufacturing_location
    ,r.distribution_location
    ,r.line_type
    ,lch.count_of_lines - coalesce(fps.scheduled_production_and_changeover_lines, 0) as count_of_lines_over_mfg_line_type
    ,r.rank_over_manufacturing_location_line_type
    ,r.sku_without_merv_rating
    ,r.sku
    ,r.production_need
    ,caph.production_capacity
    ,r.production_need::float / caph.production_capacity::float as production_lines_calc
    ,r.changeover_lines_calc
    ,r.automated_production_lines_available_to_reassign
    ,r.ranking
    -- Running Sums --
    ,sum(production_lines_calc)
        over (partition by r.inserted_dt_utc, r.manufacturing_location order by r.ranking asc rows unbounded preceding)
    + sum(changeover_lines_calc)
        over (partition by r.inserted_dt_utc, r.manufacturing_location order by r.ranking asc rows unbounded preceding) as running_sum_staffing_over_mfg
from ranking r
    join ${stg2_schema}.ps_line_count_history lch on lch.is_selected_dt
        and lch.mapped_manufacturing_location = r.manufacturing_location
        and lch.line_type = r.line_type
    join ${stg2_schema}.ps_capacity_by_sku_history caph on caph.is_selected_dt
        and caph.mapped_manufacturing_location = r.manufacturing_location
        and caph.line_type = r.line_type
        and caph.sku_without_merv_rating = r.sku_without_merv_rating
    left join
        (select
            inserted_dt_utc
            ,manufacturing_location
            ,line_type
            ,sum(production_lines) + sum(coalesce(changeover_production_lines, 0)) as scheduled_production_and_changeover_lines
        from ${dwh_schema}.fact_production_schedule
        where (is_tomorrow_production_schedule or is_future_production_schedule)
            and is_within_capacity
            and line_type in ('Single Loader', 'Double Loader', 'Manual')
        group by 1,2,3) fps on fps.inserted_dt_utc = r.inserted_dt_utc
            and fps.manufacturing_location = r.manufacturing_location
            and fps.line_type = r.line_type