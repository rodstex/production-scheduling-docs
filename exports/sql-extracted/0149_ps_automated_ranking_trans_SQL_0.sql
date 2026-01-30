-- Job: ps_automated_ranking_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 6397979
-- Component ID: 6397978

/* Base records for automated manufacturing.
 * Filters:
 *      (1) Distribution location(s) of interest
 *      (2) Manufacturing location that feeds DC(s)
 *      (3) Manufacturing location with automated line(s)
 *      (4) SKU(s) that can be made at manufacturing location on automated lines
 *      (5) MERV rating(s) that can be made at manufacturing location on automated lines
 */
with base_records as (
    select
        fmn.runtime_dt_utc
        ,fmn.inserted_dt_utc
        ,mdch.mapped_manufacturing_location
        ,mdch.mapped_distribution_location
        ,fmn.sku
        ,fmn.sku_without_merv_rating
        ,fmn.filter_type
        ,fmn.merv_rating
        ,fmn.days_of_inventory_remaining
        ,cap.production_capacity
        ,cap.auto_tooling_sets
        ,lf.max_prod_lines_per_size
        ,round((cap.production_capacity::float / 10::float) -- {production_run_per_hour_per_line}
            * lf.min_run_hrs_per_size) as min_production_run_per_size
        ,round((cap.production_capacity::float / 10::float) -- {production_run_per_hour_per_line}
            * lf.min_run_hrs_per_merv_rating) as min_production_run_per_merv_rating
        -- Calculate production need, accounting for excess distribution (if any) --
        ,case when tdih.target_days_of_inventory >= 1 then fmn.planned_manufacturing_1_day else 0 end as planned_manufacturing_1_day
        ,case when tdih.target_days_of_inventory >= 2 then fmn.planned_manufacturing_2_day else 0 end as planned_manufacturing_2_day
        ,case when tdih.target_days_of_inventory >= 3 then fmn.planned_manufacturing_3_day else 0 end as planned_manufacturing_3_day
        ,case when tdih.target_days_of_inventory >= 4 then fmn.planned_manufacturing_4_day else 0 end as planned_manufacturing_4_day
        ,case when tdih.target_days_of_inventory >= 5 then fmn.planned_manufacturing_5_day else 0 end as planned_manufacturing_5_day
        ,case when tdih.target_days_of_inventory >= 6 then fmn.planned_manufacturing_6_day else 0 end as planned_manufacturing_6_day
        ,case when tdih.target_days_of_inventory >= 7 then fmn.planned_manufacturing_7_day else 0 end as planned_manufacturing_7_day
        ,case when tdih.target_days_of_inventory >= 8 then fmn.planned_manufacturing_8_day else 0 end as planned_manufacturing_8_day
        ,case when tdih.target_days_of_inventory >= 9 then fmn.planned_manufacturing_9_day else 0 end as planned_manufacturing_9_day
        ,case when tdih.target_days_of_inventory >= 10 then fmn.planned_manufacturing_10_day else 0 end as planned_manufacturing_10_day
        ,case when tdih.target_days_of_inventory >= 11 then fmn.planned_manufacturing_11_day else 0 end as planned_manufacturing_11_day
        ,case when tdih.target_days_of_inventory >= 12 then fmn.planned_manufacturing_12_day else 0 end as planned_manufacturing_12_day
        ,case when tdih.target_days_of_inventory >= 13 then fmn.planned_manufacturing_13_day else 0 end as planned_manufacturing_13_day
        ,case when tdih.target_days_of_inventory >= 14 then fmn.planned_manufacturing_14_day else 0 end as planned_manufacturing_14_day
        ,case when tdih.target_days_of_inventory >= 15 then fmn.planned_manufacturing_15_day else 0 end as planned_manufacturing_15_day
        ,case when tdih.target_days_of_inventory >= 16 then fmn.planned_manufacturing_16_day else 0 end as planned_manufacturing_16_day
        ,case when tdih.target_days_of_inventory >= 17 then fmn.planned_manufacturing_17_day else 0 end as planned_manufacturing_17_day
        ,case when tdih.target_days_of_inventory >= 18 then fmn.planned_manufacturing_18_day else 0 end as planned_manufacturing_18_day
        ,case when tdih.target_days_of_inventory >= 19 then fmn.planned_manufacturing_19_day else 0 end as planned_manufacturing_19_day
        ,case when tdih.target_days_of_inventory >= 20 then fmn.planned_manufacturing_20_day else 0 end as planned_manufacturing_20_day
        ,case when tdih.target_days_of_inventory >= 21 then fmn.planned_manufacturing_21_day else 0 end as planned_manufacturing_21_day
        ,case when tdih.target_days_of_inventory >= 22 then fmn.planned_manufacturing_22_day else 0 end as planned_manufacturing_22_day
        ,case when tdih.target_days_of_inventory >= 23 then fmn.planned_manufacturing_23_day else 0 end as planned_manufacturing_23_day
        ,case when tdih.target_days_of_inventory >= 24 then fmn.planned_manufacturing_24_day else 0 end as planned_manufacturing_24_day
        ,case when tdih.target_days_of_inventory >= 25 then fmn.planned_manufacturing_25_day else 0 end as planned_manufacturing_25_day
        ,case when tdih.target_days_of_inventory >= 26 then fmn.planned_manufacturing_26_day else 0 end as planned_manufacturing_26_day
        ,case when tdih.target_days_of_inventory >= 27 then fmn.planned_manufacturing_27_day else 0 end as planned_manufacturing_27_day
        ,case when tdih.target_days_of_inventory >= 28 then fmn.planned_manufacturing_28_day else 0 end as planned_manufacturing_28_day
        ,case when tdih.target_days_of_inventory >= 29 then fmn.planned_manufacturing_29_day else 0 end as planned_manufacturing_29_day
        ,case when tdih.target_days_of_inventory >= 30 then fmn.planned_manufacturing_30_day else 0 end as planned_manufacturing_30_day

        ,case when tdih.target_days_of_inventory = 1 then fmn.planned_manufacturing_1_day
            when tdih.target_days_of_inventory = 2 then fmn.planned_manufacturing_2_day
            when tdih.target_days_of_inventory = 3 then fmn.planned_manufacturing_3_day
            when tdih.target_days_of_inventory = 4 then fmn.planned_manufacturing_4_day
            when tdih.target_days_of_inventory = 5 then fmn.planned_manufacturing_5_day
            when tdih.target_days_of_inventory = 6 then fmn.planned_manufacturing_6_day
            when tdih.target_days_of_inventory = 7 then fmn.planned_manufacturing_7_day
            when tdih.target_days_of_inventory = 8 then fmn.planned_manufacturing_8_day
            when tdih.target_days_of_inventory = 9 then fmn.planned_manufacturing_9_day
            when tdih.target_days_of_inventory = 10 then fmn.planned_manufacturing_10_day
            when tdih.target_days_of_inventory = 11 then fmn.planned_manufacturing_11_day
            when tdih.target_days_of_inventory = 12 then fmn.planned_manufacturing_12_day
            when tdih.target_days_of_inventory = 13 then fmn.planned_manufacturing_13_day
            when tdih.target_days_of_inventory = 14 then fmn.planned_manufacturing_14_day
            when tdih.target_days_of_inventory = 15 then fmn.planned_manufacturing_15_day
            when tdih.target_days_of_inventory = 16 then fmn.planned_manufacturing_16_day
            when tdih.target_days_of_inventory = 17 then fmn.planned_manufacturing_17_day
            when tdih.target_days_of_inventory = 18 then fmn.planned_manufacturing_18_day
            when tdih.target_days_of_inventory = 19 then fmn.planned_manufacturing_19_day
            when tdih.target_days_of_inventory = 20 then fmn.planned_manufacturing_20_day
            when tdih.target_days_of_inventory = 21 then fmn.planned_manufacturing_21_day
            when tdih.target_days_of_inventory = 22 then fmn.planned_manufacturing_22_day
            when tdih.target_days_of_inventory = 23 then fmn.planned_manufacturing_23_day
            when tdih.target_days_of_inventory = 24 then fmn.planned_manufacturing_24_day
            when tdih.target_days_of_inventory = 25 then fmn.planned_manufacturing_25_day
            when tdih.target_days_of_inventory = 26 then fmn.planned_manufacturing_26_day
            when tdih.target_days_of_inventory = 27 then fmn.planned_manufacturing_27_day
            when tdih.target_days_of_inventory = 28 then fmn.planned_manufacturing_28_day
            when tdih.target_days_of_inventory = 29 then fmn.planned_manufacturing_29_day
            when tdih.target_days_of_inventory = 30 then fmn.planned_manufacturing_30_day end as production_need
    from ${stg2_schema}.ps_fact_manufacturing_need_future fmn
        -- Identify manufacturing location(s) that send inventory to DC --
        join ${stg2_schema}.ps_manufacturing_to_distribution_center_history mdch on mdch.is_selected_dt
            and mdch.distribution_sb_alias = fmn.location_name
        -- Truncate to manufacturing location with automated lines --
        join ${stg2_schema}.ps_staffing_by_dt pl on pl.dt = fmn.inserted_dt_utc::date
            and pl.mapped_manufacturing_location = mdch.mapped_manufacturing_location
            and pl.grouped_line_type = 'Automated'
        -- Truncate to filter types made on automated lines --
        join ${stg2_schema}.ps_target_days_of_inventory_history tdih on tdih.is_selected_dt
            and tdih.grouped_line_type = 'Automated'
            and tdih.distribution_sb_alias = fmn.location_name
            and tdih.filter_type = fmn.filter_type
        -- Truncate to SKU(s) that can be made on automated lines at the manufacturing location --
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = mdch.mapped_manufacturing_location
            and cap.line_type = 'Automated'
            and cap.sku_without_merv_rating = fmn.sku_without_merv_rating
        -- Truncate to MERV rating(s) that can be made on automated lines --
        join ${stg2_schema}.ps_automated_merv_ratings_history amr on amr.is_selected_dt
            and amr.mapped_manufacturing_location = mdch.mapped_manufacturing_location
            and amr.merv_rating = fmn.merv_rating
        -- Return line facts --
        join ${stg2_schema}.ps_line_facts_history lf on lf.is_selected_dt
            and lf.line_type = 'Automated'
        -- Filter out excluded SKUs (DATA-505) --
        left join ${stg2_schema}.ps_excluded_skus_by_location_history excl on excl.is_selected_dt
            and excl.sku_without_merv_rating = fmn.sku_without_merv_rating
            and excl.manufacturing_location = mdch.mapped_manufacturing_location
    where fmn.is_selected_dt
  		and production_need > 0
        and excl.sku_without_merv_rating is null /* SKU and manufacturing location is not an invalid combination DATA-505 */
        and not(fmn.is_custom) /* Is not custom SKU DATA-520 */
)

/* Transform base_records into one record per:
 *      Manufacturing location
 *      SKU without MERV rating
 *      Day of need
 */
, days_of_need as (
    select
        br.mapped_manufacturing_location
        ,br.sku_without_merv_rating
        ,br.production_capacity
        ,br.min_production_run_per_size
        ,i.id as day_of_need
        ,sum(case when i.id = 1 then br.planned_manufacturing_1_day
                when i.id = 2 then br.planned_manufacturing_2_day
                when i.id = 3 then br.planned_manufacturing_3_day
                when i.id = 4 then br.planned_manufacturing_4_day
                when i.id = 5 then br.planned_manufacturing_5_day
                when i.id = 6 then br.planned_manufacturing_6_day
                when i.id = 7 then br.planned_manufacturing_7_day
                when i.id = 8 then br.planned_manufacturing_8_day
                when i.id = 9 then br.planned_manufacturing_9_day
                when i.id = 10 then br.planned_manufacturing_10_day
                when i.id = 11 then br.planned_manufacturing_11_day
                when i.id = 12 then br.planned_manufacturing_12_day
                when i.id = 13 then br.planned_manufacturing_13_day
                when i.id = 14 then br.planned_manufacturing_14_day
                when i.id = 15 then br.planned_manufacturing_15_day
                when i.id = 16 then br.planned_manufacturing_16_day
                when i.id = 17 then br.planned_manufacturing_17_day
                when i.id = 18 then br.planned_manufacturing_18_day
                when i.id = 19 then br.planned_manufacturing_19_day
                when i.id = 20 then br.planned_manufacturing_20_day
                when i.id = 21 then br.planned_manufacturing_21_day
                when i.id = 22 then br.planned_manufacturing_22_day
                when i.id = 23 then br.planned_manufacturing_23_day
                when i.id = 24 then br.planned_manufacturing_24_day
                when i.id = 25 then br.planned_manufacturing_25_day
                when i.id = 26 then br.planned_manufacturing_26_day
                when i.id = 27 then br.planned_manufacturing_27_day
                when i.id = 28 then br.planned_manufacturing_28_day
                when i.id = 29 then br.planned_manufacturing_29_day
                when i.id = 30 then br.planned_manufacturing_30_day
            end) as production_need_at_day_of_need
        ,case when production_need_at_day_of_need >= min_production_run_per_size then true else false end as is_production_need_gte_min_production_run
    from base_records br
        join ${stg2_schema}.indicies i on i.id between 1 and 30
    group by 1,2,3,4,5
)

/* Rank all automated SKU without MERV rating(s) where the production need is greater than or equal to the minimum production run.
 * Rank SKU without MERV rating(s) by:
 *      (1) Day of need (ascending)
 *      (2) Production need (descending)
 *      (3) SKU (ascending)
 * #3 is only done to prevent ties.
 */
, ranking as (
    select
        mapped_manufacturing_location
        ,sku_without_merv_rating
        ,min(day_of_need) as min_day_of_need
        ,min(production_need_at_day_of_need) as min_production_need
        ,row_number() over (partition by mapped_manufacturing_location order by min(day_of_need) asc, min(production_need_at_day_of_need) desc, sku_without_merv_rating asc) as ranking
    from days_of_need
    where is_production_need_gte_min_production_run /* Truncate to records where production need is greater than or equal to the minimum production run */
    group by 1,2
)

/* Calculate the production need for each SKU and manufacturing location.
 * Evaluate minimum production run per MERV rating.
 */
, production_per_mfg_location as (
    select
        mapped_manufacturing_location
        ,sku
        ,sku_without_merv_rating
        ,production_capacity
        ,min_production_run_per_size
        ,min_production_run_per_merv_rating
        ,max_prod_lines_per_size
        ,case when sum(production_need) between 1 and min_production_run_per_merv_rating then min_production_run_per_merv_rating
            else sum(production_need) end as production_need
        ,case when sum(production_need) between 1 and min_production_run_per_merv_rating then 1
            else 0 end as min_production_flg
    from base_records
    group by 1,2,3,4,5,6,7
)

/* For each manufacturing location and SKU without MERV rating, identify the production need that is less than or equal to the maximum production lines
 * and (if exists) the production need that is greater than the maximum production lines.
 * Accounting for minimum production runs per MERV rating, identify the factor that production need should be factored by (if excess exists).
 * E.g., 20x20x1 needs 4,000 filters. 2,000 can be made on an automated line during a shift. Only 1 automated line should be spent on an automated size.
 * If none of 20x20x1's MERV ratings use the minimum production run per MERV rating, the factor becomes 50%.
 * Filter to find SKU without MERV rating(s) whose production need are greater than or equal to production capacity.
 * I.e., a SKU without MERV rating takes an entire production line to fulfill demand.
 */
, factor as (
    select
        mapped_manufacturing_location
        ,sku_without_merv_rating
        ,production_capacity
        ,max_prod_lines_per_size
        ,min_production_run_per_size
        ,round(production_capacity::float * max_prod_lines_per_size)                as max_production
        ,case when sum(production_need) > max_production then 1 else 0 end          as production_exceeds_max_production_flg
        ,sum(case when min_production_flg = 1 then production_need else 0 end)      as min_production_need
        ,sum(case when min_production_flg = 0 then production_need else 0 end)      as not_min_production_need
        ,max_production - min_production_need                                       as prod_capacity_after_min_prod_need
        ,prod_capacity_after_min_prod_need::float / not_min_production_need::float  as not_min_prod_need_factor
    from production_per_mfg_location
    group by 1,2,3,4,5
        ,production_capacity
    having sum(production_need) > min_production_run_per_size /* Filter to products whose need is greater than or equal to min production run */
)

/* For each manufacturing location and SKU, identify the production need that is
 *      (1) within maximum production capacity per SKU without MERV rating and
 *      (2) outside the maximum production capacity per SKU without MERV rating
 */
, max_production_factor as (
    select
        p.mapped_manufacturing_location
        ,p.sku
        ,p.sku_without_merv_rating
        ,case when f.production_exceeds_max_production_flg = 0 then p.production_need
            when p.min_production_flg = 1 then p.production_need
            else round(p.production_need::float * f.not_min_prod_need_factor) end as within_max_production_need
        ,case when f.production_exceeds_max_production_flg = 1 and p.min_production_flg = 0
            then p.production_need - within_max_production_need end as outside_max_production_need
    from factor f
        join production_per_mfg_location p on p.mapped_manufacturing_location = f.mapped_manufacturing_location
            and p.sku_without_merv_rating = f.sku_without_merv_rating
)

select
    br.runtime_dt_utc
    ,br.inserted_dt_utc
    ,br.mapped_manufacturing_location
    ,br.mapped_distribution_location
    ,br.sku
    ,br.filter_type
    ,br.sku_without_merv_rating
    ,br.merv_rating
    ,br.days_of_inventory_remaining
    ,br.production_capacity
    ,br.auto_tooling_sets
    ,r.ranking
    ,round(mpf.within_max_production_need::float
        * (br.production_need::float / (sum(br.production_need) over (partition by br.mapped_manufacturing_location, br.sku))::float)) as production_goal_within_max_lines
    ,round(mpf.outside_max_production_need::float
        * (br.production_need::float / (sum(br.production_need) over (partition by br.mapped_manufacturing_location, br.sku))::float)) as production_goal_outside_max_lines
from max_production_factor mpf
    join base_records br on br.mapped_manufacturing_location = mpf.mapped_manufacturing_location
        and br.sku = mpf.sku
    join ranking r on r.mapped_manufacturing_location = mpf.mapped_manufacturing_location
        and r.sku_without_merv_rating = br.sku_without_merv_rating