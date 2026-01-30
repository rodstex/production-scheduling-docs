-- Job: fps_automated_reschedule
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_production
-- Component: Initialize stg2.ps_auto_day_five
-- Type: SQL Query
-- Job ID: 4650671
-- Component ID: 4650899

/* Return the staffing that is available on automated lines each day of the following week. */
with base_staffing as (
    select
        dt
        ,mapped_manufacturing_location
        ,staffing_available
        ,row_number() over (partition by mapped_manufacturing_location order by dt asc) as rn
    from ${stg2_schema}.ps_staffing_by_dt
    where grouped_line_type = 'Automated'
        and mapped_manufacturing_location in (${manufacturing_locations})
        and date_trunc('week', dt) = date_trunc('week', '${auto_reschedule_week_dt}'::date)
)

/* Return facts about automated production schedule.
 * The date that a SKU was originally scheduled and the production lines scheduled.
 * Filter to within capacity only.
 */
, base_records as (
    select
        manufacturing_location
        ,inserted_dt_utc            as original_scheduled_dt
        ,sku_without_merv_rating
        ,sum(production_lines)      as production_lines
    from ${stg2_schema}.ps_automated_production_schedule_staging
    where manufacturing_location in (${manufacturing_locations})
        and date_trunc('week', inserted_dt_utc) = date_trunc('week', '${auto_reschedule_week_dt}'::date)
    group by 1,2,3
)

/* Determine the row number for each instance of a product being scheduled.
 * Order by production lines descending. This ensures that partial production lines are scheduled later in the week.
 * Also rank each SKU without MERV rating over the manufacturing location.
 * Calculate the total production goal for each SKU without MERV rating.
 * Rank SKU without MERV ratings by the total production goal descending.
 * round() is used for production lines to prevent decimal point rounding interfering with production lines scheduled.
 * E.g., 1.01 lines and 1.00 lines would not be within 2 staffing available.
 */
, ranking as (
    select
        br.manufacturing_location
        ,br.original_scheduled_dt
        ,br.sku_without_merv_rating
        ,br.production_lines
        ,round(br.production_lines, 1)                                                                                                                          as production_lines_rounded
        ,row_number() over (partition by br.manufacturing_location, br.sku_without_merv_rating order by br.production_lines desc, br.original_scheduled_dt asc) as rn_over_sku_without_merv_rating
        ,sum(br.production_lines) over (partition by br.manufacturing_location, br.sku_without_merv_rating)                                                     as total_production_lines_needed
        ,br1.rank_over_mfg_location
    from base_records br
        join
            (select
                manufacturing_location
                ,sku_without_merv_rating
                ,sum(production_lines)
                ,row_number() over (partition by manufacturing_location order by sum(production_lines) desc, sku_without_merv_rating) as rank_over_mfg_location
            from base_records
            group by 1,2) br1 on br1.manufacturing_location = br.manufacturing_location
                and br1.sku_without_merv_rating = br.sku_without_merv_rating
)

, day_5_ranking as (
    select
        r.manufacturing_location
        ,r.sku_without_merv_rating
        ,r.production_lines
        ,r.original_scheduled_dt
        ,r.production_lines_rounded
        ,r.rank_over_mfg_location
        ,(case when day1_r.sku_without_merv_rating is null then 1 else 0 end)
            + (case when day2_r.sku_without_merv_rating is null then 1 else 0 end)
            + (case when day3_r.sku_without_merv_rating is null then 1 else 0 end)
            + (case when day4_r.sku_without_merv_rating is null then 1 else 0 end) as prior_not_scheduled_skus
        ,r.rn_over_sku_without_merv_rating + prior_not_scheduled_skus as rn_over_sku_without_merv_rating
    from ranking r
        /* Join on the exact instance */
        left join ${stg2_schema}.ps_auto_day_one day1 on day1.mapped_manufacturing_location = r.manufacturing_location
            and day1.sku_without_merv_rating = r.sku_without_merv_rating
            and day1.original_scheduled_dt = r.original_scheduled_dt
        left join ${stg2_schema}.ps_auto_day_two day2 on day2.mapped_manufacturing_location = r.manufacturing_location
            and day2.sku_without_merv_rating = r.sku_without_merv_rating
            and day2.original_scheduled_dt = r.original_scheduled_dt
        left join ${stg2_schema}.ps_auto_day_three day3 on day3.mapped_manufacturing_location = r.manufacturing_location
            and day3.sku_without_merv_rating = r.sku_without_merv_rating
            and day3.original_scheduled_dt = r.original_scheduled_dt
        left join ${stg2_schema}.ps_auto_day_four day4 on day4.mapped_manufacturing_location = r.manufacturing_location
            and day4.sku_without_merv_rating = r.sku_without_merv_rating
            and day4.original_scheduled_dt = r.original_scheduled_dt
        /* Join on the SKU w/o MERV Rating */
        left join (select distinct mapped_manufacturing_location, sku_without_merv_rating from ${stg2_schema}.ps_auto_day_one) day1_r on day1_r.mapped_manufacturing_location = r.manufacturing_location
            and day1_r.sku_without_merv_rating = r.sku_without_merv_rating
        left join (select distinct mapped_manufacturing_location, sku_without_merv_rating from ${stg2_schema}.ps_auto_day_two) day2_r on day2_r.mapped_manufacturing_location = r.manufacturing_location
            and day2_r.sku_without_merv_rating = r.sku_without_merv_rating
        left join (select distinct mapped_manufacturing_location, sku_without_merv_rating from ${stg2_schema}.ps_auto_day_three) day3_r on day3_r.mapped_manufacturing_location = r.manufacturing_location
            and day3_r.sku_without_merv_rating = r.sku_without_merv_rating
        left join (select distinct mapped_manufacturing_location, sku_without_merv_rating from ${stg2_schema}.ps_auto_day_four) day4_r on day4_r.mapped_manufacturing_location = r.manufacturing_location
            and day4_r.sku_without_merv_rating = r.sku_without_merv_rating
    where day1.sku_without_merv_rating is null
        and day2.sku_without_merv_rating is null
        and day3.sku_without_merv_rating is null
        and day4.sku_without_merv_rating is null
)

select
    mapped_manufacturing_location
    ,sku_without_merv_rating
    ,t.original_scheduled_dt
    ,br.original_scheduled_dt as new_scheduled_dt
    ,t.production_lines
    ,t.staffing_available as new_lines_available_over_manufacturing_location_line_type
from
    (select
        bs.dt
        ,bs.mapped_manufacturing_location
     	,bs.staffing_available
        ,r.sku_without_merv_rating
        ,r.production_lines
        ,r.original_scheduled_dt
        ,sum(r.production_lines_rounded) over (partition by bs.mapped_manufacturing_location, bs.dt order by r.rank_over_mfg_location asc rows unbounded preceding) as rolling_sum_production_lines
        ,bs.staffing_available - rolling_sum_production_lines as production_lines_remaining
    from base_staffing bs
        join day_5_ranking r on r.manufacturing_location = bs.mapped_manufacturing_location
            and r.rn_over_sku_without_merv_rating = bs.rn
    where bs.rn = 5 /* 5th date in period */) t
        join (select distinct original_scheduled_dt from base_records) br on br.original_scheduled_dt::date = t.dt::date
    where production_lines_remaining >= 0