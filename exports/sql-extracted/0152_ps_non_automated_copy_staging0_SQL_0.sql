-- Job: ps_non_automated_copy_staging0
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4430213
-- Component ID: 4432185

/* Return facts about production that has already been scheduled.
 * Switch from a SKU without MERV rating level to a SKU level.
 * FOR MERV 8 ONLY:
 *      Arbitrarily assign changeover production lines to MERV 8.
 *      Include even if MERV 8 has no production scheduled.
 *      If MERV 8 has no production scheduled, change to production of 1 to prevent divide-by-zero errors.
 */
with base_records as (
    ------------
    -- MERV 8 --
    ------------
    select
        fps.runtime_dt_utc
        ,fps.inserted_dt_utc
        ,fps.distribution_location                                  as mapped_distribution_location
        ,true                                                       as is_copied_fps
        -- Manufacturing location facts --
        ,fps.manufacturing_location                                 as mapped_manufacturing_location
        ,fps.lines_available_over_manufacturing_location_line_type  as count_of_staffing_over_mfg
        -- Manufacturing location & line type facts --
        ,fps.line_type
        ,lc.count_of_lines                                          as count_of_lines_over_mfg_line_type
        -- Manufacturing location, line type, & SKU facts --
        ,fps.sku_without_merv_rating || 'M8'                        as sku
        ,fps.sku_without_merv_rating
        ,'MERV 8'                                                   as merv_rating
        ,case when fps.production_goal_merv_8 = 0 then 1
            else fps.production_goal_merv_8 end                     as production_need
        ,case when fps.production_goal_merv_8 = 0
            then (1::float/ cap.production_capacity::float)::decimal(10,2)
            else (fps.production_goal_merv_8::float
                / cap.production_capacity::float)::decimal(10,2) end as production_lines_calc
        ,cap.production_capacity
        ,lf1.max_prod_lines_per_size                                as max_production_lines_per_size
         -- {capacity_per_hr_per_line} / {min_hrs_per_size} --
        ,round((cap.production_capacity::float / 10::float)
            * lf0.min_run_hrs_per_size::float)                      as min_production_run_per_size
         -- {capacity_per_hr_per_line} / {min_hrs_per_merv}
        ,round((cap.production_capacity::float / 10::float)
            * lf0.min_run_hrs_per_merv_rating::float)               as min_production_run_per_merv_rating
        -- {changeover_hrs_per_size} / {10_hrs}
        ,round(lf0.changeover_hrs_per_size::float
            / 10::float, 2)                                         as changeover_production_lines_per_size
        ,fps.current_days_of_inventory_merv_8                       as days_of_inventory_remaining
        ,fps.rank_over_manufacturing_location_line_type             as ranking
        ,fps.filters_per_pallet_over_sku_without_merv_rating        as filters_per_pallet
        ,fps.is_within_capacity
        ,fps.changeover_production_lines
    from ${dwh_schema}.fact_production_schedule fps
        join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = fps.manufacturing_location
            and cap.line_type = fps.line_type
            and cap.sku_without_merv_rating = fps.sku_without_merv_rating
        join ${stg2_schema}.ps_line_count_history lc on lc.is_selected_dt
            and lc.mapped_manufacturing_location = fps.manufacturing_location
            and lc.line_type = cap.line_type
        join ${stg2_schema}.ps_line_facts_history lf0 on lf0.is_selected_dt
            and lf0.line_type = fps.line_type
        join ${stg2_schema}.ps_line_facts_history lf1 on lf1.is_selected_dt
            and lf1.line_type = 'Non-Automated'
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and (fps.production_goal_merv_8 > 0 or fps.changeover_production_lines > 0)
        and fps.is_within_capacity
    union all

    -------------
    -- MERV 11 --
    -------------
    select
        fps.runtime_dt_utc
        ,fps.inserted_dt_utc
        ,fps.distribution_location                                  as mapped_distribution_location
        ,true                                                       as is_copied_fps
        -- Manufacturing location facts --
        ,fps.manufacturing_location                                 as mapped_manufacturing_location
        ,fps.lines_available_over_manufacturing_location_line_type  as count_of_staffing_over_mfg
        -- Manufacturing location & line type facts --
        ,fps.line_type
        ,lc.count_of_lines                                          as count_of_lines_over_mfg_line_type
        -- Manufacturing location, line type, & SKU facts --
        ,fps.sku_without_merv_rating || 'M11'                       as sku
        ,fps.sku_without_merv_rating
        ,'MERV 11'                                                  as merv_rating
        ,fps.production_goal_merv_11                                as production_need
        ,(fps.production_goal_merv_11::float
              / cap.production_capacity::float)::decimal(10,2)      as production_lines_calc
        ,cap.production_capacity
        ,lf1.max_prod_lines_per_size                                as max_production_lines_per_size
         -- {capacity_per_hr_per_line} / {min_hrs_per_size} --
        ,round((cap.production_capacity::float / 10::float)
            * lf0.min_run_hrs_per_size::float)                      as min_production_run_per_size
         -- {capacity_per_hr_per_line} / {min_hrs_per_merv}
        ,round((cap.production_capacity::float / 10::float)
            * lf0.min_run_hrs_per_merv_rating::float)               as min_production_run_per_merv_rating
        -- {changeover_hrs_per_size} / {10_hrs}
        ,round(lf0.changeover_hrs_per_size::float
            / 10::float, 2)                                         as changeover_production_lines_per_size
        ,fps.current_days_of_inventory_merv_11                      as days_of_inventory_remaining
        ,fps.rank_over_manufacturing_location_line_type             as ranking
        ,fps.filters_per_pallet_over_sku_without_merv_rating        as filters_per_pallet
        ,fps.is_within_capacity
        ,0                                                          as changeover_production_lines
    from ${dwh_schema}.fact_production_schedule fps
        join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = fps.manufacturing_location
            and cap.line_type = fps.line_type
            and cap.sku_without_merv_rating = fps.sku_without_merv_rating
        join ${stg2_schema}.ps_line_count_history lc on lc.is_selected_dt
            and lc.mapped_manufacturing_location = fps.manufacturing_location
            and lc.line_type = cap.line_type
        join ${stg2_schema}.ps_line_facts_history lf0 on lf0.is_selected_dt
            and lf0.line_type = fps.line_type
        join ${stg2_schema}.ps_line_facts_history lf1 on lf1.is_selected_dt
            and lf1.line_type = 'Non-Automated'
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and fps.production_goal_merv_11 > 0
        and fps.is_within_capacity
    union all

    -------------
    -- MERV 13 --
    -------------
    select
        fps.runtime_dt_utc
        ,fps.inserted_dt_utc
        ,fps.distribution_location                                  as mapped_distribution_location
        ,true                                                       as is_copied_fps
        -- Manufacturing location facts --
        ,fps.manufacturing_location                                 as mapped_manufacturing_location
        ,fps.lines_available_over_manufacturing_location_line_type  as count_of_staffing_over_mfg
        -- Manufacturing location & line type facts --
        ,fps.line_type
        ,lc.count_of_lines                                          as count_of_lines_over_mfg_line_type
        -- Manufacturing location, line type, & SKU facts --
        ,fps.sku_without_merv_rating || 'M13'                       as sku
        ,fps.sku_without_merv_rating
        ,'MERV 13'                                                  as merv_rating
        ,fps.production_goal_merv_13                                as production_need
        ,(fps.production_goal_merv_13::float
              / cap.production_capacity::float)::decimal(10,2)      as production_lines_calc
        ,cap.production_capacity
        ,lf1.max_prod_lines_per_size                                as max_production_lines_per_size
         -- {capacity_per_hr_per_line} / {min_hrs_per_size} --
        ,round((cap.production_capacity::float / 10::float)
            * lf0.min_run_hrs_per_size::float)                      as min_production_run_per_size
         -- {capacity_per_hr_per_line} / {min_hrs_per_merv}
        ,round((cap.production_capacity::float / 10::float)
            * lf0.min_run_hrs_per_merv_rating::float)               as min_production_run_per_merv_rating
        -- {changeover_hrs_per_size} / {10_hrs}
        ,round(lf0.changeover_hrs_per_size::float
            / 10::float, 2)                                         as changeover_production_lines_per_size
        ,fps.current_days_of_inventory_merv_13                      as days_of_inventory_remaining
        ,fps.rank_over_manufacturing_location_line_type             as ranking
        ,fps.filters_per_pallet_over_sku_without_merv_rating        as filters_per_pallet
        ,fps.is_within_capacity
        ,0                                                          as changeover_production_lines
    from ${dwh_schema}.fact_production_schedule fps
        join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = fps.manufacturing_location
            and cap.line_type = fps.line_type
            and cap.sku_without_merv_rating = fps.sku_without_merv_rating
        join ${stg2_schema}.ps_line_count_history lc on lc.is_selected_dt
            and lc.mapped_manufacturing_location = fps.manufacturing_location
            and lc.line_type = cap.line_type
        join ${stg2_schema}.ps_line_facts_history lf0 on lf0.is_selected_dt
            and lf0.line_type = fps.line_type
        join ${stg2_schema}.ps_line_facts_history lf1 on lf1.is_selected_dt
            and lf1.line_type = 'Non-Automated'
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and fps.production_goal_merv_13 > 0
        and fps.is_within_capacity
    union all

    ----------------------------
    -- MERV 8 Odor Eliminator --
    ----------------------------
    select
        fps.runtime_dt_utc
        ,fps.inserted_dt_utc
        ,fps.distribution_location                                  as mapped_distribution_location
        ,true                                                       as is_copied_fps
        -- Manufacturing location facts --
        ,fps.manufacturing_location                                 as mapped_manufacturing_location
        ,fps.lines_available_over_manufacturing_location_line_type  as count_of_staffing_over_mfg
        -- Manufacturing location & line type facts --
        ,fps.line_type
        ,lc.count_of_lines                                          as count_of_lines_over_mfg_line_type
        -- Manufacturing location, line type, & SKU facts --
        ,fps.sku_without_merv_rating || 'OE'                        as sku
        ,fps.sku_without_merv_rating
        ,'MERV 8 Odor Eliminator'                                   as merv_rating
        ,fps.production_goal_merv_8_odor_eliminator                 as production_need
        ,(fps.production_goal_merv_8_odor_eliminator::float
              / cap.production_capacity::float)::decimal(10,2)      as production_lines_calc
        ,cap.production_capacity
        ,lf1.max_prod_lines_per_size                                as max_production_lines_per_size
         -- {capacity_per_hr_per_line} / {min_hrs_per_size} --
        ,round((cap.production_capacity::float / 10::float)
            * lf0.min_run_hrs_per_size::float)                      as min_production_run_per_size
         -- {capacity_per_hr_per_line} / {min_hrs_per_merv}
        ,round((cap.production_capacity::float / 10::float)
            * lf0.min_run_hrs_per_merv_rating::float)               as min_production_run_per_merv_rating
        -- {changeover_hrs_per_size} / {10_hrs}
        ,round(lf0.changeover_hrs_per_size::float
            / 10::float, 2)                                         as changeover_production_lines_per_size
        ,fps.current_days_of_inventory_merv_8_odor_eliminator       as days_of_inventory_remaining
        ,fps.rank_over_manufacturing_location_line_type             as ranking
        ,fps.filters_per_pallet_over_sku_without_merv_rating        as filters_per_pallet
        ,fps.is_within_capacity
        ,0                                                          as changeover_production_lines
    from ${dwh_schema}.fact_production_schedule fps
        join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = fps.manufacturing_location
            and cap.line_type = fps.line_type
            and cap.sku_without_merv_rating = fps.sku_without_merv_rating
        join ${stg2_schema}.ps_line_count_history lc on lc.is_selected_dt
            and lc.mapped_manufacturing_location = fps.manufacturing_location
            and lc.line_type = cap.line_type
        join ${stg2_schema}.ps_line_facts_history lf0 on lf0.is_selected_dt
            and lf0.line_type = fps.line_type
        join ${stg2_schema}.ps_line_facts_history lf1 on lf1.is_selected_dt
            and lf1.line_type = 'Non-Automated'
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and fps.production_goal_merv_8_odor_eliminator > 0
        and fps.is_within_capacity
)

select
    runtime_dt_utc
    ,inserted_dt_utc
    ,mapped_distribution_location
    ,is_copied_fps
    ,mapped_manufacturing_location
    ,count_of_staffing_over_mfg
    ,line_type
    ,count_of_lines_over_mfg_line_type
    ,sku
    ,sku_without_merv_rating
    ,merv_rating
    ,production_need
    ,production_lines_calc
    ,changeover_production_lines
    ,production_capacity
    ,max_production_lines_per_size
    ,min_production_run_per_size
    ,min_production_run_per_merv_rating
    ,changeover_production_lines_per_size
    ,days_of_inventory_remaining
    ,ranking
    ,filters_per_pallet
    ,is_within_capacity
    -- Running Sums --
    /* PARTITION BY() clauses uses :
     *      (1) Rank
     *      (2) Distribution location
     *      (3) MERV rating
     * #2 and #3 are used simply to ensure calculation strategy remains consistent across executions and transformations.
     */
    ,sum(production_lines_calc + changeover_production_lines)
        over (partition by mapped_manufacturing_location order by ranking asc, mapped_distribution_location asc, merv_rating asc rows unbounded preceding) as running_sum_staffing_over_mfg
    ,sum(production_lines_calc + changeover_production_lines)
        over (partition by mapped_manufacturing_location, line_type order by ranking asc, mapped_distribution_location asc, merv_rating asc rows unbounded preceding) as running_sum_lines_over_mfg_line_type
    ,sum(production_lines_calc)
        over (partition by mapped_manufacturing_location, sku_without_merv_rating order by ranking asc, mapped_distribution_location asc, merv_rating asc rows unbounded preceding) as running_sum_production_lines_per_size
from base_records