-- Job: fps_non_automated_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4438508
-- Component ID: 4438543

/* Return all the products that are within capacities. */
with base_within_capacity as (
    select
        inserted_dt_utc
        ,mapped_manufacturing_location                                                                  as manufacturing_location
        ,mapped_distribution_location                                                                   as distribution_location
        ,line_type
        ,count_of_staffing_over_mfg                                                                     as lines_available_over_manufacturing_location_line_type
        ,ranking
        ,sku_without_merv_rating
        ,filter_per_pallet                                                                              as filters_per_pallet_over_sku_without_merv_rating
        ,sum(case when merv_rating = 'MERV 8'                   then production_need else 0 end)        as production_goal_merv_8
        ,sum(case when merv_rating = 'MERV 11'                  then production_need else 0 end)        as production_goal_merv_11
        ,sum(case when merv_rating = 'MERV 13'                  then production_need else 0 end)        as production_goal_merv_13
        ,sum(case when merv_rating = 'MERV 8 Odor Eliminator'   then production_need else 0 end)        as production_goal_merv_8_odor_eliminator
        ,production_goal_merv_8 + production_goal_merv_11
             + production_goal_merv_13 + production_goal_merv_8_odor_eliminator                         as production_goal_total
        ,(production_goal_total::float / production_capacity::float)::decimal(10,2)                     as production_lines
        ,min(case when merv_rating = 'MERV 8'                   then days_of_inventory_remaining end)   as current_days_of_inventory_merv_8
        ,min(case when merv_rating = 'MERV 11'                  then days_of_inventory_remaining end)   as current_days_of_inventory_merv_11
        ,min(case when merv_rating = 'MERV 13'                  then days_of_inventory_remaining end)   as current_days_of_inventory_merv_13
        ,min(case when merv_rating = 'MERV 8 Odor Eliminator'   then days_of_inventory_remaining end)   as current_days_of_inventory_merv_8_odor_eliminator
        ,true                                                                                           as is_within_capacity
        ,runtime_dt_utc
        ,sum(changeover_production_lines)                                                               as changeover_production_lines
    from ${stg2_schema}.ps_non_automated_production_schedule_staging
    where not(is_copied_fps)
    group by 1,2,3,4,5,6,7,8
        ,runtime_dt_utc, production_capacity
)

/* stg2.ps_non_automated_production_schedule_staging ranking comes from stg2.ps_non_automated_ranking
 * stg2.ps_non_automated_ranking ranks products based on their production need over the manufacturing location.
 * However, fact_production_schedule shows ranking over the manufacturing location and line type.
 * Furthermore, fact_production_schedule may already have records for the manufacturing location and line type.
 * Therefore, a new ranking must be calculated.
 */
, within_capacity_ranking as (
    select
        manufacturing_location
        ,line_type
        ,ranking
        ,row_number() over (partition by manufacturing_location, line_type order by ranking asc)
            + max_rank as rank_over_manufacturing_location_line_type
    from
        (select distinct /* DISTINCT is necessary in case >1 distribution center exist for the same MFG location, line type, and rank */
            bwc.manufacturing_location
            ,bwc.line_type
            ,bwc.ranking
            ,coalesce(fps.max_rank, 0) as max_rank
        from base_within_capacity bwc
            left join
                (select
                    fps.manufacturing_location
                    ,fps.line_type
                    ,max(fps.rank_over_manufacturing_location_line_type) as max_rank
                from ${dwh_schema}.fact_production_schedule fps
                    join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_production_schedule_staging) t on t.inserted_dt_utc = fps.inserted_dt_utc
                group by 1,2) fps on fps.manufacturing_location = bwc.manufacturing_location
                    and fps.line_type = bwc.line_type) t
)

, base_outside_capacity as (
    select
        oc.inserted_dt_utc
        ,oc.manufacturing_location
        ,oc.distribution_location
        ,oc.line_type
        ,oc.lines_available_over_manufacturing_location_line_type
        ,oc.sku_without_merv_rating
        ,oc.filters_per_pallet_over_sku_without_merv_rating
        ,oc.runtime_dt_utc
        ,'${non_automated_logic_type}'                              as non_automated_logic_type
        ,oc.is_within_capacity
        ,min(oc.rank_over_manufacturing_location_line_type)         as ranking
        ,sum(oc.production_goal_merv_8)                             as production_goal_merv_8
        ,sum(oc.production_goal_merv_11)                            as production_goal_merv_11
        ,sum(oc.production_goal_merv_13)                            as production_goal_merv_13
        ,sum(oc.production_goal_merv_8_odor_eliminator)             as production_goal_merv_8_odor_eliminator
        ,sum(oc.production_goal_total)                              as production_goal_total
        ,sum(oc.production_lines)                                   as production_lines
        ,min(oc.current_days_of_inventory_merv_8)                   as current_days_of_inventory_merv_8
        ,min(oc.current_days_of_inventory_merv_11)                  as current_days_of_inventory_merv_11
        ,min(oc.current_days_of_inventory_merv_13)                  as current_days_of_inventory_merv_13
        ,min(oc.current_days_of_inventory_merv_8_odor_eliminator)   as current_days_of_inventory_merv_8_odor_eliminator
        ,sum(oc.changeover_production_lines)                        as changeover_production_lines
    from ${stg2_schema}.ps_non_automated_outside_capacity oc
    group by 1,2,3,4,5,6,7,8,9,10
)

, outside_capacity_ranking as (
    select
        manufacturing_location
        ,line_type
        ,ranking
        ,row_number() over (partition by manufacturing_location, line_type order by ranking asc)
            + coalesce(max_rank, 0) as rank_over_manufacturing_location_line_type
    from
        (select distinct
            oc.manufacturing_location
            ,oc.line_type
            ,oc.ranking
            ,coalesce(wcr.max_rank, fps.max_rank, fps1.max_rank, 0) as max_rank
        from base_outside_capacity oc
            left join
                (select
                    manufacturing_location
                    ,line_type
                    ,max(rank_over_manufacturing_location_line_type) as max_rank
                from within_capacity_ranking
                group by 1,2) wcr on wcr.manufacturing_location = oc.manufacturing_location
                    and wcr.line_type = oc.line_type
            left join
                (select
                    fps.manufacturing_location
                    ,fps.line_type
                    ,max(fps.rank_over_manufacturing_location_line_type) as max_rank
                from ${dwh_schema}.fact_production_schedule fps
                    join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_outside_capacity) t on t.inserted_dt_utc = fps.inserted_dt_utc
                group by 1,2) fps on fps.manufacturing_location = oc.manufacturing_location
                    and fps.line_type = oc.line_type
            left join
                (select
                    fps.manufacturing_location
                    ,fps.line_type
                    ,max(fps.rank_over_manufacturing_location_line_type) as max_rank
                from ${dwh_schema}.fact_production_schedule fps
                    join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_production_schedule_staging) t on t.inserted_dt_utc = fps.inserted_dt_utc
                group by 1,2) fps1 on fps1.manufacturing_location = oc.manufacturing_location
                    and fps1.line_type = oc.line_type) t
)

, final as (
  ---------------------
  -- Within capacity --
  ---------------------
  select
      bwc.inserted_dt_utc
      ,bwc.manufacturing_location
      ,bwc.distribution_location
      ,bwc.line_type
      ,bwc.lines_available_over_manufacturing_location_line_type
      ,r.rank_over_manufacturing_location_line_type
      ,bwc.sku_without_merv_rating
      ,bwc.filters_per_pallet_over_sku_without_merv_rating
      ,bwc.production_goal_merv_8
      ,bwc.production_goal_merv_11
      ,bwc.production_goal_merv_13
      ,bwc.production_goal_merv_8_odor_eliminator
      ,bwc.production_goal_total
      ,bwc.production_lines
      ,bwc.current_days_of_inventory_merv_8
      ,bwc.current_days_of_inventory_merv_11
      ,bwc.current_days_of_inventory_merv_13
      ,bwc.current_days_of_inventory_merv_8_odor_eliminator
      ,false                                                              as is_current_production_schedule
      ,case when '${iteration_num0}'::int = 2 then true else false end    as is_tomorrow_production_schedule
      ,case when '${iteration_num0}'::int = 2 then false else true end    as is_future_production_schedule
      ,bwc.runtime_dt_utc
      ,'${non_automated_logic_type}' as non_automated_logic_type
      ,bwc.is_within_capacity
      ,bwc.changeover_production_lines
      ,'${iteration_num}'::int as non_automated_efficiency_rank
  from base_within_capacity bwc
      join within_capacity_ranking r on r.manufacturing_location = bwc.manufacturing_location
          and r.line_type = bwc.line_type
          and r.ranking = bwc.ranking
  union all

  ----------------------
  -- Outside capacity --
  ----------------------
  select
      oc.inserted_dt_utc
      ,oc.manufacturing_location
      ,oc.distribution_location
      ,oc.line_type
      ,oc.lines_available_over_manufacturing_location_line_type
      ,ocr.rank_over_manufacturing_location_line_type
      ,oc.sku_without_merv_rating
      ,oc.filters_per_pallet_over_sku_without_merv_rating
      ,oc.production_goal_merv_8
      ,oc.production_goal_merv_11
      ,oc.production_goal_merv_13
      ,oc.production_goal_merv_8_odor_eliminator
      ,oc.production_goal_total
      ,oc.production_lines
      ,oc.current_days_of_inventory_merv_8
      ,oc.current_days_of_inventory_merv_11
      ,oc.current_days_of_inventory_merv_13
      ,oc.current_days_of_inventory_merv_8_odor_eliminator
      ,false                                                              as is_current_production_schedule
      ,case when '${iteration_num0}'::int = 2 then true else false end    as is_tomorrow_production_schedule
      ,case when '${iteration_num0}'::int = 2 then false else true end    as is_future_production_schedule
      ,oc.runtime_dt_utc
      ,'${non_automated_logic_type}' as non_automated_logic_type
      ,oc.is_within_capacity
      ,null::decimal as changeover_production_lines
      ,'${iteration_num}'::int as non_automated_efficiency_rank
  from base_outside_capacity oc
      join outside_capacity_ranking ocr on ocr.manufacturing_location = oc.manufacturing_location
          and ocr.line_type = oc.line_type
          and ocr.ranking = oc.ranking
)

, prior_sku as (
    select distinct
        fps.manufacturing_location
        ,fps.line_type
        ,fps.sku_without_merv_rating as max_scheduled_sku_without_merv_rating
    from
        (select
            fps.inserted_dt_utc
            ,fps.manufacturing_location
            ,fps.line_type
            ,max(fps.rank_over_manufacturing_location_line_type) as max_rank
        from ${dwh_schema}.fact_production_schedule fps
            join (select distinct inserted_dt_utc from ${stg2_schema}.ps_fact_manufacturing_need_future where is_selected_dt) t on t.inserted_dt_utc = fps.inserted_dt_utc
        where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        group by 1,2,3) t
        join ${dwh_schema}.fact_production_schedule fps on fps.inserted_dt_utc = t.inserted_dt_utc
            and fps.manufacturing_location = t.manufacturing_location
            and fps.line_type = t.line_type
            and fps.rank_over_manufacturing_location_line_type = t.max_rank
)

select
    f.inserted_dt_utc
    ,f.manufacturing_location
    ,f.distribution_location
    ,f.line_type
    ,f.lines_available_over_manufacturing_location_line_type
    ,f.rank_over_manufacturing_location_line_type
    ,f.sku_without_merv_rating
    ,f.filters_per_pallet_over_sku_without_merv_rating
    ,f.production_goal_merv_8
    ,f.production_goal_merv_11
    ,f.production_goal_merv_13
    ,f.production_goal_merv_8_odor_eliminator
    ,f.production_goal_total
    ,f.production_lines
    ,f.current_days_of_inventory_merv_8
    ,f.current_days_of_inventory_merv_11
    ,f.current_days_of_inventory_merv_13
    ,f.current_days_of_inventory_merv_8_odor_eliminator
    ,f.is_current_production_schedule
    ,f.is_tomorrow_production_schedule
    ,f.is_future_production_schedule
    ,f.runtime_dt_utc
    ,f.non_automated_logic_type
    ,f.is_within_capacity
    ,coalesce(lag(f.sku_without_merv_rating) over (partition by f.inserted_dt_utc, f.manufacturing_location, f.line_type order by f.rank_over_manufacturing_location_line_type asc)
        ,ps.max_scheduled_sku_without_merv_rating) as prior_sku_without_merv_rating
    ,case when f.changeover_production_lines is not null then f.changeover_production_lines
        when prior_sku_without_merv_rating is null then 0::decimal
        when f.sku_without_merv_rating = prior_sku_without_merv_rating then 0::decimal
        else round(lfh.changeover_hrs_per_size::float / 10::float, 2) -- {changeover_hrs_per_size} / {10_hrs}
    end as changeover_production_lines_calc
    ,f.non_automated_efficiency_rank
    ,case when '${reactive_logic_iteration}'::int = 0 then null::int else '${reactive_logic_iteration}'::int end as non_automated_reactive_logic_iteration
from final f
    left join prior_sku ps on ps.manufacturing_location = f.manufacturing_location
        and ps.line_type = f.line_type
    left join ${stg2_schema}.ps_line_facts_history lfh on lfh.is_selected_dt
        and lfh.line_type = f.line_type