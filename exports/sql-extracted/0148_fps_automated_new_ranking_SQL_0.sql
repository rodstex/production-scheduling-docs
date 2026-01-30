-- Job: fps_automated_new_ranking
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4853447
-- Component ID: 4853616

with base_records as (
    select
        inserted_dt_utc
        ,automated_original_inserted_dt_utc
        ,manufacturing_location
        ,line_type
        ,rank_over_manufacturing_location_line_type as og_rank
        ,sku_without_merv_rating
        ,is_within_capacity
        ,lines_available_over_manufacturing_location_line_type
        ,sum(production_goal_total) as production_goal_total
        ,sum(production_lines) as production_lines_total
    from ${stg2_schema}.ps_auto_fact_production_schedule_copy
    group by 1,2,3,4,5,6,7,8
)

, new_ranking as (
    select
        inserted_dt_utc
        ,coalesce(automated_original_inserted_dt_utc, inserted_dt_utc) as automated_original_inserted_dt_utc /* DATA-396 */
        ,manufacturing_location
        ,line_type
        ,lines_available_over_manufacturing_location_line_type
        ,sku_without_merv_rating
        ,production_lines_total
        ,og_rank
        ,row_number() over (partition by inserted_dt_utc, manufacturing_location, line_type
                            order by is_within_capacity desc, og_rank asc, production_goal_total desc, sku_without_merv_rating asc) as new_rank
        ,case when new_rank <= lines_available_over_manufacturing_location_line_type then true else false end as is_within_capacity /* DATA-395 */
    from base_records
)

select
    fps.inserted_dt_utc
    ,fps.manufacturing_location
    ,fps.distribution_location
    ,fps.line_type
    ,fps.lines_available_over_manufacturing_location_line_type
    ,nr.new_rank as rank_over_manufacturing_location_line_type
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
    ,nr.is_within_capacity
    ,fps.is_tomorrow_production_schedule
    ,fps.runtime_dt_utc
    ,fps.non_automated_logic_type
    ,fps.changeover_production_lines
    ,nr.automated_original_inserted_dt_utc
    ,fps.non_automated_efficiency_rank
    ,fps.reassigned_automated_production_lines_over_dt_manufacturing_location
    ,fps.prior_to_rounding_rank_over_manufacturing_location_line_type
    ,fps.prior_to_rounding_is_within_capacity
    ,fps.prior_to_rounding_lines_available_over_manufacturing_location_line_type
from new_ranking nr
    join ${stg2_schema}.ps_auto_fact_production_schedule_copy fps on fps.inserted_dt_utc = nr.inserted_dt_utc
        and fps.manufacturing_location = nr.manufacturing_location
        and fps.line_type = nr.line_type
        and fps.rank_over_manufacturing_location_line_type = nr.og_rank
        and fps.sku_without_merv_rating = nr.sku_without_merv_rating
        and (fps.is_within_capacity = nr.is_within_capacity)