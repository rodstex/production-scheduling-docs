-- Job: fps_non_automated_fix_changeover_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 5502423
-- Component ID: 5502381

with base_records as (
    select
        fps.inserted_dt_utc::date as dt
        ,fps.manufacturing_location
        ,fps.line_type
        ,fps.rank_over_manufacturing_location_line_type
        ,fps.sku_without_merv_rating
        ,round(lfh.changeover_hrs_per_size::float / 10::float, 2)   as changeover_production_lines_per_size
        ,sum(fps.changeover_production_lines)                       as changeover_production_lines
    from ${dwh_schema}.fact_production_schedule fps
        join ${stg2_schema}.ps_line_facts_history lfh on lfh.inserted_dt_utc = fps.runtime_dt_utc
            and fps.line_type = lfh.line_type
        join
            (select distinct inserted_dt_utc::date as dt from ${stg2_schema}.ps_fact_manufacturing_need_future where is_selected_dt) t on t.dt = fps.inserted_dt_utc::date
    where fps.line_type in ('Single Loader', 'Double Loader','Manual') /* Non-Automated production lines only */
    group by 1,2,3,4,5,6
)

, error_detection as (
    select
        dt
        ,manufacturing_location
        ,line_type
        ,rank_over_manufacturing_location_line_type
        ,sku_without_merv_rating
        ,lag(sku_without_merv_rating) over (partition by dt, manufacturing_location, line_type order by rank_over_manufacturing_location_line_type asc) as prior_sku_without_merv_rating
        ,changeover_production_lines
        ,changeover_production_lines_per_size
        ,case
            when changeover_production_lines_per_size = 0                                                           then 0 /* Line type has no changeover costs */
            when prior_sku_without_merv_rating is null                      and changeover_production_lines = 0     then 0 /* CORRECT: is 1st size to be scheduled to date, location, and line type and has no changeover */
            when prior_sku_without_merv_rating is null                      and changeover_production_lines != 0    then 1 /* INCORRECT: is 1st size to be scheduled to date, location, and line type and has changeover */
            when sku_without_merv_rating = prior_sku_without_merv_rating    and changeover_production_lines = 0     then 0 /* CORRECT: prior size is the same and no changeover */
            when sku_without_merv_rating = prior_sku_without_merv_rating    and changeover_production_lines != 0    then 1 /* INCORRECT: prior size is the same and changeover */
            when sku_without_merv_rating != prior_sku_without_merv_rating   and changeover_production_lines = 0     then 1 /* INCORRECT: prior size is not the same and no changeover */
            when sku_without_merv_rating != prior_sku_without_merv_rating   and changeover_production_lines != 0    then 0 /* CORRECT: prior size is not the same and changeover */
        end as is_error_flg
        ,case
            when changeover_production_lines_per_size = 0                                                           then null
            when prior_sku_without_merv_rating is null                      and changeover_production_lines = 0     then null
            when prior_sku_without_merv_rating is null                      and changeover_production_lines != 0    then 'Is 1st size to be scheduled to date, location, and line type and has changeover'
            when sku_without_merv_rating = prior_sku_without_merv_rating    and changeover_production_lines = 0     then null
            when sku_without_merv_rating = prior_sku_without_merv_rating    and changeover_production_lines != 0    then 'Prior size is the same and changeover'
            when sku_without_merv_rating != prior_sku_without_merv_rating   and changeover_production_lines = 0     then 'Prior size is not the same and no changeover'
            when sku_without_merv_rating != prior_sku_without_merv_rating   and changeover_production_lines != 0    then null
        end as error_description
        ,case
            when changeover_production_lines_per_size = 0                                                           then null::decimal
            when prior_sku_without_merv_rating is null                      and changeover_production_lines = 0     then null::decimal
            when prior_sku_without_merv_rating is null                      and changeover_production_lines != 0    then 0::decimal
            when sku_without_merv_rating = prior_sku_without_merv_rating    and changeover_production_lines = 0     then null
            when sku_without_merv_rating = prior_sku_without_merv_rating    and changeover_production_lines != 0    then 0::decimal
            when sku_without_merv_rating != prior_sku_without_merv_rating   and changeover_production_lines = 0     then changeover_production_lines_per_size
            when sku_without_merv_rating != prior_sku_without_merv_rating   and changeover_production_lines != 0    then null
        end as correct_changeover_production_lines
    from base_records
)

select
    inserted_dt_utc
    ,manufacturing_location
    ,distribution_location
    ,line_type
    ,rank_over_manufacturing_location_line_type
    ,correct_changeover_production_lines
from
    (select
        fps.inserted_dt_utc
        ,fps.manufacturing_location
        ,fps.distribution_location
        ,fps.line_type
        ,fps.rank_over_manufacturing_location_line_type
        ,ed.correct_changeover_production_lines
        ,row_number() over (partition by fps.inserted_dt_utc, fps.manufacturing_location, fps.line_type, fps.rank_over_manufacturing_location_line_type order by fps.distribution_location asc) as rn
    from error_detection ed
        join ${dwh_schema}.fact_production_schedule fps on fps.inserted_dt_utc::date = ed.dt
            and fps.manufacturing_location = ed.manufacturing_location
            and fps.line_type = ed.line_type
            and fps.rank_over_manufacturing_location_line_type = ed.rank_over_manufacturing_location_line_type
    where ed.is_error_flg = 1 /* Is error */) t
where rn = 1