-- Job: fps_non_automated_line_rounding_iteration
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_line_rounding
-- Component: Initialize stg2.ps_non_automated_rounding_within_capacity
-- Type: SQL Query
-- Job ID: 4883578
-- Component ID: 4884159

/* Return all records that are of concern.
 * Calculate records that are outside of capacity.
 * Return the lines available for prior records.
 */
with base_records as (
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
        ,running_sum_staffing_over_mfg_line_type
        ,og_is_within_capacity
        -- Helper Columns --
        ,case when running_sum_staffing_over_mfg_line_type > new_lines_available_over_manufacturing_location_line_type then true else false end as is_outside_capacity
        ,coalesce(new_lines_available_over_manufacturing_location_line_type - lag(running_sum_staffing_over_mfg_line_type) over (partition by inserted_dt_utc, manufacturing_location, line_type order by ranking asc)
            , new_lines_available_over_manufacturing_location_line_type) as prior_lines_available
    from ${stg2_schema}.ps_non_automated_rounding_staging_iteration
)

/* If any record(s) are not within automated staff reassignment, identify the first record that is outside of capacity.
 */
, record_to_factor as (
    select
        inserted_dt_utc
        ,manufacturing_location
  		,line_type
        ,min(ranking) as min_ranking
    from base_records
    where is_outside_capacity
    group by 1,2,3
)

---------------------
-- WITHIN CAPACITY --
---------------------
select
    inserted_dt_utc
    ,manufacturing_location
    ,distribution_location
    ,line_type
    ,sku_without_merv_rating
    ,rank_over_manufacturing_location_line_type
    ,sku
    ,production_need
    ,production_lines_calc
    ,changeover_lines_calc
    ,new_lines_available_over_manufacturing_location_line_type
    ,ranking
    ,running_sum_staffing_over_mfg_line_type
    ,og_is_within_capacity
from base_records
where not(is_outside_capacity)
union all

----------------------
-- RECORD TO FACTOR --
----------------------
select
    br.inserted_dt_utc
    ,br.manufacturing_location
    ,br.distribution_location
    ,br.line_type
    ,br.sku_without_merv_rating
    ,br.rank_over_manufacturing_location_line_type
    ,br.sku
    ,round(br.prior_lines_available * br.production_capacity::float) as production_need
    ,br.prior_lines_available::decimal(10,2) as production_lines_calc
    ,br.changeover_lines_calc
    ,br.new_lines_available_over_manufacturing_location_line_type
    ,br.ranking
    ,br.running_sum_staffing_over_mfg_line_type
    ,br.og_is_within_capacity
from base_records br
    join record_to_factor rtf on rtf.inserted_dt_utc = br.inserted_dt_utc
        and rtf.manufacturing_location = br.manufacturing_location
        and rtf.min_ranking = br.ranking
        and rtf.line_type = br.line_type