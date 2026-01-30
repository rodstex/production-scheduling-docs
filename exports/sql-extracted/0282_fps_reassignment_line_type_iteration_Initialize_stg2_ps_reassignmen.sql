-- Job: fps_reassignment_line_type_iteration
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/automated_staffing_reassignment
-- Component: Initialize stg2.ps_reassignment_within_capacity
-- Type: SQL Query
-- Job ID: 4856308
-- Component ID: 4860416

/* Return base records that automated staffing needs to be reassigned to.
 * Identify record(s) that are outside of the automated staffing able to be reassigned.
 * Return the prior amount of automated staffing available.
 */
with base_records_mfg as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,distribution_location
        ,line_type
        ,count_of_lines_over_mfg_line_type
        ,rank_over_manufacturing_location_line_type
        ,sku_without_merv_rating
        ,sku
        ,production_need
        ,production_capacity
        ,production_lines_calc
        ,changeover_lines_calc
        ,automated_production_lines_available_to_reassign
        ,ranking
        ,running_sum_staffing_over_mfg
        -- Helper Columns --
        ,case when running_sum_staffing_over_mfg > automated_production_lines_available_to_reassign then true else false end as is_outside_mfg_capacity
        ,coalesce(automated_production_lines_available_to_reassign - lag(running_sum_staffing_over_mfg) over (partition by inserted_dt_utc, manufacturing_location order by ranking asc)
            , automated_production_lines_available_to_reassign) as prior_staff_available
    from ${stg2_schema}.ps_reassignment_staging_iteration
)

/* If any record(s) are not within automated staff reassignment, identify the first record that is outside of capacity.
 */
, record_to_factor_mfg as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,min(ranking) as min_ranking
    from base_records_mfg
    where is_outside_mfg_capacity
    group by 1,2
)

/* Identify the production needs that are within the excess automated staff reassigned to a MFG location.
 */
, mfg_final as (
    ----------------------
    -- Within capacity --
    ----------------------
    select
        br.inserted_dt_utc
        ,br.manufacturing_location
        ,br.distribution_location
        ,br.line_type
        ,br.count_of_lines_over_mfg_line_type
        ,br.rank_over_manufacturing_location_line_type
        ,br.sku_without_merv_rating
        ,br.sku
        ,br.production_need as production_need_calc
        ,br.production_capacity
        ,br.production_lines_calc
        ,br.changeover_lines_calc
        ,br.automated_production_lines_available_to_reassign
        ,br.ranking
        ,br.running_sum_staffing_over_mfg
    from base_records_mfg br
    where not(is_outside_mfg_capacity)
    union all

    ----------------------
    -- Record to factor --
    ----------------------
    select
        br.inserted_dt_utc
        ,br.manufacturing_location
        ,br.distribution_location
        ,br.line_type
        ,br.count_of_lines_over_mfg_line_type
        ,br.rank_over_manufacturing_location_line_type
        ,br.sku_without_merv_rating
        ,br.sku
        ,round(br.prior_staff_available::float * br.production_capacity::float) as production_need_calc
        ,br.production_capacity
        ,br.prior_staff_available as production_lines_calc
        ,br.changeover_lines_calc
        ,br.automated_production_lines_available_to_reassign
        ,br.ranking
        ,br.running_sum_staffing_over_mfg
    from base_records_mfg br
        join record_to_factor_mfg rtf on rtf.inserted_dt_utc = br.inserted_dt_utc
            and rtf.manufacturing_location = br.manufacturing_location
            and rtf.min_ranking = br.ranking
)

, base_records_mfg_line as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,distribution_location
        ,line_type
        ,count_of_lines_over_mfg_line_type
        ,rank_over_manufacturing_location_line_type
        ,sku_without_merv_rating
        ,sku
        ,production_need_calc
        ,production_capacity
        ,production_lines_calc
        ,changeover_lines_calc
        ,automated_production_lines_available_to_reassign
        ,ranking
        ,running_sum_staffing_over_mfg_line_type
        ,is_outside_mfg_line_capacity
        ,coalesce(count_of_lines_over_mfg_line_type - lag(running_sum_staffing_over_mfg_line_type) over (partition by inserted_dt_utc, manufacturing_location order by ranking asc)
            , count_of_lines_over_mfg_line_type) as prior_lines_available
    from
        (select
            inserted_dt_utc
            ,manufacturing_location
            ,distribution_location
            ,line_type
            ,count_of_lines_over_mfg_line_type
            ,rank_over_manufacturing_location_line_type
            ,sku_without_merv_rating
            ,sku
            ,production_need_calc
            ,production_capacity
            ,production_lines_calc
            ,changeover_lines_calc
            ,automated_production_lines_available_to_reassign
            ,ranking
            ,sum(production_lines_calc)
                over (partition by inserted_dt_utc, manufacturing_location, line_type order by ranking asc rows unbounded preceding)
            + sum(changeover_lines_calc)
                over (partition by inserted_dt_utc, manufacturing_location, line_type order by ranking asc rows unbounded preceding) as running_sum_staffing_over_mfg_line_type
            -- Helper Columns --
            ,case when running_sum_staffing_over_mfg_line_type > count_of_lines_over_mfg_line_type then true else false end as is_outside_mfg_line_capacity
        from mfg_final) t
)

/* If any record(s) are not within the number of lines available, identify the first record that is outside of capacity.
 */
, record_to_factor_mfg_line as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,min(ranking) as min_ranking
    from base_records_mfg_line
    where is_outside_mfg_line_capacity
    group by 1,2
)

----------------------
-- Within capacity --
----------------------
select
    br.inserted_dt_utc
    ,br.manufacturing_location
    ,br.distribution_location
    ,br.line_type
    ,br.count_of_lines_over_mfg_line_type
    ,br.rank_over_manufacturing_location_line_type
    ,br.sku_without_merv_rating
    ,br.sku
    ,br.production_need_calc as production_need_calc
    ,br.production_capacity
    ,br.production_lines_calc
    ,br.changeover_lines_calc
    ,br.automated_production_lines_available_to_reassign
    ,br.ranking
    ,br.running_sum_staffing_over_mfg_line_type
from base_records_mfg_line br
where not(is_outside_mfg_line_capacity)
union all

----------------------
-- Record to factor --
----------------------
select
    br.inserted_dt_utc
    ,br.manufacturing_location
    ,br.distribution_location
    ,br.line_type
    ,br.count_of_lines_over_mfg_line_type
    ,br.rank_over_manufacturing_location_line_type
    ,br.sku_without_merv_rating
    ,br.sku
    ,round(br.prior_lines_available::float * br.production_capacity::float) as production_need_calc
    ,br.production_capacity
    ,br.prior_lines_available as production_lines_calc
    ,br.changeover_lines_calc
    ,br.automated_production_lines_available_to_reassign
    ,br.ranking
    ,br.running_sum_staffing_over_mfg_line_type
from base_records_mfg_line br
    join record_to_factor_mfg_line rtf on rtf.inserted_dt_utc = br.inserted_dt_utc
        and rtf.manufacturing_location = br.manufacturing_location
        and rtf.min_ranking = br.ranking