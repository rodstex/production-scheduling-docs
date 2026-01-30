-- Job: ps_non_automated_copy_staging2
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4449417
-- Component ID: 4449422

/* Identify the production that has already been identified as out of capacity for each manufacturing location, distribution center, and SKU.
 */
with prior_production as (
    select
        manufacturing_location
        ,distribution_location
        ,sku_without_merv_rating
        ,sum(production_goal_merv_8) as prior_merv8
        ,sum(production_goal_merv_11) as prior_merv11
        ,sum(production_goal_merv_13) as prior_merv13
        ,sum(production_goal_merv_8_odor_eliminator) as prior_oe
    from sandbox_stg2.ps_non_automated_outside_capacity
  	where outside_capacity_logic_type = 'Manufacturing Location Staffing Limit'
    group by 1,2,3
)

/* Identify the production lines that have already been scheduled for each MFG location and SKU without MERV rating.
 */
, prior_production_lines_over_mfg_sku as (
    select
        fps.manufacturing_location
        ,fps.sku_without_merv_rating
        ,sum(production_lines) as prior_lines_over_mfg_sku
    from sandbox.fact_production_schedule fps
        join (select distinct inserted_dt_utc from sandbox_stg2.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and fps.is_within_capacity
    group by 1,2
)

/* Identify the production lines that have already been scheduled for each MFG location and line type.
 */
, prior_production_lines_over_mfg_line_type as (
    select
        fps.manufacturing_location
        ,fps.line_type
        ,sum(production_lines) as prior_lines_over_mfg_line
    from sandbox.fact_production_schedule fps
        join (select distinct inserted_dt_utc from sandbox_stg2.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and fps.is_within_capacity
    group by 1,2
)

/* Identify the production lines that have already been scheduled for each MFG location.
 */
, prior_production_lines_over_mfg as (
    select
        fps.manufacturing_location
        ,sum(production_lines) as prior_lines_over_mfg
    from sandbox.fact_production_schedule fps
        join (select distinct inserted_dt_utc from sandbox_stg2.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and fps.is_within_capacity
    group by 1
)

select
    t.runtime_dt_utc
    ,t.inserted_dt_utc
    ,t.mapped_distribution_location
    ,t.is_copied_fps
    ,t.mapped_manufacturing_location
    ,t.count_of_staffing_over_mfg
    ,t.line_type
    ,t.count_of_lines_over_mfg_line_type
    ,t.sku
    ,t.sku_without_merv_rating
    ,t.merv_rating
    ,t.production_capacity
    ,t.max_production_lines_per_size
    ,t.min_production_run_per_size
    ,t.min_production_run_per_merv_rating
    ,t.changeover_production_lines_per_size
    ,t.days_of_inventory_remaining
    ,t.ranking
    ,t.filter_per_pallet
    ,t.production_need -
        case when t.merv_rating = 'MERV 8' then coalesce(pp.prior_merv8, 0)
            when t.merv_rating = 'MERV 11' then coalesce(pp.prior_merv11, 0)
            when t.merv_rating = 'MERV 13' then coalesce(pp.prior_merv13, 0)
            when t.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(pp.prior_oe, 0) end as production_need_calc
    ,(production_need_calc::float / t.production_capacity::float)::decimal(10,2) as production_lines_calc
    ,null::boolean as is_within_capacity
    -- Running Sums --
    /* PARTITION BY() clauses uses :
     *      (1) Rank
     *      (2) Distribution location
     *      (3) MERV rating
     * #2 and #3 are used simply to ensure calculation strategy remains consistent across executions and transformations.
     */
    ,sum(production_lines_calc)
        over (partition by t.mapped_manufacturing_location order by t.ranking asc, t.mapped_distribution_location asc, t.merv_rating asc rows unbounded preceding)
        + coalesce(pp_mfg.prior_lines_over_mfg, 0) as running_sum_staffing_over_mfg
    ,sum(production_lines_calc)
        over (partition by t.mapped_manufacturing_location, t.line_type order by t.ranking asc, t.mapped_distribution_location asc, t.merv_rating asc rows unbounded preceding)
        + coalesce(pp_mfg_line.prior_lines_over_mfg_line, 0) as running_sum_lines_over_mfg_line_type
    ,sum(production_lines_calc)
        over (partition by t.mapped_manufacturing_location, t.sku_without_merv_rating order by t.ranking asc, t.mapped_distribution_location asc, t.merv_rating asc rows unbounded preceding)
        + coalesce(pp_mfg_sku.prior_lines_over_mfg_sku, 0) as running_sum_production_lines_per_size
from sandbox_stg2.ps_non_automated_production_schedule_staging_copy t
    -- Prior outside of capacity facts --
    left join prior_production pp on pp.manufacturing_location = t.mapped_manufacturing_location
        and pp.distribution_location = t.mapped_distribution_location
        and pp.sku_without_merv_rating = t.sku_without_merv_rating
    -- Prior production lines by MFG and SKU without MERV rating --
    left join prior_production_lines_over_mfg_sku pp_mfg_sku on pp_mfg_sku.manufacturing_location = t.mapped_manufacturing_location
        and pp_mfg_sku.sku_without_merv_rating = t.sku_without_merv_rating
    -- Prior production lines by MFG and line type --
    left join prior_production_lines_over_mfg_line_type pp_mfg_line on pp_mfg_line.manufacturing_location = t.mapped_manufacturing_location
        and pp_mfg_line.line_type = t.line_type
    -- Prior production lines by MFG --
    left join prior_production_lines_over_mfg pp_mfg on pp_mfg.manufacturing_location = t.mapped_manufacturing_location
where production_need_calc > 0