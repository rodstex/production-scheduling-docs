-- Job: ps_non_automated_append_staging0
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_production
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4432352
-- Component ID: 4432512

/* Identify the production that has already been scheduled for each manufacturing location, distribution center, and SKU.
 * Include (1) non-automated production and (2) non-automated threshold exceeded.
 */
with prior_production as (
    select
        manufacturing_location
        ,distribution_location
        ,sku_without_merv_rating
        ,sum(prior_merv8)   as prior_merv8
        ,sum(prior_merv11)  as prior_merv11
        ,sum(prior_merv13)  as prior_merv13
        ,sum(prior_oe)      as prior_oe
    from
        (select
            fps.manufacturing_location
            ,fps.distribution_location
            ,fps.sku_without_merv_rating
            ,sum(fps.production_goal_merv_8)                    as prior_merv8
            ,sum(fps.production_goal_merv_11)                   as prior_merv11
            ,sum(fps.production_goal_merv_13)                   as prior_merv13
            ,sum(fps.production_goal_merv_8_odor_eliminator)    as prior_oe
        from ${dwh_schema}.fact_production_schedule fps
            join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
        where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
            and fps.is_within_capacity
        group by 1,2,3
        union all
        select
            fps.manufacturing_location
            ,fps.distribution_location
            ,fps.sku_without_merv_rating
            ,sum(fps.production_goal_merv_8)                    as prior_merv8
            ,sum(fps.production_goal_merv_11)                   as prior_merv11
            ,sum(fps.production_goal_merv_13)                   as prior_merv13
            ,sum(fps.production_goal_merv_8_odor_eliminator)    as prior_oe
        from ${dwh_schema}.fact_production_schedule fps
            join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
        where fps.line_type = 'Non-Automated Production Threshold Exceeded'
        group by 1,2,3) t
    group by 1,2,3
)

/* Identify the production lines that have already been scheduled for each MFG location and SKU without MERV rating.
 */
, prior_production_lines_over_mfg_sku as (
    select
        fps.manufacturing_location
        ,fps.sku_without_merv_rating
        ,sum(production_lines) as prior_lines_over_mfg_sku
    from ${dwh_schema}.fact_production_schedule fps
        join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
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
        ,sum(production_lines + changeover_production_lines) as prior_lines_over_mfg_line
    from ${dwh_schema}.fact_production_schedule fps
        join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and fps.is_within_capacity
    group by 1,2
)

/* Identify the production lines that have already been scheduled for each MFG location.
 */
, prior_production_lines_over_mfg as (
    select
        fps.manufacturing_location
        ,sum(production_lines + changeover_production_lines) as prior_lines_over_mfg
    from ${dwh_schema}.fact_production_schedule fps
        join (select distinct inserted_dt_utc from ${stg2_schema}.ps_non_automated_ranking) nar on nar.inserted_dt_utc = fps.inserted_dt_utc
    where fps.line_type in ('Single Loader', 'Double Loader', 'Manual')
        and fps.is_within_capacity
    group by 1
)

, base_records as (
    select
        nar.runtime_dt_utc
        ,nar.inserted_dt_utc
        ,nar.mapped_distribution_location
        ,false                              as is_copied_fps
        -- Manufacturing location facts --
        ,nar.mapped_manufacturing_location
        ,sbd.staffing_available            as count_of_staffing_over_mfg
        -- Manufacturing location & line type facts --
        ,cap.line_type
        ,lc.count_of_lines                 as count_of_lines_over_mfg_line_type
        -- Manufacturing location, line type, & SKU facts --
        ,nar.sku
        ,nar.sku_without_merv_rating
        ,nar.merv_rating
        ,nar.production_need -
            case when nar.merv_rating = 'MERV 8' then coalesce(pp.prior_merv8, 0)
                when nar.merv_rating = 'MERV 11' then coalesce(pp.prior_merv11, 0)
                when nar.merv_rating = 'MERV 13' then coalesce(pp.prior_merv13, 0)
                when nar.merv_rating = 'MERV 8 Odor Eliminator' then coalesce(pp.prior_oe, 0) end as production_need_calc
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
        ,nar.days_of_inventory_remaining
        ,nar.ranking
        ,nar.filters_per_pallet
        ,null::boolean                                              as is_within_capacity
    from ${stg2_schema}.ps_non_automated_ranking nar
        -- Capacity facts --
        join ${stg2_schema}.ps_capacity_by_sku_history cap on cap.is_selected_dt
            and cap.mapped_manufacturing_location = nar.mapped_manufacturing_location
            and cap.sku_without_merv_rating = nar.sku_without_merv_rating
            and cap.non_automated_efficiency_rank = '${iteration_num}'
        -- Line count facts --
        join ${stg2_schema}.ps_line_count_history lc on lc.is_selected_dt
            and lc.mapped_manufacturing_location = nar.mapped_manufacturing_location
            and lc.line_type = cap.line_type
        -- Staffing facts --
        join ${stg2_schema}.ps_staffing_by_dt sbd on sbd.mapped_manufacturing_location = nar.mapped_manufacturing_location
            and sbd.grouped_line_type = 'Non-Automated'
            and sbd.dt = nar.inserted_dt_utc::date
        -- Line facts for assigned line type --
        join ${stg2_schema}.ps_line_facts_history lf0 on lf0.is_selected_dt
            and lf0.line_type = cap.line_type
        -- Line facts for non-automated line types --
        join ${stg2_schema}.ps_line_facts_history lf1 on lf1.is_selected_dt
            and lf1.line_type = 'Non-Automated'
        -- Prior production facts --
        left join prior_production pp on pp.manufacturing_location = nar.mapped_manufacturing_location
            and pp.distribution_location = nar.mapped_distribution_location
            and pp.sku_without_merv_rating = nar.sku_without_merv_rating
    where not(nar.is_unscheduled_production)
        and production_need_calc > 0
)

/* Evaluating the minimum production runs per MERV rating and SKU without MERV rating,
 * calculate the factors that production needs to be multiplied by.
 */
, min_production_runs as (
    select
        mapped_manufacturing_location
        ,line_type
        ,sku_without_merv_rating
        ,min_production_run_per_size
        ,case when sum(production_need_w_min_merv) >= min_production_run_per_size then 1::float
                else 1::float + ((min_production_run_per_size - sum(production_need_w_min_merv))::float / sum(production_need_w_min_merv)::float) end as size_factor
        ,max(case when merv_rating = 'MERV 8'                   then merv_rating_factor end) as merv8_factor
        ,max(case when merv_rating = 'MERV 11'                  then merv_rating_factor end) as merv11_factor
        ,max(case when merv_rating = 'MERV 13'                  then merv_rating_factor end) as merv13_factor
        ,max(case when merv_rating = 'MERV 8 Odor Eliminator'   then merv_rating_factor end) as oe_factor
    from
        (select
            mapped_manufacturing_location
            ,line_type
            ,sku
            ,sku_without_merv_rating
            ,merv_rating
            ,min_production_run_per_size
            ,min_production_run_per_merv_rating
            ,case when sum(production_need_calc) >= min_production_run_per_merv_rating then 1::float
                else 1::float + ((min_production_run_per_merv_rating - sum(production_need_calc))::float / sum(production_need_calc)::float) end as merv_rating_factor
            ,round(sum(production_need_calc)::float * merv_rating_factor) as production_need_w_min_merv
        from base_records
        group by 1,2,3,4,5,6,7
            ,min_production_run_per_merv_rating) t
    group by 1,2,3,4
)

select
    br.runtime_dt_utc
    ,br.inserted_dt_utc
    ,br.mapped_distribution_location
    ,br.is_copied_fps
    ,br.mapped_manufacturing_location
    ,br.count_of_staffing_over_mfg
    ,br.line_type
    ,br.count_of_lines_over_mfg_line_type
    ,br.sku
    ,br.sku_without_merv_rating
    ,br.merv_rating
    ,round(br.production_need_calc::float *
        case when br.merv_rating = 'MERV 8'                 then mpr.merv8_factor
            when br.merv_rating = 'MERV 11'                 then mpr.merv11_factor
            when br.merv_rating = 'MERV 13'                 then mpr.merv13_factor
            when br.merv_rating = 'MERV 8 Odor Eliminator'  then mpr.oe_factor end
        * mpr.size_factor) as production_need
    ,(production_need::float / br.production_capacity::float)::decimal(10,2) as production_lines
    ,br.production_capacity
    ,br.max_production_lines_per_size
    ,br.min_production_run_per_size
    ,br.min_production_run_per_merv_rating
    ,br.changeover_production_lines_per_size
    ,br.days_of_inventory_remaining
    ,br.ranking
    ,br.filters_per_pallet
    ,br.is_within_capacity
    -- Running Sums --
    /* PARTITION BY() clauses uses :
     *      (1) Rank
     *      (2) Distribution location
     *      (3) MERV rating
     * #2 and #3 are used simply to ensure calculation strategy remains consistent across executions and transformations.
     */
    ,sum(production_lines)
        over (partition by br.mapped_manufacturing_location order by br.ranking asc, br.mapped_distribution_location asc, br.merv_rating asc rows unbounded preceding)
        + coalesce(pp_mfg.prior_lines_over_mfg, 0) as running_sum_staffing_over_mfg
    ,sum(production_lines)
        over (partition by br.mapped_manufacturing_location, br.line_type order by br.ranking asc, br.mapped_distribution_location asc, br.merv_rating asc rows unbounded preceding)
        + coalesce(pp_mfg_line.prior_lines_over_mfg_line, 0) as running_sum_lines_over_mfg_line_type
    ,sum(production_lines)
        over (partition by br.mapped_manufacturing_location, br.sku_without_merv_rating order by br.ranking asc, br.mapped_distribution_location asc, br.merv_rating asc rows unbounded preceding)
        + coalesce(pp_mfg_sku.prior_lines_over_mfg_sku, 0) as running_sum_production_lines_per_size
from base_records br
    join min_production_runs mpr on mpr.mapped_manufacturing_location = br.mapped_manufacturing_location
        and mpr.line_type = br.line_type
        and mpr.sku_without_merv_rating = br.sku_without_merv_rating
    -- Prior production lines by MFG and SKU without MERV rating --
    left join prior_production_lines_over_mfg_sku pp_mfg_sku on pp_mfg_sku.manufacturing_location = br.mapped_manufacturing_location
        and pp_mfg_sku.sku_without_merv_rating = br.sku_without_merv_rating
    -- Prior production lines by MFG and line type --
    left join prior_production_lines_over_mfg_line_type pp_mfg_line on pp_mfg_line.manufacturing_location = br.mapped_manufacturing_location
        and pp_mfg_line.line_type = br.line_type
    -- Prior production lines by MFG --
    left join prior_production_lines_over_mfg pp_mfg on pp_mfg.manufacturing_location = br.mapped_manufacturing_location