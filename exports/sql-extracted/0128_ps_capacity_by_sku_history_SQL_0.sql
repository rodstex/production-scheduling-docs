-- Job: ps_capacity_by_sku_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324208
-- Component ID: 4335974

select
    '${inserted_dt_utc}'::timestamptz   as inserted_dt_utc
    ,true                               as is_selected_dt
    ,sku_without_merv_rating
    ,line_type
    ,mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,production_capacity
    ,auto_tooling_sets
    /* Rank each SKU without MERV rating and non-automated line type production capacity.
     * If an automated line, return null.
     * Order by 
     *      (1) production capacity (descending)
     *      (2) line type (descending)
     * Line type is used so that, if Single and Double loader lines are tied in efficiency, rank Single loader higher.
     * Double loader lines can make more products than Single loader lines can, so use Single loader lines as much as possible.
     */
    ,case when line_type = 'Automated' then null
        else row_number() over (partition by mapped_manufacturing_location, sku_without_merv_rating order by production_capacity desc, line_type desc)
        - max(case when line_type = 'Automated' then 1 else 0 end) over (partition by mapped_manufacturing_location, sku_without_merv_rating) end as non_automated_efficiency_rank
from ${stg2_schema}.ps_capacity_by_sku