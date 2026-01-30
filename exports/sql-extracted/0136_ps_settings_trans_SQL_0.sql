-- Job: ps_settings_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4320111
-- Component ID: 4320227

with base_records as (
    --------------------------------------------
    -- Excess inventory % of target inventory --
    --------------------------------------------
    select
        (replace(trim(lower(setting)), '%', '')::float / 100::float)::decimal(10,2) as excess_inventory_perc_of_target_inventory
        ,null::int as is_deduct_excess_distribution_from_prod_need
        ,null::int as day_of_week_int_line_capacity_set
        ,null::int as day_of_week_int_auto_schedule_set
        ,null::int as is_schedule_auto_sizes_to_non_auto_lines
        ,null::int as non_automated_proactive_minimum_production_run
    from ${stg1_schema}.ps_settings
    where lower(variable) like '%excess inventory is%'
        and lower(variable) like '%of target inventory%'
    union all
    --------------------------------
    -- Deduct excess distribution --
    --------------------------------
    select
        null::decimal(10,2) as excess_inventory_perc_of_target_inventory
        ,case when trim(lower(setting)) = 'no' then 0
            when trim(lower(setting)) = 'yes' then 1 end as is_deduct_excess_distribution_from_prod_need
        ,null::int as day_of_week_int_line_capacity_set
        ,null::int as day_of_week_int_auto_schedule_set
        ,null::int as is_schedule_auto_sizes_to_non_auto_lines
        ,null::int as non_automated_proactive_minimum_production_run
    from ${stg1_schema}.ps_settings
    where variable = 'Deduct Excess Distribution from production need'
        and trim(lower(setting)) in ('no', 'yes')
    union all
    --------------------------------------------
    -- Day where production lines per day set --
    --------------------------------------------
    select
        null::decimal(10,2) as excess_inventory_perc_of_target_inventory
        ,null::int as is_deduct_excess_distribution_from_prod_need
        ,case when trim(lower(setting)) = 'monday'  then 1
            when trim(lower(setting)) = 'tuesday'   then 2
            when trim(lower(setting)) = 'wednesday' then 3
            when trim(lower(setting)) = 'thursday'  then 4
            when trim(lower(setting)) = 'friday'    then 5
            when trim(lower(setting)) = 'saturday'  then 6
            when trim(lower(setting)) = 'sunday'    then 0 end as day_of_week_int_line_capacity_set
        ,null::int as day_of_week_int_auto_schedule_set
        ,null::int as is_schedule_auto_sizes_to_non_auto_lines
        ,null::int as non_automated_proactive_minimum_production_run
    from ${stg1_schema}.ps_settings
    where variable = 'Day where production lines per day are set'
        and trim(lower(setting)) in ('monday','tuesday','wednesday','thursday','friday','saturday','sunday')
    union all
    -------------------------------------------------------
    -- Day where automated production schedule generated --
    -------------------------------------------------------
     select
        null::decimal(10,2) as excess_inventory_perc_of_target_inventory
        ,null::int as is_deduct_excess_distribution_from_prod_need
        ,null::int as day_of_week_int_line_capacity_set
        ,case when trim(lower(setting)) = 'monday'  then 1
            when trim(lower(setting)) = 'tuesday'   then 2
            when trim(lower(setting)) = 'wednesday' then 3
            when trim(lower(setting)) = 'thursday'  then 4
            when trim(lower(setting)) = 'friday'    then 5
            when trim(lower(setting)) = 'saturday'  then 6
            when trim(lower(setting)) = 'sunday'    then 0 end as day_of_week_int_auto_schedule_set
        ,null::int as is_schedule_auto_sizes_to_non_auto_lines
        ,null::int as non_automated_proactive_minimum_production_run
    from ${stg1_schema}.ps_settings
    where variable = 'Day where automated production schedule is generated'
        and trim(lower(setting)) in ('monday','tuesday','wednesday','thursday','friday','saturday','sunday')
    union all
    -----------------------------------------------------
    -- Schedule automated sizes to non-automated lines --
    -----------------------------------------------------
    select
        null::decimal(10,2) as excess_inventory_perc_of_target_inventory
        ,null::int as is_deduct_excess_distribution_from_prod_need
        ,null::int as day_of_week_int_line_capacity_set
        ,null::int as day_of_week_int_auto_schedule_set
        ,case when trim(lower(setting)) = 'no' then 0
            when trim(lower(setting)) = 'yes' then 1 end as is_schedule_auto_sizes_to_non_auto_lines
        ,null::int as non_automated_proactive_minimum_production_run
    from ${stg1_schema}.ps_settings
    where variable = 'Schedule automated sizes to non-automated lines'
        and trim(lower(setting)) in ('no', 'yes')
    union all
    ------------------------------------------------------------
    -- Non-Automated (Proactive logic) minimum production run --
    ------------------------------------------------------------
    select
        null::decimal(10,2) as excess_inventory_perc_of_target_inventory
        ,null::int as is_deduct_excess_distribution_from_prod_need
        ,null::int as day_of_week_int_line_capacity_set
        ,null::int as day_of_week_int_auto_schedule_set
        ,null::int as is_schedule_auto_sizes_to_non_auto_lines
        ,trim(setting)::int as non_automated_proactive_minimum_production_run
    from ${stg1_schema}.ps_settings
    where variable = 'Non-Automated Proactive production exclude production need less than'
        and regexp_instr(trim(setting), '^[0-9]+$') > 0
)

select
    max(excess_inventory_perc_of_target_inventory)              as excess_inventory_perc_of_target_inventory
    ,max(is_deduct_excess_distribution_from_prod_need)::boolean as is_deduct_excess_distribution_from_prod_need
    ,max(day_of_week_int_line_capacity_set)                     as day_of_week_int_line_capacity_set
    ,max(day_of_week_int_auto_schedule_set)                     as day_of_week_int_auto_schedule_set
    ,max(is_schedule_auto_sizes_to_non_auto_lines)::boolean     as is_schedule_auto_sizes_to_non_auto_lines
    ,max(non_automated_proactive_minimum_production_run)        as non_automated_proactive_minimum_production_run
from base_records