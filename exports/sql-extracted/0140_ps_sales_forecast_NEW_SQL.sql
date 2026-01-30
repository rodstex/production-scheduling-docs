-- Job: ps_sales_forecast
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: NEW SQL
-- Type: SQL Query
-- Job ID: 4309081
-- Component ID: 9411269

/* Return a record for every inventory change and missed opportunity.
 * Logic is taken from new_production().
 * https://github.com/supplybuy/supplybuy/blob/master/shops/views.py#L13484.
 * Supplybuy only includes following model types: tmsordershipment and nonstandardfiltersusage.
 */
with base_sales as (
    select
        'sb_shops_pleated_filter_inventory_change'  as source
        ,ic.pleated_filter_inventory_change_id      as id
        ,ic.created_dt_utc                          as dt
        ,lower(i.location_name)                     as sb_location_alias
        ,trim(upper(i.sku))                         as sku
        ,ic.quantity * -1                           as quantity /* Adjustment types in question are negative, so change to positive here */
    from ${stg2_schema}.sb_shops_pleated_filter_inventory i
        join ${stg2_schema}.sb_shops_pleated_filter_inventory_change ic on i.pleated_filter_inventory_id = ic.pleated_filter_inventory_id
    where ic.adjustment_type in
        ('amazontodayshipmentitem'
        ,'nonstandardfiltersusage'
        ,'tmsordershipment')
        and ic.created_dt_utc >= current_timestamp - interval '28 day' /* Event took place in prior 28 days */
        and ic.quantity < 0
        and lower(i.location_name) in (${locations_list})
    union all

    /* MISSED OPPORTUNITIES
     * Logic is taken from update_production_missed_opportunities().
     * https://github.com/supplybuy/supplybuy_bixly/blob/master/shops/tasks.py#L7590 */
    select
        'sb_shops_production_missed_opportunity'    as source
        ,mo.production_missed_opportunity_id        as id
        ,mo.created_dt_utc                          as dt
        ,lower(mol.location_name)                   as sb_location_alias
        ,trim(upper(mo.sku))                        as sku
        ,mo.quantity
    from ${stg2_schema}.sb_shops_production_missed_opportunity mo
        join ${stg2_schema}.sb_shops_production_missed_opportunity_locations mol on mo.production_missed_opportunity_id = mol.production_missed_opportunity_id
    where mo.created_dt_utc >= current_timestamp - interval '28 day' /* Event took place in prior 28 days */
  		and lower(mol.location_name) in (${locations_list})
)

, sales as (
    select
        sb_location_alias
        ,sku
        ,(sum(quantity)::decimal(10,2) / 28::decimal(10,2))::decimal(10,2) as daily_sales_avg_rolling_28_day
        /* Using ROUND() matches logic in Supplybuy
        ,round(sum(quantity) / 28) as daily_sales_avg_rolling_28_day
         */
    from base_sales
    group by 1,2
)

, results as (
    select
        case when br.sb_location_alias = 'chicago'     then 'Elgin, IL'
            when br.sb_location_alias = 'dallas'       then 'Dallas, TX'
            when br.sb_location_alias = 'fresno'       then 'Fresno, CA'
            when br.sb_location_alias = 'newberry'     then 'Talladega, AL (Newberry)'
            when br.sb_location_alias = 'orlando'      then 'Orlando, FL'
            when br.sb_location_alias = 'pittsburgh'   then 'New Kensington, PA'
            when br.sb_location_alias = 'pope'         then 'Talladega, AL (Pope)'
            when br.sb_location_alias = 'slc'          then 'Ogden, UT'
            when br.sb_location_alias = 'retail'       then 'Talladega, AL (Retail)'
            when br.sb_location_alias = 'toronto'      then 'Toronto, ON'
            end as mapped_location_name
        ,br.sb_location_alias as sb_location_name
        ,br.sku
        ,dd.dt
        ,br.daily_sales_avg_rolling_28_day as quantity
        ,sum(quantity) over (partition by br.sb_location_alias, br.sku order by dd.dt asc rows unbounded preceding) as rolling_sum_quantity
        ,br.daily_sales_avg_rolling_28_day
        ,br.daily_sales_avg_rolling_28_day * 7                                                                                              as weekly_sales_avg_rolling_28_day
    from sales br
        join ${dwh_schema}.dim_date dd on dd.dt between current_date::date and (current_date + interval '90 day')
        join ${stg2_schema}.dw_sku_to_dim_item_id dsdi on dsdi.sku = br.sku
            and dsdi.rn = 1
        join ${dwh_schema}.dim_item di on di.dim_item_id = dsdi.dim_item_id
        join ${stg2_schema}.ps_filter_types_history pfth on pfth.is_selected_dt
            and pfth.filter_type = di.filter_type
)

, final as (
    select
        mapped_location_name
        ,sb_location_name
        ,sku
        ,dt
        ,quantity
        ,rolling_sum_quantity
        ,daily_sales_avg_rolling_28_day
        ,weekly_sales_avg_rolling_28_day
    from results
    union all
    /* Add in demand from projected B2B demand (DATA-596) */
    select
        case when sb_location_name = 'chicago'     then 'Elgin, IL'
            when sb_location_name = 'dallas'       then 'Dallas, TX'
            when sb_location_name = 'fresno'       then 'Fresno, CA'
            when sb_location_name = 'newberry'     then 'Talladega, AL (Newberry)'
            when sb_location_name = 'orlando'      then 'Orlando, FL'
            when sb_location_name = 'pittsburgh'   then 'New Kensington, PA'
            when sb_location_name = 'pope'         then 'Talladega, AL (Pope)'
            when sb_location_name = 'slc'          then 'Ogden, UT'
            when sb_location_name = 'retail'       then 'Talladega, AL (Retail)'
            when sb_location_name = 'toronto'      then 'Toronto, ON'
            end as mapped_location_name
        ,sb_location_name
        ,sku
        ,estimated_order_week as dt
        ,estimated_order_quantity as quantity
        ,0 as rolling_sum_quantity
        ,0 as daily_sales_avg_rolling_28_day
        ,0 as weekly_sales_avg_rolling_28_day
    from ${stg2_schema}.ps_projected_b2b_demand
    where sb_location_name in (${locations_list})
    union all
    /* Add in demand from projected retail demand (DATA-180) */
    select
        'Talladega, AL (Pope)' as mapped_location_name -- Arbitrarily assign to Pope
        ,'pope' as sb_location_name
        ,sku
        ,dt
        ,quantity
        ,0 as rolling_sum_quantity
        ,0 as daily_sales_avg_rolling_28_day
        ,0 as weekly_sales_avg_rolling_28_day
    from ${stg2_schema}.ps_projected_retail_demand
    where 'pope' in (${locations_list})
)

select
    mapped_location_name
    ,sb_location_name
    ,sku
    ,dt
    ,sum(quantity)                          as quantity
    ,sum(rolling_sum_quantity)              as rolling_sum_quantity
    ,sum(daily_sales_avg_rolling_28_day)    as daily_sales_avg_rolling_28_day
    ,sum(weekly_sales_avg_rolling_28_day)   as weekly_sales_avg_rolling_28_day
from final
group by 1,2,3,4