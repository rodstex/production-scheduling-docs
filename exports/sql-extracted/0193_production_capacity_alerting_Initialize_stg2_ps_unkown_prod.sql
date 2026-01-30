-- Job: production_capacity_alerting
-- Path: ROOT/filterbuy_dw/dwh/orchestrations
-- Component: Initialize stg2.ps_unkown_production_capacity
-- Type: SQL Query
-- Job ID: 6297333
-- Component ID: 6297365

with base_records as (
    select
        fm.manufacture_dt_lt::date              as dt
        ,dbl.name                               as manufacturing_location
        ,coalesce(fm.line_type, 'Not Defined')  as line_type
        ,di.sku_without_merv_rating
        ,fm.line_number                         as production_line_id
        ,sum(fm.quantity)                       as quantity_manufactured
    from dwh.fact_manufacturing fm
        join dwh.dim_business_location dbl on fm.business_location_id = dbl.business_location_id
        join dwh.dim_item di on fm.dim_item_id = di.dim_item_id
        join
            (select distinct inserted_dt_utc::date as dt
            from stg2.ps_capacity_by_sku_history) cap_dt on cap_dt.dt = fm.manufacture_dt_lt::date
    where fm.manufacture_dt_lt::date >= (current_timestamp at time zone 'America/Chicago' - interval '7 day')::date /* Date is in prior week */
        and fm.manufacture_dt_lt::date < (current_timestamp at time zone 'America/Chicago')::date /* Exclude today's data */
        and di.filter_type in ('1" Pleated', '2" Pleated', '4" Pleated', 'Whole-House')
        and dbl.name in
            ('New Kensington, PA'
            ,'Talladega, AL (Pope)', 'Talladega, AL (Woodland)', 'Talladega, AL (Newberry)', 'Talladega, AL (TMS)'
            ,'Ogden, UT')
        and fm.line_type in ('Automated','Single Loader','Double Loader','Manual')
    group by 1,2,3,4,5
)

, production_capacities as (
    select
        dd.dt
        ,bd.mapped_manufacturing_location
        ,bd.line_type
        ,bd.sku_without_merv_rating
        /* DATA-567 */
        ,coalesce(cap.production_capacity
            ,lag(cap.production_capacity) ignore nulls over (partition by bd.mapped_manufacturing_location
                ,bd.line_type
                ,bd.sku_without_merv_rating
                order by dd.dt asc)) as production_capacity
    from dwh.dim_date dd
        join
            (select distinct
                mapped_manufacturing_location
                ,sku_without_merv_rating
                ,line_type
            from stg2.ps_capacity_by_sku_history) bd on true
        left join stg2.ps_capacity_by_sku_history cap on cap.inserted_dt_utc::date = dd.dt
            and cap.mapped_manufacturing_location = bd.mapped_manufacturing_location
            and cap.line_type = bd.line_type
            and cap.sku_without_merv_rating = bd.sku_without_merv_rating
    where dd.dt >= (current_timestamp at time zone 'America/Chicago' - interval '7 day')::date /* Date is in prior week */
        and dd.dt < (current_timestamp at time zone 'America/Chicago')::date /* Exclude today's data */
)

select
    br.dt
    ,br.manufacturing_location
    ,br.line_type
    ,replace(br.sku_without_merv_rating, 'X', 'x') as sku
    ,br.production_line_id
    ,br.quantity_manufactured
    ,v4_cap.production_capacity
    ,sum(br.quantity_manufactured) over (partition by br.manufacturing_location, br.line_type, br.sku_without_merv_rating) as quantity_manufactured_over_week
from base_records br
    left join production_capacities v4_cap on v4_cap.dt = br.dt
        and v4_cap.line_type = br.line_type
        and v4_cap.sku_without_merv_rating = br.sku_without_merv_rating
        and v4_cap.mapped_manufacturing_location = br.manufacturing_location
where true
    and v4_cap.production_capacity is null
order by 7 desc