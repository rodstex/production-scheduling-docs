-- Job: ps_projected_b2b_demand_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: NEW SQL
-- Type: SQL Query
-- Job ID: 7560226
-- Component ID: 9411181

/* Identify the latitude and longitude of every Filterbuy distribution location */
with dim_business_location as (
    select distinct
        case when dbl.name like 'Talladega%' then 'Talladega, AL'
            else dbl.name end as mapped_location_name
        ,dbl.postal_code    as origin_dim_msa_postal_code
        ,case when dbl.name = 'Toronto, ON' then 43.6532
            else pc.latitude end as origin_lat
        ,case when dbl.name = 'Toronto, ON' then -79.3832
            else pc.longitude end as origin_lon
    from ${dwh_schema}.dim_business_location dbl
        join ${stg1_schema}.gs_us_postal_codes pc on trim(to_char(pc.zip, '00000')) = dbl.postal_code
    where dbl.postal_code is not null
)

/* Identify the closest Filterbuy distribution location to every postal code in the US. */
, location_attribution as (
    select
        trim(to_char(br.zip, '00000')) as dest_postal_code
        ,round(3959 * ACOS(
            LEAST(
                1.0,
                COS(RADIANS(dallas.origin_lat)) * COS(RADIANS(br.latitude)) *
                COS(RADIANS(br.longitude) - RADIANS(dallas.origin_lon)) +
                SIN(RADIANS(dallas.origin_lat)) * SIN(RADIANS(br.latitude))
            )
        )) AS dallas_distance_miles
        ,round(3959 * ACOS(
            LEAST(
                1.0,
                COS(RADIANS(elgin.origin_lat)) * COS(RADIANS(br.latitude)) *
                COS(RADIANS(br.longitude) - RADIANS(elgin.origin_lon)) +
                SIN(RADIANS(elgin.origin_lat)) * SIN(RADIANS(br.latitude))
            )
        )) AS elgin_distance_miles
        ,round(3959 * ACOS(
            LEAST(
                1.0,
                COS(RADIANS(fresno.origin_lat)) * COS(RADIANS(br.latitude)) *
                COS(RADIANS(br.longitude) - RADIANS(fresno.origin_lon)) +
                SIN(RADIANS(fresno.origin_lat)) * SIN(RADIANS(br.latitude))
            )
        )) AS fresno_distance_miles
        ,round(3959 * ACOS(
            LEAST(
                1.0,
                COS(RADIANS(new_kensington.origin_lat)) * COS(RADIANS(br.latitude)) *
                COS(RADIANS(br.longitude) - RADIANS(new_kensington.origin_lon)) +
                SIN(RADIANS(new_kensington.origin_lat)) * SIN(RADIANS(br.latitude))
            )
        )) AS new_kensington_distance_miles
        ,round(3959 * ACOS(
            LEAST(
                1.0,
                COS(RADIANS(ogden.origin_lat)) * COS(RADIANS(br.latitude)) *
                COS(RADIANS(br.longitude) - RADIANS(ogden.origin_lon)) +
                SIN(RADIANS(ogden.origin_lat)) * SIN(RADIANS(br.latitude))
            )
        )) AS ogden_distance_miles
        ,round(3959 * ACOS(
            LEAST(
                1.0,
                COS(RADIANS(orlando.origin_lat)) * COS(RADIANS(br.latitude)) *
                COS(RADIANS(br.longitude) - RADIANS(orlando.origin_lon)) +
                SIN(RADIANS(orlando.origin_lat)) * SIN(RADIANS(br.latitude))
            )
        )) AS orlando_distance_miles
        ,round(3959 * ACOS(
            LEAST(
                1.0,
                COS(RADIANS(talladega.origin_lat)) * COS(RADIANS(br.latitude)) *
                COS(RADIANS(br.longitude) - RADIANS(talladega.origin_lon)) +
                SIN(RADIANS(talladega.origin_lat)) * SIN(RADIANS(br.latitude))
            )
        )) AS talladega_distance_miles
        ,round(3959 * ACOS(
            LEAST(
                1.0,
                COS(RADIANS(toronto.origin_lat)) * COS(RADIANS(br.latitude)) *
                COS(RADIANS(br.longitude) - RADIANS(toronto.origin_lon)) +
                SIN(RADIANS(toronto.origin_lat)) * SIN(RADIANS(br.latitude))
            )
        )) AS toronto_distance_miles
        ,least(
            dallas_distance_miles
            ,elgin_distance_miles
            ,fresno_distance_miles
            ,new_kensington_distance_miles
            ,ogden_distance_miles
            ,orlando_distance_miles
            ,talladega_distance_miles
            ,toronto_distance_miles
        ) as shortest_distance_miles
        ,case when shortest_distance_miles = dallas_distance_miles          then dallas.mapped_location_name
            when shortest_distance_miles = elgin_distance_miles             then elgin.mapped_location_name
            when shortest_distance_miles = fresno_distance_miles            then fresno.mapped_location_name
            when shortest_distance_miles = new_kensington_distance_miles    then new_kensington.mapped_location_name
            when shortest_distance_miles = ogden_distance_miles             then ogden.mapped_location_name
            when shortest_distance_miles = orlando_distance_miles           then orlando.mapped_location_name
            when shortest_distance_miles = talladega_distance_miles         then talladega.mapped_location_name
            when shortest_distance_miles = toronto_distance_miles           then toronto.mapped_location_name
        end as route_to_location
    from ${stg1_schema}.gs_us_postal_codes br
        join dim_business_location dallas           on dallas.mapped_location_name = 'Dallas, TX'
        join dim_business_location elgin            on elgin.mapped_location_name = 'Elgin, IL'
        join dim_business_location fresno           on fresno.mapped_location_name = 'Fresno, CA'
        join dim_business_location new_kensington   on new_kensington.mapped_location_name = 'New Kensington, PA'
        join dim_business_location ogden            on ogden.mapped_location_name = 'Ogden, UT'
        join dim_business_location orlando          on orlando.mapped_location_name = 'Orlando, FL'
        join dim_business_location talladega        on talladega.mapped_location_name = 'Talladega, AL'
        join dim_business_location toronto          on toronto.mapped_location_name = 'Toronto, ON'
)

/* Base records for customer orders.
 * Filtered to:
 *      (1) Products that are scheduled using production scheduling tool
 *      (2) Non-subscription orders
 *      (3) B2B orders
 *      (4) Orders from prior 2 years
 * Aggregate records to 1 record per customer, product, route to location, and day.
 * Prevents dirty data when >1 order placed on the same day.
 * E.g., business_customer_id 423368, sku = AFB20x24x2M1.
 */
, base_records as (
    select
        dc.dim_customer_id
        ,c.business_customer_id /* QA only */
        ,di.sku
        ,dc.cart_created_dt_ct::date        as order_dt
        ,dc.dim_msa_postal_code             as dest_postal_code
        ,case when loc.route_to_location = 'Talladega, AL' and di.filter_type in ('4" Pleated', 'Whole-House') then 'Talladega, AL (Newberry)'
            when loc.route_to_location = 'Talladega, AL' and di.filter_type in ('1" Pleated', '2" Pleated') then 'Talladega, AL (Pope)'
            else loc.route_to_location end  as route_to_location
        ,sum(fst.quantity * fi.pack_size)   as gross_units_sold
        ,sum(fst.price_usd)                 as gross_product_revenue
    from
        (select distinct
            sku_without_merv_rating
        from ${stg2_schema}.ps_capacity_by_sku) cap
        join ${dwh_schema}.dim_item di on di.sku_without_merv_rating = cap.sku_without_merv_rating
        join ${dwh_schema}.fact_item fi on di.dim_item_id = fi.dim_item_id
        join ${dwh_schema}.fact_sales_transaction fst on fi.fact_item_id = fst.fact_item_id
        join ${dwh_schema}.dim_cart dc on fst.generic_order_id = dc.generic_order_id
        join ${dwh_schema}.dim_customer c on dc.dim_customer_id = c.dim_customer_id
        join location_attribution loc on loc.dest_postal_code = dc.dim_msa_postal_code
    where true
        and fst.is_included_in_total_income     /* Is included in total income */
        and di.is_product                       /* Is product */
        and fst.fact_subscription_id is null    /* Not subscription */
        and dc.marketplace in ('wholesale')     /* B2B order */
        and dc.cart_created_dt_ct::date between /* Sales in last 2 years */
            (current_timestamp at time zone 'America/Chicago' - interval '731 day')::date
            and (current_timestamp at time zone 'America/Chicago')::date
        and dc.dim_msa_postal_code is not null  /* Shipping destination is known */
        and loc.route_to_location is not null   /* Route to location is known */
    group by 1,2,3,4,5,6
)

/* Truncate all records from base_records table to instances where an order has occurred >= N times.
 * Primary key will be: customer, SKU, and route to location.
 */
, unique_identifier as (
    select
        dim_customer_id
        ,sku
        ,route_to_location
        ,dim_customer_id::text || sku || route_to_location  as unique_key
        ,count(*)                                           as count_of_orders
        ,avg(gross_units_sold)                              as avg_gross_units_sold
    from base_records
    where route_to_location is not null /* For safety only */
    group by 1,2,3,4
    having count(*) >= 3 /* Customer has place >= 3 orders for unique key */
)

/* Transform records.
 * Truncate to records that match unique identifier logic.
 * Identify facts about each order, including first order, most recent order,
 *  days between orders, and the bin size between orders.
 * Bin size between orders allows orders with similar frequencies to be collected together.
 * E.g., a combination of customer, SKU, and route to location was ordered at following intervals: 88, 91, 87, 90, & 92.
 *  Group orders into a 90-day interval bin.
 */
, transformed as (
    select
        ui.dim_customer_id
        ,ui.sku
        ,ui.route_to_location
        ,ui.unique_key
        ,br.order_dt
        ,br.gross_units_sold
        ,ui.avg_gross_units_sold
        ,row_number() over (partition by ui.dim_customer_id, ui.sku, ui.route_to_location order by br.order_dt asc) = 1     as is_first_order
        ,row_number() over (partition by ui.dim_customer_id, ui.sku, ui.route_to_location order by br.order_dt desc) = 1    as is_most_recent_order
        ,lag(order_dt) over (partition by ui.dim_customer_id, ui.sku, ui.route_to_location order by br.order_dt asc)        as prior_order_dt
        ,datediff('day', prior_order_dt, br.order_dt)   as days_from_prior_order
        ,floor(days_from_prior_order / 15) * 15         as bin_start
        ,bin_start + 15 - 1                             as bin_end
        ,(bin_end - bin_start) / 2 + bin_start          as bin_middle
    from unique_identifier ui
        join base_records br on br.dim_customer_id = ui.dim_customer_id
            and br.route_to_location = ui.route_to_location
            and br.sku = ui.sku
)

/* Identify the most common bin for each unique key. */
, binning as (
    select
        t.dim_customer_id
        ,t.sku
        ,t.route_to_location
        ,t.unique_key
        ,t.bin_start
        ,t.bin_middle
        ,t.bin_end
        ,t1.count_of_orders_for_unique_key
        ,count(t.dim_customer_id)                                   as count_of_orders_in_bin
        ,count(t.dim_customer_id)::float
            / t1.count_of_orders_for_unique_key::float              as perc_of_orders
        ,row_number() over (partition by t.dim_customer_id, t.sku, t.route_to_location
            order by count_of_orders_in_bin desc)                   as rn
        ,rn = 1                                                     as is_most_common_interval
        ,case when rn = 1 and count(*) = 1 then true else false end as is_most_common_interval_has_one_instance
    from transformed t
        join
            (select
                dim_customer_id
                ,sku
                ,route_to_location
                ,count(*) as count_of_orders_for_unique_key
            from transformed
            group by 1,2,3) t1 on t.dim_customer_id = t1.dim_customer_id
                and t.route_to_location = t1.route_to_location
                and t.sku = t1.sku
    where not(t.is_first_order)
    group by 1,2,3,4,5,6,7
        ,t1.count_of_orders_for_unique_key
)

/* Filter to records that:
 *      (1) Have a consistent ordering pattern
 *      (2) Have an estimated next order date in the future
 */
, results as (
    select
        b.dim_customer_id
        ,b.sku
        ,b.route_to_location
        ,b.unique_key
        ,t.avg_gross_units_sold
        ,a.avg_unit_price
        ,b.count_of_orders_for_unique_key
        ,b.count_of_orders_in_bin
        ,b.bin_start
        ,b.bin_middle
        ,b.bin_end
        ,b.perc_of_orders                                       as percent_of_orders_in_bin_size
        ,t.order_dt                                             as most_recent_order_dt
        ,date_add('day', b.bin_middle::int, t.order_dt)::date   as estimated_next_order_dt
        ,estimated_next_order_dt >
            (current_timestamp at time zone 'America/Chicago')::date as is_estimated_order_dt_in_future
    from binning b
        join transformed t on t.dim_customer_id = b.dim_customer_id
            and t.sku = b.sku
            and t.route_to_location = b.route_to_location
            and t.is_most_recent_order
        join
            (select
                dim_customer_id
                ,sku
                ,round(sum(gross_product_revenue)::float
                    / sum(gross_units_sold)::float,2) as avg_unit_price
            from base_records
            group by 1,2) a on a.dim_customer_id = b.dim_customer_id
                and a.sku = b.sku
    where true
        and b.is_most_common_interval                       /* Is most common ordering interval */
        and not(b.is_most_common_interval_has_one_instance) /* Most common ordering interval has more than on order */
        and b.perc_of_orders >= 0.3                         /* Ordering interval accounts for 30% or more of orders */
        and estimated_next_order_dt >
            (current_timestamp at time zone 'America/Chicago')::date /* Order is in the future */
        and b.bin_end > 28                                  /* Ordering interval is greater than the planning period for the production scheduling tool */
)

select
    date_trunc('week', r.estimated_next_order_dt)::date as estimated_order_week
    ,r.route_to_location
    ,dbl.sb_location_name
    ,r.sku
    ,sum(r.avg_gross_units_sold)  as estimated_order_quantity
    ,count(distinct r.unique_key) as estimated_orders
from results r
    join
        (select distinct
            name as mapped_location_name
            ,shops_location_alias as sb_location_name
        from ${stg2_schema}.gs_dim_business_location) dbl on dbl.mapped_location_name = r.route_to_location
where lower(dbl.sb_location_name) in (${locations_list})
group by 1,2,3,4
having sum(r.avg_gross_units_sold) > 200