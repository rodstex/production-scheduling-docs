-- Job: ps_projected_retail_demand
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 8849299
-- Component ID: 8849518

/* Base records for retail / FBA demand */
with base_records as (
    select
        business_customer_id
        ,case when business_customer_id in (1253281) then 'walmart'
            when business_customer_id in (1333696) then 'amazon_fba' end as customer_name
        ,business_customer_order_id
        ,create_dt_utc::date                as order_dt
        ,date_trunc('week', order_dt)::date as order_wk
        ,case when sku in ('12X12X1M8','10X10X1M8','14X14X1M8','14X14X1M11') -- Fix Amazon SKU formatting
            then 'AFB' || sku
            else sku end    as sku
        ,sum(quantity)      as quantity
    from ${stg2_schema}.ps_retail_demand
    where business_customer_id in (1253281, 1333696) /* Truncate to Walmart and Amazon FBA */
    group by 1,2,3,4,5,6
)

/* Identify the time between orders for each customer.
 * Group time delays into weeks as precision is not necessary; week-level accuracy is sufficient.
 */
, base_time_between_orders as (
    select
        customer_name
        ,order_wk
        ,lag(order_wk) over (partition by customer_name order by order_wk)  as prior_order_dt
        ,datediff(days, prior_order_dt, order_wk)                           as days_from_prior_order
    from
        (select distinct
            customer_name
            ,order_wk
        from base_records) t
)

/* Predict when each customer will place an order next.
 * Use (1) the most frequent ordering interval and (2) the most recent order date for each customer to predict.
 * Predicted orders will always be on Monday.
 */
, predicted_order_dt as (
    select
        t0.customer_name
        ,t0.days_from_prior_order as days_between_orders
        ,br.most_recent_order_wk
    from
        (select
            customer_name
            ,days_from_prior_order
            ,count_of_instances
            ,row_number() over (partition by customer_name order by count_of_instances desc, days_from_prior_order asc) as rn_over_customer
        from
            (select
                customer_name
                ,days_from_prior_order
                ,count(*) as count_of_instances
            from base_time_between_orders
            where days_from_prior_order is not null /* Exclude initial order */
            group by 1,2) t) t0
        join
            (select
                customer_name
                ,max(order_wk) as most_recent_order_wk
            from base_records
            group by 1) br on br.customer_name = t0.customer_name
    where rn_over_customer = 1
)

/* As of Oct 2025, Walmart follows a predictable ordering pattern.
 * I.e., they order every Monday and in roughly the same quantity.
 * However, they place many orders / wk.
 * Therefore, use the approach to look at sales from the prior 3 weeks and average the quantity over SKU.
 * Exclude the current week
 */
, walmart_demand as (
    select
        sku
        ,round(sum(quantity)::float
            / 3::float) as avg_quantity_per_wk
    from base_records
    where order_wk between (date_trunc('week', current_timestamp at time zone 'America/Chicago') - interval '21 day')
        and date_trunc('week', current_timestamp at time zone 'America/Chicago' - interval '1 day')
        and customer_name in ('walmart')
    group by 1
    having avg_quantity_per_wk > 50 /* Truncate to a meaningful quantity */
)

/* Identify the five most recent Amazon orders */
, amazon_order_inclusions as (
    select
        customer_name
        ,order_dt
        ,row_number() over (partition by customer_name order by order_dt desc) <= 5 as is_most_recent_five_orders
    from
        (select distinct
            customer_name
            ,order_dt
        from base_records
        where customer_name in ('amazon_fba')) t
)

/* As of Oct 2025, Amazon FBA's ordering pattern is to place large orders infrequently for similar products each order.
 * For this customer, get the average quantity ordered on each order for each SKU.
 */
, amazon_demand as (
    select
        br.sku
        ,round(sum(br.quantity)::float / 5::float) as avg_quantity_per_order
    from amazon_order_inclusions aoi
        join base_records br on br.customer_name = aoi.customer_name
            and br.order_dt = aoi.order_dt
    where aoi.is_most_recent_five_orders
    group by 1
)

/* Use indicies table to create demand for the next 15 orders for each customer.
 * Truncate to dates between today and 90 days from now.
 */
select
    dateadd(day, pod.days_between_orders * i.id, pod.most_recent_order_wk)::date    as dt
   	,'Talladega, AL (Pope)'															as mapped_distribution_location
    ,pod.customer_name
    ,br.business_customer_id
    ,coalesce(amz.sku, wal.sku)                                                     as sku
    ,coalesce(amz.avg_quantity_per_order, wal.avg_quantity_per_wk)                  as quantity
from predicted_order_dt pod
    join ${stg2_schema}.indicies i on i.id between 1 and 15
    left join amazon_demand amz on pod.customer_name in ('amazon_fba')
    left join walmart_demand wal on pod.customer_name in ('walmart')
    left join
        (select distinct 
            customer_name
            ,business_customer_id
        from base_records) br on br.customer_name = pod.customer_name
where dt between (current_timestamp at time zone 'America/Chicago')::date
    and dateadd(day, 90, current_timestamp at time zone 'America/Chicago')::date