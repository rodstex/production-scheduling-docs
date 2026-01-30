-- Job: ps_projected_retail_demand_errors
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: Initialize stg2.ps_projected_retail_demand_errors
-- Type: SQL Query
-- Job ID: 8851474
-- Component ID: 8852685

select
    prd.business_customer_id
    ,prd.customer_name
    ,count(distinct prd.business_customer_order_id) as count_of_orders
    ,sum(prd.quantity) as sum_of_quantity
from ${stg2_schema}.ps_retail_demand prd
    left join ${stg2_schema}.ps_projected_retail_demand pprd on pprd.business_customer_id = prd.business_customer_id
where pprd.customer_name is null
group by 1,2