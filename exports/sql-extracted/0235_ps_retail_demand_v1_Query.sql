-- Job: ps_retail_demand
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4
-- Component: v1 Query
-- Type: SQL Query
-- Job ID: 8791463
-- Component ID: 8791468

/* Mimic the retail shipment page (i.e., https://admin.supplybuy.com/retail/).
 * View is in supplybuy / dashboard / retail_views.py.
 * def retail_shipment_tool()
 *      https://github.com/filterbuy/supplybuy/blob/master/dashboard/retail_views.py#L55
 */
select
    o.id                as business_customer_order_id
    ,o.created_at       as create_dt_utc
    ,o.updated_at       as update_dt_utc
    ,o.status           as order_status
    ,c.id               as business_customer_id
    ,c.complete_name    as customer_name
    ,oi.id              as business_customer_order_item_id
    ,upper(oi.sku)      as sku
    ,oi.quantity
from dashboard_businesscustomer c
    join dashboard_businesscustomerorder o on c.id = o.customer_id
    left join dashboard_businesscustomeraddress a on o.shipping_address_id = a.id
    join dashboard_businesscustomerorderitem oi on o.id = oi.order_id
where
    /* https://github.com/filterbuy/supplybuy/blob/master/dashboard/managers.py#L14 class BusinessCustomerOrderRetailToolManager(models.Manager): */
    o.origin in ('retail_tool')
    and o.id not in (68515) -- Exclude test order