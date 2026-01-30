-- Job: dw_b2b_organization_relationships
-- Path: ROOT/filterbuy_dw/dwh/transformations
-- Component: Initialize stg2.dw_b2b_relationships_logic1
-- Type: SQL Query
-- Job ID: 8704741
-- Component ID: 8710009

/* Returns all Pipedrive organizations that have the custom field 'Supplybuy Link' populated */
with base_records as (
    select
        po.organization_id
        ,pocf.value                         as supplybuy_links
        ,regexp_count(pocf.value, ',') + 1  as count_of_comma_separated_links
    from ${stg2_schema}.pd_organizations po
        join ${stg2_schema}.pd_organization_custom_fields pocf on po.organization_id = pocf.organization_id
    where pocf.field_name in ('SupplyBuy Link')
)

/* Organizations can have multiple Supplybuy customers linked.
 * Create a record for each Supplybuy customer an Organization is associated with. */
, parsed as (
    select
        organization_id
        ,case when supplybuy_link ~ 'https://admin\\.supplybuy\\.com/dashboard/customer/[0-9]+'
            then regexp_substr(supplybuy_link, '[0-9]+')::int end as business_customer_id
    from
        (select
            br.organization_id
            ,trim(split_part(br.supplybuy_links, ',', i.id)) as supplybuy_link
        from base_records br
            join ${stg2_schema}.indicies i on i.id between 1 and br.count_of_comma_separated_links) t
)

------------------------------------------------------------------------------
-- Return a record for each organization and Supplybuy customer combination --
------------------------------------------------------------------------------
select
    p.organization_id
    ,p.business_customer_id
    ,case when p.business_customer_id is null then false
        else c.business_customer_id is not null end as is_valid_business_customer_id
from parsed p
    left join ${stg2_schema}.sb_dashboard_business_customer c on c.business_customer_id = p.business_customer_id