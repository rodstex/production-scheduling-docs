-- Job: dw_b2b_organization_relationships_trans
-- Path: ROOT/filterbuy_dw/dwh/transformations
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 8724661
-- Component ID: 8724675

/* Identify the Pipedrive Organization associated with a Supplybuy Customer.
 * The join is made on the 'Supplybuy Link' custom field on the Organization page.
 * Exclude any instances where >1 Organization is associated with one Supplybuy Customer.
 */
with organization_link as (
    select
        t.business_customer_id
        ,t0.organization_id
    from
        (select
            business_customer_id
        from ${stg2_schema}.dw_b2b_relationships_logic1
        where is_valid_business_customer_id
        group by 1
        having count(*) = 1 /* Truncate to Customers only linked to one Organization */
        ) t
        join ${stg2_schema}.dw_b2b_relationships_logic1 t0 on t0.business_customer_id = t.business_customer_id
)

/* Identify the Pipedrive Organization associated with a Supplybuy Customer.
 * The join is made on the 'Supplybuy Link' custom field on the Person page and in turn associating that with an Organization.
 * Exclude any instances where >1 Organization is associated with one Supplybuy Customer.
 * Prior transformation excludes any instances where >1 distinct 'Supplybuy Link' exists amongst Persons linked to an Organization.
 */
, person_link as (
    select
        t.business_customer_id
        ,t0.organization_id
    from
        (
        select
            l2.business_customer_id
        from ${stg2_schema}.dw_b2b_relationships_logic2 l2
            left join organization_link l1 on l1.business_customer_id = l2.business_customer_id
        where l1.business_customer_id is null /* Truncate to organizations where link is not already identified */
            and l2.is_valid_business_customer_id
        group by 1
        having count(l2.organization_id) = 1 /* Truncate to Customers only linked to one Organization */
        ) t
        join ${stg2_schema}.dw_b2b_relationships_logic2 t0 on t0.business_customer_id = t.business_customer_id
)

/* Identify the Pipedrive Organization associated with a Supplybuy Customer.
 * The join is made on normalized address fields.
 * A prior transformation excludes instances where addresses are not distinct.
 */
, address_link as (
    select
        l3.business_customer_id
        ,l3.organization_id
    from ${stg2_schema}.dw_b2b_relationships_logic3 l3
        left join organization_link l1 on l1.business_customer_id = l3.business_customer_id
        left join person_link l2 on l2.business_customer_id = l3.business_customer_id
    where l1.business_customer_id is null /* Truncate to organizations where link is not already identified */
        and l2.business_customer_id is null /* Truncate to organizations where link is not already identified */
)

/* Identify the Pipedrive Organization associated with a Supplybuy Customer.
 * The join is made on email address fields.
 * A prior transformation excludes instances 1 Supplybuy customer is associated with >1 Pipedrive organizations.
 */
, email_address_link as (
    select
        l4.business_customer_id
        ,l4.organization_id
    from ${stg2_schema}.dw_b2b_relationships_logic4 l4
        left join organization_link l1 on l1.business_customer_id = l4.business_customer_id
        left join person_link l2 on l2.business_customer_id = l4.business_customer_id
        left join address_link l3 on l3.business_customer_id = l4.business_customer_id
    where l1.business_customer_id is null /* Truncate to organizations where link is not already identified */
        and l2.business_customer_id is null /* Truncate to organizations where link is not already identified */
        and l3.business_customer_id is null /* Truncate to organizations where link is not already identified */
)

select
    business_customer_id
    ,organization_id
    ,true  as is_linked_via_organization_custom_field
    ,false as is_linked_via_person_custom_field
    ,false as is_linked_via_address
    ,false as is_linked_via_email
from organization_link
union all

select
    business_customer_id
    ,organization_id
    ,false as is_linked_via_organization_custom_field
    ,true  as is_linked_via_person_custom_field
    ,false as is_linked_via_address
    ,false as is_linked_via_email
from person_link
union all

select
    business_customer_id
    ,organization_id
    ,false as is_linked_via_organization_custom_field
    ,false as is_linked_via_person_custom_field
    ,true  as is_linked_via_address
    ,false as is_linked_via_email
from address_link
union all

select
    business_customer_id
    ,organization_id
    ,false as is_linked_via_organization_custom_field
    ,false as is_linked_via_person_custom_field
    ,false as is_linked_via_address
    ,true  as is_linked_via_email
from email_address_link