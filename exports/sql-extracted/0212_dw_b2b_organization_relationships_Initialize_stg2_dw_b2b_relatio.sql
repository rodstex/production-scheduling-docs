-- Job: dw_b2b_organization_relationships
-- Path: ROOT/filterbuy_dw/dwh/transformations
-- Component: Initialize stg2.dw_b2b_relationships_logic2
-- Type: SQL Query
-- Job ID: 8704741
-- Component ID: 8710095

/* Return a distinct list of Organization and Supplybuy Links.
 * >1 Persons can be associated with an Organization, so DISTINCT is necessary.
 */
with base_records as (
    select distinct
        po.organization_id
        ,ppcf.value as supplybuy_links
        ,regexp_count(ppcf.value, ',') + 1  as count_of_comma_separated_links
    from ${stg2_schema}.pd_organizations po
        join ${stg2_schema}.pd_persons pp on po.organization_id = pp.organization_id
        join ${stg2_schema}.pd_person_custom_fields ppcf on pp.person_id = ppcf.person_id
            and ppcf.field_name in ('SupplyBuy Person Link')
)

/* Since >1 Persons can be associated with an Organization,
 * only return records where all Persons have the same Supplybuy Link field.
 */
, unique_records as (
    select
        organization_id
    from base_records
    group by 1
    having count(*) = 1
)

/* Parse the Business Customer ID from each 'Supplybuy Link' field.
 * If a comma-separated field exists, create a record for each permutation.
 */
, parsed as (
    select
        br.organization_id
        ,br.supplybuy_links
        ,trim(split_part(br.supplybuy_links, ',', i.id)) as supplybuy_link
        ,case when supplybuy_link ~ 'https://admin\\.supplybuy\\.com/dashboard/customer/[0-9]+'
            then regexp_substr(supplybuy_link, '[0-9]+')::int end as business_customer_id
    from unique_records ur
        join base_records br on br.organization_id = ur.organization_id
        join ${stg2_schema}.indicies i on i.id between 1 and br.count_of_comma_separated_links
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