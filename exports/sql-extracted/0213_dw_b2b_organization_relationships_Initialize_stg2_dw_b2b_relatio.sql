-- Job: dw_b2b_organization_relationships
-- Path: ROOT/filterbuy_dw/dwh/transformations
-- Component: Initialize stg2.dw_b2b_relationships_logic4
-- Type: SQL Query
-- Job ID: 8704741
-- Component ID: 8760873

/* Return all email addresses associated with a Supplybuy customer.
 */
with base_supplybuy as (
    select
        c.business_customer_id
        ,c.customer_name
        ,trim(lower(c.email_address)) as company_email
        ,coalesce(regexp_count(trim(lower(c.email_address)), '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') > 0
            , false) as is_company_email_valid
        ,trim(lower(con.email_address)) as contact_email
        ,coalesce(regexp_count(trim(lower(con.email_address)), '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') > 0
            , false) as is_contact_email_valid
        ,con.is_main_contact
    from ${stg2_schema}.sb_dashboard_business_customer c
        left join ${stg2_schema}.sb_dashboard_business_customer_contact con on c.business_customer_id = con.business_customer_id
)

/* Return all email addresses associated with a Pipedrive organization.
 */
, base_pipedrive as (
    select distinct
        po.organization_id
        ,po.name as organization_name
        ,pp.person_id
        ,trim(lower(ppc.value)) as email_address
        ,coalesce(regexp_count(trim(lower(ppc.value)), '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') > 0
            , false) as is_contact_email_valid
    from ${stg2_schema}.pd_organizations po
        join ${stg2_schema}.pd_persons pp on po.organization_id = pp.organization_id
        join ${stg2_schema}.pd_person_contacts ppc on pp.person_id = ppc.person_id
            and ppc.contact_type in ('email')
)

/* Join Pipedrive organizations to Supplybuy customers using email address match(es).
 * Indicate the preferred method of joining:
 *      (1) Company email, (2) Main contact email, (3) Any other contact email
 */
, base_records as (
    ------------------------------------------------
    -- Join via Supplybuy's company email address --
    ------------------------------------------------
    select distinct
        1 as preferred_join_order
        ,bp.organization_id
        ,bs.business_customer_id
    from base_pipedrive bp
        join base_supplybuy bs on bs.is_company_email_valid
            and bs.company_email = bp.email_address
    union all
    -----------------------------------------------------
    -- Join via Supplybuy's main contact email address --
    -----------------------------------------------------
    select distinct
        2 as preferred_join_order
        ,bp.organization_id
        ,bs.business_customer_id
    from base_pipedrive bp
    join base_supplybuy bs on bs.is_main_contact
        and bs.is_contact_email_valid
        and bs.contact_email = bp.email_address
    union all
    ------------------------------------------------------------------
    -- Join via any Supplybuy contact email address that isn't main --
    ------------------------------------------------------------------
    select distinct
        3 as preferred_join_order
        ,bp.organization_id
        ,bs.business_customer_id
    from base_pipedrive bp
    join base_supplybuy bs on not(bs.is_main_contact)
        and bs.is_contact_email_valid
        and bs.contact_email = bp.email_address
)

/* Truncate results where a Supplybuy customer is only associated with one Pipedrive organization.
 * Use row_number() to prevent duplicates.
 */
, results as (
    select
        br.business_customer_id
        ,br.organization_id
        ,row_number() over (partition by br.business_customer_id order by br.preferred_join_order) as rn
    from
        (select
            business_customer_id
        from base_records
        group by 1
        having count(distinct organization_id) = 1) t
        join base_records br on br.business_customer_id = t.business_customer_id
)

select
    business_customer_id
    ,organization_id
from results
where rn = 1