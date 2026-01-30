-- Job: dw_b2b_organization_relationships_errors_trans
-- Path: ROOT/filterbuy_dw/dwh/transformations
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 8725411
-- Component ID: 8727666

/* Identify all organizations with won deals */
with won_deals as (
    select distinct
        pd.organization_id
    from ${stg2_schema}.pd_deals pd
        join ${stg2_schema}.pd_pipelines pp on pd.pipeline_id = pp.pipeline_id
        join ${stg2_schema}.pd_stages ps on pd.pipeline_id = ps.pipeline_id
            and pd.stage_id = ps.stage_id
    where ps.name in ('6 Deal Won')
)

select
    po.organization_id
    ,null::int as business_customer_id
    ,'Organization with won deal(s) and not linked to a Supplybuy Customer' as error_description
from ${stg2_schema}.pd_organizations po
    join won_deals wd on wd.organization_id = po.organization_id
    left join ${stg2_schema}.dw_b2b_organization_relationships r on r.organization_id = po.organization_id
where r.organization_id is null
union all

select
    organization_id
    ,business_customer_id
    ,'Business Customer ID not valid' as error_description
from ${stg2_schema}.dw_b2b_relationships_logic1
where not(is_valid_business_customer_id)
union all

select
    t0.organization_id
    ,t.business_customer_id
    ,'Multiple Organizations linked to Supplybuy Customer' as error_description
from
    (select
        business_customer_id
    from ${stg2_schema}.dw_b2b_relationships_logic1
    where is_valid_business_customer_id
    group by 1
    having count(*) > 1) t
    join ${stg2_schema}.dw_b2b_relationships_logic1 t0 on t0.business_customer_id = t.business_customer_id
        and t0.is_valid_business_customer_id