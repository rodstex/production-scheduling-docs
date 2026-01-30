-- Job: ps_filters_per_pallet_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4319038
-- Component ID: 4319087

with base_records as (
    select
        trim(upper("sku without merv rating")) as sku_without_merv_rating
        ,"filters per full pallet"::int as filters_per_pallet
    from ${stg1_schema}.ps_filters_per_pallet
)

select
    sku_without_merv_rating
    ,filters_per_pallet
    ,row_number() over (partition by sku_without_merv_rating order by filters_per_pallet asc) as rn
from base_records