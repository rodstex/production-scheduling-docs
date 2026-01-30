-- Job: ps_qa_missing_inputs
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/quality_assurance
-- Component: Error #1
-- Type: SQL Query
-- Job ID: 4341443
-- Component ID: 4346217

select distinct
    'Production capacities not input for size '|| psfh.sku_without_merv_rating as error_type
from ${stg2_schema}.ps_sales_forecast_history psfh
    left join ${stg2_schema}.ps_capacity_by_sku_history pcbsh on pcbsh.is_selected_dt
        and pcbsh.sku_without_merv_rating = psfh.sku_without_merv_rating
    join ${stg2_schema}.ps_fact_manufacturing_need_history fmnh on fmnh.is_selected_dt
        and fmnh.sku = psfh.sku
where psfh.is_selected_dt
    and pcbsh.sku_without_merv_rating is null
    and not(fmnh.is_custom) /* DATA-520 */