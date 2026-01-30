-- Job: ps_excluded_skus_by_location_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 6312356
-- Component ID: 6312424

select
    trim(upper("sku without merv rating"))                                              as sku_without_merv_rating
    ,replace(regexp_substr("manufacturing location(s)", '"[^"]*"', 1,  i.id), '"', '')  as manufacturing_location
from ${stg1_schema}.ps_excluded_skus_by_location ps
    join ${stg2_schema}.indicies i on i.id between 1 and 10 /* Assuming a maximum of 10 locations exist */
where manufacturing_location != ''