-- Job: ps_manufacturing_to_distribution_center_history
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4324198
-- Component ID: 4335521

select
    '${inserted_dt_utc}'::timestamptz as inserted_dt_utc
    ,true as is_selected_dt
    ,mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,mapped_distribution_location
    ,distribution_sb_alias
    ,in_transit_days_to_distribution_center
from ${stg2_schema}.ps_manufacturing_to_distribution_center
where distribution_sb_alias in (${locations_list})