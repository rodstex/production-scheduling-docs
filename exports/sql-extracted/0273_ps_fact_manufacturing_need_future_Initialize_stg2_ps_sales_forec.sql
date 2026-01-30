-- Job: ps_fact_manufacturing_need_future
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/future_production_need_calc
-- Component: Initialize stg2.ps_sales_forecast_future
-- Type: SQL Query
-- Job ID: 4355882
-- Component ID: 4359782

select
    case when psf.sb_location_name = 'chicago'    then 'Elgin, IL'
        when psf.sb_location_name = 'dallas'       then 'Dallas, TX'
        when psf.sb_location_name = 'fresno'       then 'Fresno, CA'
        when psf.sb_location_name = 'newberry'     then 'Talladega, AL (Newberry)'
        when psf.sb_location_name = 'orlando'      then 'Orlando, FL'
        when psf.sb_location_name = 'pittsburgh'   then 'New Kensington, PA'
        when psf.sb_location_name = 'pope'         then 'Talladega, AL (Pope)'
        when psf.sb_location_name = 'slc'          then 'Ogden, UT'
        when psf.sb_location_name = 'retail'       then 'Talladega, AL (Retail)' 
        when psf.sb_location_name = 'toronto'	   then 'Toronto, ON' end as mapped_location_name
    ,psf.sb_location_name
    ,psf.sku
    ,psf.dt
    ,psf.quantity
    ,sum(psf.quantity) over (partition by psf.sb_location_name, psf.sku order by psf.dt asc rows unbounded preceding) as rolling_sum_quantity
    ,psf.daily_sales_avg_rolling_28_day
    ,psf.weekly_sales_avg_rolling_28_day
from ${stg2_schema}.ps_sales_forecast_history psf
    join
        (select distinct
            inserted_dt_utc
            ,location_name
        from ${stg2_schema}.ps_fact_manufacturing_need_future
        where is_selected_dt) fmn on fmn.location_name = psf.sb_location_name
            and psf.dt >= fmn.inserted_dt_utc::date
where psf.is_selected_dt