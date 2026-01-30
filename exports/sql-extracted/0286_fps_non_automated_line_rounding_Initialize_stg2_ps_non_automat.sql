-- Job: fps_non_automated_line_rounding
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/non_automated_line_rounding
-- Component: Initialize stg2.ps_non_automated_line_rounding 00
-- Type: SQL Query
-- Job ID: 4881076
-- Component ID: 5189695

with base_records as (
    select
        inserted_dt_utc
        ,manufacturing_location
        ,line_type
        ,sum(production_lines) + sum(coalesce(changeover_production_lines, 0)) as original_total_lines
    from ${dwh_schema}.fact_production_schedule
    where line_type in ('Single Loader', 'Double Loader', 'Manual')
        and is_within_capacity
        and (is_tomorrow_production_schedule or is_future_production_schedule)
  		and manufacturing_location in (${manufacturing_locations})
    group by 1,2,3
)

/* Aggregate total lines per date and location, and fetch staffing_available */
, total_per_day_location as (
    select
        br.inserted_dt_utc
        ,br.manufacturing_location
        ,ps.staffing_available
        ,sum(br.original_total_lines) as total_lines_per_day_location
    from base_records br
    left join ${stg2_schema}.ps_staffing_by_dt ps
        on ps.dt = br.inserted_dt_utc::date
        and ps.mapped_manufacturing_location = br.manufacturing_location
        and ps.grouped_line_type = 'Non-Automated'
    group by 1,2,3
)

/* Calculate how much adjustment is needed to match staffing_available */
, adjustment_needed as (
    select
        br.inserted_dt_utc
        ,br.manufacturing_location
        ,br.line_type
        ,br.original_total_lines
        ,round(br.original_total_lines) as initially_rounded
        ,round(total.staffing_available - total.total_lines_per_day_location) as adjustment
        ,br.original_total_lines - floor(br.original_total_lines) as decimal_part
        ,row_number() over (partition by br.inserted_dt_utc, br.manufacturing_location order by br.original_total_lines - floor(br.original_total_lines) desc) as rank
    from base_records br
    join total_per_day_location total
        on br.inserted_dt_utc = total.inserted_dt_utc
        and br.manufacturing_location = total.manufacturing_location
)

/* First adjustment to keep values within the line_count limit */
, initial_adjustments as (
    select
        an.inserted_dt_utc
        ,an.manufacturing_location
        ,an.line_type
        ,least(
            case
                when adjustment > 0 and rank <= adjustment then ceil(original_total_lines)
                when adjustment < 0 and rank <= abs(adjustment) then floor(original_total_lines)
                else round(original_total_lines)
            end
            ,coalesce(ps.count_of_lines, 99999) -- Ensure it does not exceed line_count
        ) as adjusted_total_lines
        ,coalesce(ps.count_of_lines, 99999) as max_line_count
    from adjustment_needed an
    left join ${stg2_schema}.ps_line_count_history ps
        on ps.is_selected_dt
        and an.manufacturing_location = ps.mapped_manufacturing_location
        and an.line_type = ps.line_type
)

/* Compute the difference between adjusted lines and staffing_available */
, shortfall_excess as (
    select
        ia.inserted_dt_utc
        ,ia.manufacturing_location
        ,sum(ia.adjusted_total_lines) as adjusted_total_lines
        ,total.staffing_available
        ,total.staffing_available - sum(ia.adjusted_total_lines) as net_difference
    from initial_adjustments ia
    join total_per_day_location total
        on ia.inserted_dt_utc = total.inserted_dt_utc
        and ia.manufacturing_location = total.manufacturing_location
    group by 1,2, total.staffing_available
)

/* Redistribute adjustments so total matches staffing_available */
, final_adjustments as (
    select
        ia.inserted_dt_utc
        ,ia.manufacturing_location
        ,ia.line_type
        ,least(ia.adjusted_total_lines +
            case
                when se.net_difference > 0 -- Need to increase total
                     and row_number() over (partition by ia.inserted_dt_utc, ia.manufacturing_location order by ia.max_line_count - ia.adjusted_total_lines desc) <= se.net_difference
                then 1
                when se.net_difference < 0 -- Need to decrease total
                     and row_number() over (partition by ia.inserted_dt_utc, ia.manufacturing_location order by ia.adjusted_total_lines asc) <= abs(se.net_difference)
                then -1
                else 0
            end
            ,ia.max_line_count
        ) as final_adjusted_total_lines
    from initial_adjustments ia
    join shortfall_excess se
        on ia.inserted_dt_utc = se.inserted_dt_utc
        and ia.manufacturing_location = se.manufacturing_location
)

select
    inserted_dt_utc
    ,manufacturing_location
    ,line_type
    ,final_adjusted_total_lines as adjusted_total_lines
from final_adjustments