# Production Scheduling Tool (v4)

> Source: Confluence - https://filterbuy.atlassian.net/wiki/spaces/FA/pages/392331266

## Purpose

Filterbuy has a complex supply chain. Multiple manufacturing locations, distribution centers, manufacturing equipment types, and a large variety of products. The production scheduling tool solves this by considering all variables that impact demand, production, and distribution, then using an algorithm to output the ideal manufacturing plan.

## Key Tables

### Output Tables (dwh schema)
- `dwh.fact_manufacturing_need` - Demand for every distribution center and SKU on a given date
- `dwh.fact_production_schedule` - Production and distribution plan output

### Staging Tables (stg2 schema)
All tables with prefix `ps_` are used by the production scheduling tool.

**Audit Tables:**
```sql
stg2.ps_manufacturing_to_distribution_center_history
stg2.ps_target_days_of_inventory_history
stg2.ps_automated_merv_ratings_history
stg2.ps_line_count_history
stg2.ps_staff_by_day_history
stg2.ps_line_facts_history
stg2.ps_capacity_by_sku_history
stg2.ps_filter_per_pallet_history
stg2.ps_reactive_production_loops_history
stg2.ps_settings_history
stg2.ps_filter_types_history
stg2.ps_excluded_skus_by_location_history
stg2.ps_fact_manufacturing_need_history
stg2.ps_sales_forecast_history
stg2.ps_projected_b2b_demand_history
stg2.ps_projected_retail_demand_history
```

**Rollback Tables:**
```sql
stg2.ps_copy_fact_production_schedule
stg2.ps_copy_staffing_by_dt
```

## fact_production_schedule Critical Fields

| Field | Description |
|-------|-------------|
| `inserted_dt_utc` | Date the record is scheduled for production |
| `manufacturing_location` | Location assigned for manufacturing |
| `distribution_location` | Location assigned for distribution |
| `line_type` | Type of line for production (Automated, Non-Automated) |
| `rank_over_manufacturing_location_line_type` | Order rank for manufacturing |
| `sku_without_merv_rating` | Filter size to manufacture |
| `production_goal_merv_*` | Production goals per MERV rating |
| `production_lines` | Number of production lines consumed |
| `is_current_production_schedule` | Scheduled for today |
| `is_tomorrow_production_schedule` | Scheduled for tomorrow |
| `is_within_capacity` | Within estimated production capacity |
| `runtime_dt_utc` | Timestamp when record was generated |

**Important:** Production schedule for today and tomorrow CANNOT change (production lines configured at end of each day).

## Architecture Overview

Main ETL Job: https://matillion.filterbuy.com/#FilterBuy/filterbuy_dw/default/run_production_schedule_v4

### Pipeline Steps

1. **Data Loaders** - Extract from Supplybuy and Google Sheets
2. **Demand Transformations** - Project sales demand per SKU/location
3. **Copies** - Snapshots before/after execution for rollbacks
4. **Generate Production Schedule** - Main algorithm
   - Initialize future production lines
   - Delete future production schedules
   - Excess Distribution
   - Calculate future manufacturing need
   - **Iterate over date(s)** (sub-job: `fps_dt_iteration`)
     - Automated scheduling
     - Non-Automated scheduling
     - Recalculate manufacturing need
   - Reschedule automated products (minimize changeovers)
   - Calculate production lines for upcoming weeks
   - Redistribute automated staff (CURRENTLY DISABLED)
   - Round non-automated production lines
   - Quality Assurance checks
5. **Refresh Quick Suite**

## Key Sub-Jobs

- `v4 fact_production_schedule` - Creates production schedule
- `fps_dt_iteration` - Iterates over dates
- `fps_automated_staffing_reassignment` - Staff redistribution (DISABLED)
- `fps_non_automated_line_rounding` - Rounds production lines
- `fps_qa` - Quality assurance checks

## Inputs

Google Sheet (protected): https://docs.google.com/spreadsheets/d/1aBLKbfhf2k1R_gv-eWlq5opunWHlT6Fe-VbZVTeAlJ4

Only Spencer Nedved and CJ Searcy can modify inputs.
Analytics service account (reporting@filterbuy.com) has edit access.

## Schedule

| Day | SLA | Notes |
|-----|-----|-------|
| Mon-Fri | 11:30am ET (Eastern locations), 11:30am MT (Ogden) | Full refresh |
| Saturday | Not time-critical | Large refresh - calculates production lines |
| Sunday | Not time-critical | Light refresh |

## Alerting

- Slack: #production-scheduling-tools (manual notifications)
- Slack: #production_schedule_etl_alerts (automated bot)
- Email: Weekly alerts to CJ Searcy for missing SKUs

## Rollback

Rollbacks are possible but must be manually kicked off via the ETL job.
