# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

This repository contains exports and analysis tools for Filterbuy's Matillion ETL infrastructure. It is used to:
1. Extract and analyze Matillion job definitions from the production ETL platform
2. Debug data pipeline issues (particularly the Production Scheduling Tool)
3. Document the data warehouse architecture

**Critical Context:** Matillion v1.74.5 will cease to work after April 1, 2026. This repository supports the migration/debugging effort.

## Commands

### Analyze Matillion Export
```bash
python scripts/analyze-matillion-export.py
```
Parses `exports/matillion-full-export.json` to find production schedule jobs, SQL queries, and line_type JOINs. Outputs to `exports/analysis-results.json`.

### Extract All SQL
```bash
python scripts/extract-all-sql.py
```
Extracts all SQL queries from the Matillion export into individual files in `exports/sql-extracted/`. Creates a special `line_type_joins/` subdirectory for queries that JOIN on `line_type` (suspected failure points).

### Export from Matillion API
```bash
export MATILLION_HOST="matillion.filterbuy.com"
export MATILLION_USER="your-username"
export MATILLION_PASS="your-password"

# Single job
curl -u "$MATILLION_USER:$MATILLION_PASS" \
  "https://$MATILLION_HOST/rest/v1/group/FilterBuy/project/filterbuy_dw/version/default/job/run_production_schedule_v4/export"

# Full project
curl -u "$MATILLION_USER:$MATILLION_PASS" \
  "https://$MATILLION_HOST/rest/v1/group/FilterBuy/project/filterbuy_dw/version/default/export" \
  -o filterbuy_dw_export.zip
```

## Architecture Overview

### Data Flow
```
Data Sources (RDS, Google Sheets, QuickBooks, etc.)
        ↓
   Matillion ETL (AWS EC2)
        ↓
   Amazon Redshift (dwh schema)
        ↓
   Dashboards / Quick Suite
```

### Key ETL Structure

The main data warehouse project is `filterbuy_dw`. Jobs are organized as:
- **Orchestrations** - Control flow, call other jobs
- **Transformations** - SQL-based data transformations

Production Schedule Tool (v4) hierarchy:
```
run_production_schedule_v4          (entry point)
├── data_loaders/                   (Google Sheets → staging tables)
├── ps_retail_demand                (demand calculations)
├── fps_automated                   (automated line scheduling)
├── fps_non_automated               (manual line scheduling)
├── fps_non_automated_line_rounding (capacity rounding)
└── fps_qa                          (quality assurance)
```

### Table Naming Conventions

| Prefix | Source |
|--------|--------|
| `ab_` | Airfilterbuy (RDS) |
| `dw_` | Data warehouse derived |
| `gs_` | Google Sheets |
| `ps_` | Production Schedule staging |
| `qbo_` | QuickBooks |
| `sb_` | Supplybuy (RDS) |

### Critical Tables for Production Schedule

- `dwh.fact_production_schedule` - Final output
- `dwh.fact_manufacturing_need` - Demand calculations
- `stg2.ps_staffing_by_dt` - Staffing availability by date
- `stg2.ps_staff_by_day_history` - Staffing source (from Google Sheets)

## Current Investigation Context

The Production Schedule Tool runs successfully but produces empty output. Root cause investigation has identified:

1. **Critical JOIN:** `ps_staffing_by_dt` JOIN in `ps_automated_ranking_trans` - if this table has no rows for the current date, the entire schedule is empty
2. **Suspected failure point:** `is_selected_dt` flag in `ps_staff_by_day_history` not being set correctly
3. **SQL files to investigate:** `exports/sql-extracted/line_type_joins/` contains 69 queries with line_type JOINs

See `docs/context/investigation-findings.md` for detailed root cause analysis.

## File Structure

```
exports/
├── matillion-full-export.json    # Raw Matillion project export
├── analysis-results.json         # Script output
└── sql-extracted/                # Individual SQL files
    └── line_type_joins/          # Queries with line_type JOINs

docs/context/
├── data-warehouse-etl.md         # Infrastructure documentation
├── production-scheduling-tool-v4.md  # Tool documentation
├── current-problem.md            # Problem statement
└── investigation-findings.md     # Root cause analysis

scripts/
├── analyze-matillion-export.py   # Analysis script
├── extract-sql-queries.py        # SQL extractor (older version)
└── extract-all-sql.py            # Full SQL extractor
```

## AWS Resources

| Resource | ID |
|----------|-----|
| EC2 (Matillion) | i-0a5182d61a22ede85 |
| S3 Bucket | filterbuy-datawarehouse |
| Redshift | redshift-cluster-filterbuy |
| Secrets | Prefix: `analytics/` |

SSH access requires key from AWS Secrets Manager (`analytics/matillion_ec2_key`).

## Redshift Access & Debugging

### Query via AWS Data API (No psql needed)

```bash
# Execute a query
aws redshift-data execute-statement \
  --cluster-identifier redshift-cluster-filterbuy \
  --database filterbuy_dw \
  --sql "YOUR SQL HERE"

# Get results (wait 3-5 seconds after execute)
aws redshift-data get-statement-result --id "STATEMENT_ID" --output json

# Check status if no results
aws redshift-data describe-statement --id "STATEMENT_ID"
```

### Grant Permissions to IAM User

Use admin credentials from `analytics/redshift_admin` secret:

```bash
# Get admin creds
aws secretsmanager get-secret-value --secret-id analytics/redshift_admin --query SecretString --output text

# Grant access (run as admin)
aws redshift-data execute-statement \
  --cluster-identifier redshift-cluster-filterbuy \
  --database filterbuy_dw \
  --db-user admin \
  --sql "GRANT USAGE ON SCHEMA stg2 TO \"IAM:username\"; GRANT SELECT ON ALL TABLES IN SCHEMA stg2 TO \"IAM:username\";"
```

Schemas to grant: `stg1`, `stg2`, `dwh`

### Key Debugging Queries

**1. Check ps_staffing_by_dt for date gaps:**
```sql
SELECT dt, mapped_manufacturing_location, grouped_line_type, staffing_available
FROM stg2.ps_staffing_by_dt
WHERE dt >= CURRENT_DATE - 14
ORDER BY dt;
```

**2. Check is_selected_dt flag status:**
```sql
SELECT is_selected_dt, COUNT(*), MIN(inserted_dt_utc), MAX(inserted_dt_utc)
FROM stg2.ps_staff_by_day_history
GROUP BY 1;
```

**3. Check fact_production_schedule timeline:**
```sql
SELECT DATE(inserted_dt_utc) as dt, COUNT(*) as records,
       COUNT(CASE WHEN line_type IS NOT NULL THEN 1 END) as with_line_type
FROM dwh.fact_production_schedule
WHERE inserted_dt_utc >= CURRENT_DATE - 14
GROUP BY 1 ORDER BY 1 DESC;
```

**4. Check history table freshness:**
```sql
SELECT 'ps_staff_by_day_history' as tbl, MAX(inserted_dt_utc) FROM stg2.ps_staff_by_day_history
UNION ALL SELECT 'ps_line_count_history', MAX(inserted_dt_utc) FROM stg2.ps_line_count_history
UNION ALL SELECT 'ps_capacity_by_sku_history', MAX(inserted_dt_utc) FROM stg2.ps_capacity_by_sku_history;
```

## Production Schedule Data Flow

```
Google Sheets (Staff By Day tab)
        ↓
stg1.ps_staff_by_day (raw staging)
        ↓
stg2.ps_staff_by_day (transformed)
        ↓
stg2.ps_staff_by_day_history (is_selected_dt=true for current)
        ↓
stg2.ps_staffing_by_dt ← [COMMON FAILURE POINT - date gaps]
        ↓
ps_automated_ranking_trans (INNER JOIN - fails if no staffing data)
        ↓
dwh.fact_production_schedule
```

### Critical JOIN (ps_automated_ranking_trans line 101-103)
```sql
join stg2.ps_staffing_by_dt pl
    on pl.dt = fmn.inserted_dt_utc::date
    and pl.mapped_manufacturing_location = mdch.mapped_manufacturing_location
    and pl.grouped_line_type = 'Automated'
```
**If `ps_staffing_by_dt` has no rows for the target date, entire schedule is empty.**

### Initialize ps_staffing_by_dt_next_wk Logic
Creates data for NEXT WEEK only:
```sql
WHERE date_trunc('week', dd.dt) = (date_trunc('week', current_timestamp) + interval '7 day')::date
```
- If this job fails, next week's data won't exist
- Current week data should have been created LAST week

## Matillion UI Access

- **URL:** https://matillion.filterbuy.com
- **Project:** filterbuy_dw
- **Task History:** Project → Task History (shows job execution history with success/failure)
- **Schedules:** Project → Manage Schedules

## Common Issues & Fixes

| Issue | Symptom | Fix |
|-------|---------|-----|
| Date gap in ps_staffing_by_dt | Empty production schedule | Run Initialize job with `interval '0 day'` for current week |
| is_selected_dt all false | No current data snapshot | Re-run data loader orchestration |
| Out of Memory | Job fails silently | Check Matillion Notices panel |
| Location name mismatch | Zero rows from transform | Check Google Sheets for typos in "Manufacturing Location" column |

## Handoffs & Continuity

Investigation handoffs are stored in `thoughts/handoffs/`. Resume with:
```
/resume_handoff thoughts/handoffs/<filename>.md
```
