---
name: debug-matillion
description: Debug Matillion ETL and Redshift data pipeline issues
allowed-tools: [Bash, Read, Write, Glob, Grep]
---

# Debug Matillion / Redshift Pipeline

Use this skill when investigating data pipeline issues, empty tables, or failed Matillion jobs.

## When to Use

- Production schedule is empty or missing data
- Dashboard shows "No data" for expected dates
- Matillion job completed but output is wrong
- Need to check data freshness or gaps

## Quick Diagnosis (Run These First)

### 1. Check ps_staffing_by_dt for date gaps

```bash
aws redshift-data execute-statement \
  --cluster-identifier redshift-cluster-filterbuy \
  --database filterbuy_dw \
  --sql "SELECT dt, COUNT(*) as rows FROM stg2.ps_staffing_by_dt WHERE dt >= CURRENT_DATE - 14 GROUP BY dt ORDER BY dt;"
```

Wait 3-5 seconds, then:

```bash
aws redshift-data get-statement-result --id "STATEMENT_ID" --output json
```

**Expected:** 6-7 rows per day. If a date has 0 rows, that's the problem.

### 2. Check is_selected_dt flag

```bash
aws redshift-data execute-statement \
  --cluster-identifier redshift-cluster-filterbuy \
  --database filterbuy_dw \
  --sql "SELECT is_selected_dt, COUNT(*) FROM stg2.ps_staff_by_day_history GROUP BY 1;"
```

**Expected:** Should have rows with `is_selected_dt = true`

### 3. Check fact_production_schedule output

```bash
aws redshift-data execute-statement \
  --cluster-identifier redshift-cluster-filterbuy \
  --database filterbuy_dw \
  --sql "SELECT DATE(inserted_dt_utc), COUNT(*), COUNT(CASE WHEN line_type IS NOT NULL THEN 1 END) FROM dwh.fact_production_schedule WHERE inserted_dt_utc >= CURRENT_DATE - 7 GROUP BY 1 ORDER BY 1 DESC;"
```

**Expected:** Records with non-null line_type for each day

### 4. Check history table freshness

```bash
aws redshift-data execute-statement \
  --cluster-identifier redshift-cluster-filterbuy \
  --database filterbuy_dw \
  --sql "SELECT 'ps_staff_by_day_history', MAX(inserted_dt_utc) FROM stg2.ps_staff_by_day_history UNION ALL SELECT 'ps_line_count_history', MAX(inserted_dt_utc) FROM stg2.ps_line_count_history;"
```

**Expected:** Recent timestamps (today or yesterday)

## Common Root Causes

### 1. Date Gap in ps_staffing_by_dt

**Symptom:** Production schedule empty for specific dates
**Cause:** `Initialize ps_staffing_by_dt_next_wk` job failed last week
**Fix:** INSERT current week data manually:

```sql
INSERT INTO stg2.ps_staffing_by_dt (dt, mapped_manufacturing_location, grouped_line_type, staffing_available)
SELECT dd.dt, sbd.mapped_manufacturing_location, sbd.grouped_line_type, sbd.count_of_lines
FROM dwh.dim_date dd
LEFT JOIN stg2.ps_staff_by_day_history sbd
    ON sbd.is_selected_dt AND sbd.day_of_week_int = date_part(dow, dd.dt)
WHERE date_trunc('week', dd.dt) = date_trunc('week', current_timestamp)::date
    AND sbd.mapped_manufacturing_location IN ('New Kensington, PA', 'Ogden, UT', 'Talladega, AL (TMS)', 'Talladega, AL (Newberry)', 'Talladega, AL (Pope)', 'Talladega, AL (Woodland)');
```

### 2. is_selected_dt All False

**Symptom:** No current data snapshot
**Cause:** Data loader didn't update the flag
**Fix:** Re-run `ps_inputs_history` orchestration in Matillion

### 3. Out of Memory Error

**Symptom:** Job fails silently or partially
**Where to check:** Matillion UI → Notices panel (shows OOM history)
**Fix:** May need to increase EC2 instance size or optimize query

### 4. Google Sheets Location Name Mismatch

**Symptom:** Zero rows from ps_staff_by_day_trans
**Cause:** Typo in "Manufacturing Location" column
**Fix:** Check Google Sheets values match exactly:
- `New Kensington, PA`
- `Ogden, UT`
- `Talladega, AL (TMS)`
- `Talladega, AL (Newberry)`
- `Talladega, AL (Pope)`
- `Talladega, AL (Woodland)`

## Grant Redshift Access to IAM User

```bash
aws redshift-data execute-statement \
  --cluster-identifier redshift-cluster-filterbuy \
  --database filterbuy_dw \
  --db-user admin \
  --sql "GRANT USAGE ON SCHEMA stg2 TO \"IAM:USERNAME\"; GRANT SELECT ON ALL TABLES IN SCHEMA stg2 TO \"IAM:USERNAME\"; GRANT USAGE ON SCHEMA stg1 TO \"IAM:USERNAME\"; GRANT SELECT ON ALL TABLES IN SCHEMA stg1 TO \"IAM:USERNAME\"; GRANT USAGE ON SCHEMA dwh TO \"IAM:USERNAME\"; GRANT SELECT ON ALL TABLES IN SCHEMA dwh TO \"IAM:USERNAME\";"
```

## Matillion UI Navigation

1. Go to https://matillion.filterbuy.com
2. Join Project → FilterBuy → filterbuy_dw
3. **Task History:** Project menu → Task History
4. **Schedules:** Project menu → Manage Schedules
5. **Run Job:** Right-click job in tree → Run

## Key SQL Files (in exports/sql-extracted/)

| File | Purpose |
|------|---------|
| `0149_ps_automated_ranking_trans_SQL_0.sql` | The critical JOIN that fails |
| `0107_Initialize_ps_staffing_by_dt_next_wk_SQL_0.sql` | Creates next week staffing data |
| `0124_ps_staff_by_day_history_SQL_0.sql` | History table with is_selected_dt |
| `0125_ps_staff_by_day_trans_SQL_0.sql` | Transforms Google Sheets data |

## Data Flow Chain

```
Google Sheets → stg1.ps_staff_by_day → stg2.ps_staff_by_day
    → stg2.ps_staff_by_day_history → stg2.ps_staffing_by_dt
    → ps_automated_ranking_trans → dwh.fact_production_schedule
```

**Failure Point:** The INNER JOIN in `ps_automated_ranking_trans` requires `ps_staffing_by_dt` to have data for the target date. If missing, entire schedule is empty.

---

## Search and Export Matillion Jobs

Use these commands to find and export orchestrations or transformations from Matillion.

### Prerequisites

Credentials are stored in `credentials.sh` (gitignored):

```bash
export MATILLION_HOST="matillion.filterbuy.com"
export MATILLION_USER="your-username"
export MATILLION_PASS="your-password"
```

### List All Jobs in Project

```bash
source credentials.sh && curl -s -u "$MATILLION_USER:$MATILLION_PASS" \
  "https://$MATILLION_HOST/rest/v1/group/name/FilterBuy/project/name/filterbuy_dw/version/name/default/job" | jq '.[]'
```

### Export a Specific Job

```bash
source credentials.sh && curl -s -u "$MATILLION_USER:$MATILLION_PASS" \
  "https://$MATILLION_HOST/rest/v1/group/name/FilterBuy/project/name/filterbuy_dw/version/name/default/job/name/JOB_NAME/export" \
  -o exports/JOB_NAME_export.json
```

Replace `JOB_NAME` with the job name (e.g., `run_incremental`, `pd_data_loaders`, `run_production_schedule_v4`).

### Export Full Project (ZIP)

```bash
source credentials.sh && curl -s -u "$MATILLION_USER:$MATILLION_PASS" \
  "https://$MATILLION_HOST/rest/v1/group/name/FilterBuy/project/name/filterbuy_dw/version/name/default/export" \
  -o exports/filterbuy_dw_full_export.zip
```

### Analyze Exported Job

After exporting, use jq to extract component information:

```bash
# List all components with names
cat exports/JOB_NAME_export.json | jq -r '
  .objects[0].jobObject.components | to_entries[] |
  "\(.key)|\(.value.parameters["1"].elements["1"].values["1"].value // "NO_NAME")"'

# Get job metadata
cat exports/JOB_NAME_export.json | jq '.objects[0].info'

# List variables
cat exports/JOB_NAME_export.json | jq '.objects[0].jobObject.variables | keys'

# Map execution flow (unconditional connectors)
cat exports/JOB_NAME_export.json | jq -r '
  (.objects[0].jobObject.components | to_entries |
   map({key: .key, value: .value.parameters["1"].elements["1"].values["1"].value}) | from_entries) as $names |
  (.objects[0].jobObject.unconditionalConnectors | to_entries[] |
   "\($names[.value.sourceID | tostring] // "Unknown") -> \($names[.value.targetID | tostring] // "Unknown")")'
```

### Component Implementation IDs

Common `implementationID` values in Matillion exports:

| ID | Component Type |
|----|----------------|
| `444132438` | Start |
| `1785813072` | Run Orchestration |
| `237373210` | Truncate Table |
| `-1773186829` | Python Script / Set Variable |
| `-1357378929` | If/Else Condition |
| `-1343684451` | Merge/And |
| `-798585337` | SQL Script |
| `-741198691` | Table Input |
| `-377483579` | Loop While |
| `1227580116` | Table Iterator |
| `438858066` | SNS Message |

### Search for Jobs Containing Specific SQL

First export the full project, then search:

```bash
# Search in exported JSON for SQL containing a keyword
unzip -p exports/filterbuy_dw_full_export.zip | grep -l "ps_staffing_by_dt"

# Or use the extracted SQL files
grep -r "ps_staffing_by_dt" exports/sql-extracted/
```

### API Endpoint Reference

| Endpoint | Purpose |
|----------|---------|
| `GET /rest/v1/group/name/{group}/project/name/{project}/version/name/{version}/job` | List all jobs |
| `GET /rest/v1/group/name/{group}/project/name/{project}/version/name/{version}/job/name/{job}/export` | Export single job |
| `GET /rest/v1/group/name/{group}/project/name/{project}/version/name/{version}/export` | Export full project |
| `GET /rest/v1/group/name/{group}/project/name/{project}/version/name/{version}/job/name/{job}` | Get job metadata |

### Troubleshooting API Access

**401 Unauthorized:** Check credentials. Special characters in password may need escaping - try using single quotes around the curl -u argument:

```bash
curl -s -u 'username:p@ssw0rd!' "https://..."
```

**404 Not Found:** Verify job name exists. Job names are case-sensitive.

**Empty Response:** Job may be in a subfolder. Check the job path in Matillion UI.

### Existing Exports

| File | Contents |
|------|----------|
| `exports/matillion-full-export.json` | Full project export (older) |
| `exports/run_incremental_export.json` | run_incremental orchestration |
| `exports/pd_data_loaders_export.json` | pd_data_loaders orchestration |
| `exports/sql-extracted/` | Individual SQL files extracted |

### Key Orchestrations

| Job Name | Purpose |
|----------|---------|
| `run_incremental` | Main incremental ETL (runs all data loaders) |
| `run_production_schedule_v4` | Production schedule generation |
| `pd_data_loaders` | Pipedrive CRM data loading |
| `gs_data_loaders` | Google Sheets data loading |
| `ab_data_loaders` | Airfilterbuy data loading |
| `sb_data_loaders` | Supplybuy data loading |
| `qbo_orchestrations` | QuickBooks Online data loading |
| `incremental_transformations` | Main transformation orchestration |
