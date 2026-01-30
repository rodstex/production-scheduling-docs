# Investigation Findings: Production Schedule Empty Data

## Root Cause Analysis

### Data Flow Discovery

The production schedule depends on this critical chain:

```
Google Sheets (ps_staff_by_day)
         ↓
ps_staff_by_day_history
         ↓
ps_staffing_by_dt  ← ← ← ← ← [SUSPECTED FAILURE POINT]
         ↓
ps_automated_ranking_trans → fact_production_schedule
```

### Critical JOIN Identified

In `ps_automated_ranking_trans` (file: `0149_ps_automated_ranking_trans_SQL_0.sql`), lines 101-103:

```sql
join ${stg2_schema}.ps_staffing_by_dt pl
    on pl.dt = fmn.inserted_dt_utc::date
    and pl.mapped_manufacturing_location = mdch.mapped_manufacturing_location
    and pl.grouped_line_type = 'Automated'
```

**If `ps_staffing_by_dt` has no rows for the current date, the entire production schedule will be empty.**

### How `ps_staffing_by_dt` is Populated

From `Initialize ps_staffing_by_dt_next_wk` (file: `0107_Initialize_ps_staffing_by_dt_next_wk_SQL_0.sql`):

```sql
select
    dd.dt
    ,sbd.mapped_manufacturing_location
    ,sbd.grouped_line_type
    ,sbd.count_of_lines as staffing_available
from ${dwh_schema}.dim_date dd
    left join ${stg2_schema}.ps_staff_by_day_history sbd
        on sbd.is_selected_dt
        and sbd.day_of_week_int = date_part(dow, dd.dt)
where date_trunc('week', dd.dt) = (date_trunc('week', current_timestamp at time zone '${timezone}') + interval '7 day')::date
    and sbd.mapped_manufacturing_location in (${manufacturing_locations})
```

**Critical conditions:**
1. `sbd.is_selected_dt` must be TRUE
2. Manufacturing locations must match the `${manufacturing_locations}` variable

## Suspected Root Causes

### Hypothesis 1: `is_selected_dt` Flag Issue
The `ps_staff_by_day_history` table uses an `is_selected_dt` flag to identify the current snapshot. If this flag was not set correctly during the data load, no staffing data would be returned.

**Verification Query:**
```sql
SELECT is_selected_dt, COUNT(*)
FROM stg2.ps_staff_by_day_history
GROUP BY 1;
```

### Hypothesis 2: Missing Google Sheets Data
The source data in Google Sheets (Staff By Day tab) may be missing values for the current week.

**Verification:**
Check Google Sheets: https://docs.google.com/spreadsheets/d/1aBLKbfhf2k1R_gv-eWlq5opunWHlT6Fe-VbZVTeAlJ4
- Tab: "Staff By Day"
- Check for empty cells in recent dates

### Hypothesis 3: Manufacturing Location Mismatch
The `${manufacturing_locations}` variable may not include all required locations.

**Verification Query:**
```sql
SELECT DISTINCT mapped_manufacturing_location
FROM stg2.ps_staff_by_day_history
WHERE is_selected_dt;
```

### Hypothesis 4: Date/Timezone Issue
The query uses `current_timestamp at time zone '${timezone}'`. If the timezone variable is incorrect, dates may not match.

## Immediate Verification Steps

1. **Check `ps_staffing_by_dt` for current week:**
```sql
SELECT dt, mapped_manufacturing_location, grouped_line_type, staffing_available
FROM stg2.ps_staffing_by_dt
WHERE dt >= CURRENT_DATE
ORDER BY dt, mapped_manufacturing_location, grouped_line_type;
```

2. **Check source data in `ps_staff_by_day_history`:**
```sql
SELECT *
FROM stg2.ps_staff_by_day_history
WHERE is_selected_dt
ORDER BY mapped_manufacturing_location, day_of_week_int;
```

3. **Check when data stopped appearing:**
```sql
SELECT DATE(inserted_dt_utc) as dt, COUNT(*)
FROM stg2.ps_staff_by_day_history
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20;
```

## Key Tables to Check

| Table | Purpose | Source |
|-------|---------|--------|
| `stg2.ps_staff_by_day_history` | Staffing by day of week | Google Sheets |
| `stg2.ps_staffing_by_dt` | Staffing by specific date | Calculated |
| `stg2.ps_fact_manufacturing_need_future` | Manufacturing demand | Calculated |
| `stg2.ps_line_count_history` | Line counts | Google Sheets |

## Next Steps

1. Run verification queries against Redshift
2. Check Google Sheets input data for Jan 7-11
3. Review Matillion job logs for `ps_staff_by_day` data loader
4. If data issue confirmed, manually re-run the data loader job
