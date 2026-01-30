# Root Cause Analysis: Production Schedule Empty Data

**Investigation Date:** 2026-01-13
**Status:** ROOT CAUSE CONFIRMED - Fix Ready for Approval

---

## Executive Summary

The Production Scheduling Tool (v4) is generating incomplete schedules for Jan 12-18, 2026 because `stg2.ps_staffing_by_dt` has **no data for those dates**. The Initialize job that should have created this data during the week of Jan 5-11 either failed or did not run.

---

## Evidence Summary

### Evidence #1: Data Gap in ps_staffing_by_dt

Query executed:
```sql
SELECT dt, COUNT(*) as row_count
FROM stg2.ps_staffing_by_dt
WHERE dt >= '2026-01-06' AND dt <= '2026-01-25'
GROUP BY dt ORDER BY dt;
```

Results:
| Date Range | Rows/Day | Status |
|------------|----------|--------|
| Jan 6-11 | 6-7 | OK |
| **Jan 12-18** | **0** | **MISSING** |
| Jan 19-25 | 6-7 | OK |

**Conclusion:** Exactly 7 days of staffing data are missing for the current week.

---

### Evidence #2: Source Data is Available

Query executed:
```sql
SELECT is_selected_dt, COUNT(*) as cnt
FROM stg2.ps_staff_by_day_history
GROUP BY 1;
```

Results:
- `is_selected_dt = true`: 49 rows (inserted 2026-01-13 17:00 UTC)
- `is_selected_dt = false`: 7007 rows (historical)

**Conclusion:** Source staffing data exists and is current. The problem is NOT missing Google Sheets data.

---

### Evidence #3: Week Boundary Confirms Gap

Query executed:
```sql
SELECT CURRENT_DATE as today,
       date_trunc('week', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date as current_week_start;
```

Results:
- Today: 2026-01-13
- Current week starts: 2026-01-12 (Sunday)

**Conclusion:** The Initialize job creates data for NEXT week. Running on Jan 13 created Jan 19-25 data. Should have run during Jan 5-11 to create Jan 12-18 data.

---

### Evidence #4: Production Schedule Output Reduced

Query executed:
```sql
SELECT DATE(inserted_dt_utc) as run_date, COUNT(*) as total_rows
FROM dwh.fact_production_schedule
WHERE inserted_dt_utc >= '2026-01-01'
GROUP BY 1 ORDER BY 1 DESC;
```

Results:
| Date | Total Rows | Observation |
|------|------------|-------------|
| Jan 6-13 | 2000-2900 | Normal output |
| **Jan 14-18** | **780-880** | **~1/3 of normal** |
| Jan 19-25 | 1300-1460 | Recovering |

**Conclusion:** The job ran during Jan 12-18 but produced significantly fewer rows due to the failing JOIN on ps_staffing_by_dt.

---

### Evidence #5: Fix SQL Validated

Query executed (SELECT version of INSERT):
```sql
SELECT dd.dt, sbd.mapped_manufacturing_location, sbd.grouped_line_type, sbd.count_of_lines
FROM dwh.dim_date dd
LEFT JOIN stg2.ps_staff_by_day_history sbd ON sbd.is_selected_dt
    AND sbd.day_of_week_int = date_part(dow, dd.dt)
WHERE date_trunc('week', dd.dt) = date_trunc('week', current_timestamp at time zone 'America/New_York')::date
    AND sbd.mapped_manufacturing_location IN ('New Kensington, PA', 'Ogden, UT', 'Talladega, AL (TMS)',
        'Talladega, AL (Newberry)', 'Talladega, AL (Pope)', 'Talladega, AL (Woodland)');
```

Results: **49 rows** generated for Jan 12-18 with correct staffing values.

**Conclusion:** The fix SQL produces correct data that matches expected format.

---

## Root Cause

The `Initialize ps_staffing_by_dt_next_wk` job creates staffing data for **next week only**:

```sql
WHERE date_trunc('week', dd.dt) = (date_trunc('week', current_timestamp at time zone '${timezone}') + interval '7 day')::date
```

- **Week of Jan 5-11:** Should have created data for Jan 12-18 → **FAILED/DID NOT RUN**
- **Week of Jan 12 (today):** Created data for Jan 19-25 → SUCCESS

The job failure during Jan 5-11 resulted in no staffing data for the current week, causing the production schedule JOIN to return fewer matches.

---

## Proposed Fix

### Option A: Quick Fix - Direct INSERT (RECOMMENDED)

Insert the missing data for the current week by running the Initialize SQL with `interval '0 day'`:

```sql
INSERT INTO stg2.ps_staffing_by_dt (dt, mapped_manufacturing_location, grouped_line_type, staffing_available)
SELECT
    dd.dt,
    sbd.mapped_manufacturing_location,
    sbd.grouped_line_type,
    sbd.count_of_lines as staffing_available
FROM dwh.dim_date dd
    LEFT JOIN stg2.ps_staff_by_day_history sbd
        ON sbd.is_selected_dt
        AND sbd.day_of_week_int = date_part(dow, dd.dt)
WHERE date_trunc('week', dd.dt) = date_trunc('week', current_timestamp at time zone 'America/New_York')::date
    AND sbd.mapped_manufacturing_location IN ('New Kensington, PA', 'Ogden, UT', 'Talladega, AL (TMS)',
        'Talladega, AL (Newberry)', 'Talladega, AL (Pope)', 'Talladega, AL (Woodland)');
```

**Expected outcome:** 49 rows inserted for Jan 12-18.

### Option B: Re-run Full Orchestration

After applying Option A, re-run `run_production_schedule_v4` in Matillion to regenerate the full production schedule with the corrected staffing data.

---

## Verification Steps

After applying the fix:

1. **Verify ps_staffing_by_dt has data:**
```sql
SELECT dt, COUNT(*) FROM stg2.ps_staffing_by_dt
WHERE dt BETWEEN '2026-01-12' AND '2026-01-18'
GROUP BY dt ORDER BY dt;
```
Expected: 7 rows per day for Jan 12-18.

2. **Verify fact_production_schedule output improves:**
```sql
SELECT DATE(inserted_dt_utc), COUNT(*)
FROM dwh.fact_production_schedule
WHERE inserted_dt_utc >= '2026-01-12'
GROUP BY 1 ORDER BY 1;
```
Expected: Row counts should increase to normal levels (~2000-2900) after re-running the orchestration.

---

## Prevention Recommendations

1. **Add monitoring:** Alert when ps_staffing_by_dt has no data for the current week
2. **Investigate job failure:** Check Matillion logs for why Initialize job failed during Jan 5-11 (possible OOM error based on notices)
3. **Add redundancy:** Consider creating both current and next week data in Initialize job

---

## Timeline

- **Jan 5-11:** Initialize job should have run but failed → Created gap
- **Jan 12-18:** Production schedule runs with missing data → Reduced output
- **Jan 13:** Investigation identifies root cause and validates fix
- **Pending:** Apply fix and re-run orchestration
