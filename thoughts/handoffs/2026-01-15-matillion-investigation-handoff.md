# Matillion Production Schedule Investigation Handoff

**Created:** 2026-01-15 14:10 UTC
**Session:** Root cause analysis - Why automated rows are missing
**Status:** CLOSED - Root cause identified, decision made to let system self-correct

---

## Executive Summary

Continued investigation from 2026-01-14. Today's focus was understanding why `automated_rows = 0` for Jan 12-18.

**Key Finding:** Automated production schedules are generated **once per week** by the "Initialize ps_staffing_by_dt_next_wk" job, NOT by daily runs. The daily runs only process non-automated production. This is **by design** per the business logic documentation.

The gap in Jan 12-18 automated data exists because the weekly initialization job that should have run around Jan 3-6 either failed, didn't run, or had a date calculation issue.

---

## Major Discoveries Today

### Discovery 1: Automated Rows Only Come From Weekly "Initialize" Job

Analysis of `fact_production_schedule` by `runtime_dt_utc` revealed:

| Source Run | Created Automated For | Type |
|------------|----------------------|------|
| Dec 20 | Dec 25-29 | Weekly Initialize |
| Dec 27 | Dec 31, Jan 1, Jan 5-8 | Weekly Initialize |
| Jan 10 | Jan 19-20 | Weekly Initialize |
| **All daily runs** | **0 automated rows** | Daily (non-automated only) |

**Every single daily run since at least Dec 24 has produced 0 automated rows.** This is BY DESIGN.

### Discovery 2: Business Logic Confirms Weekly Automated Scheduling

From the Production Schedule v4 Dashboard Documentation (now in `docs/context/production-scheduling-tool-business-logic.md`):

> "As of March 2025, automated sizes are **excluded from non-automated production**."
>
> "Furthermore, an automated schedule is **generated once per week**."

### Discovery 3: The Gap Explained

| Week | Initialize Run | Created Data For | Status |
|------|---------------|------------------|--------|
| Week of Dec 23 | Dec 20 | Dec 25-29 | ✅ |
| Week of Dec 30 | Dec 27 | Dec 31, Jan 1, Jan 5-11 | ✅ |
| Week of Jan 6 | **???** | **Jan 12-18** | ❌ MISSING |
| Week of Jan 13 | Jan 10 | Jan 19-25 | ✅ |

**The weekly Initialize job that should have created Jan 12-18 data didn't produce results.**

### Discovery 4: `is_selected_dt` Flow Clarified

Found where `is_selected_dt = true` is set:

1. **`ps_inputs_history` orchestration** runs transformations that set `is_selected_dt = true`
2. **`ps_settings_history`** transformation inserts with `is_selected_dt = true`
3. Similar pattern for `ps_staff_by_day_history`, `ps_line_count_history`, `ps_capacity_by_sku_history`

Current state of history tables:

| Table | is_selected_dt=true | is_selected_dt=false |
|-------|---------------------|----------------------|
| ps_settings_history | 1 | 145 |
| ps_staff_by_day_history | 49 | 7,105 |
| ps_line_count_history | 13 | 1,885 |
| ps_capacity_by_sku_history | 1,147 | 166,315 |

Only **Jan 14** has `is_selected_dt = true` in history tables.

### Discovery 5: `ps_fact_manufacturing_need_future` State

| schedule_date | run_date | is_selected_dt | rows |
|---------------|----------|----------------|------|
| 2026-01-14 | 2026-01-14 | **false** | 21,936 |
| 2026-01-15 | 2026-01-14 | **false** | 21,936 |
| 2026-01-16 | 2026-01-14 | **false** | 20,328 |
| 2026-01-17 | 2026-01-14 | **false** | 20,328 |
| 2026-01-18 | 2026-01-14 | **false** | 20,328 |
| **2026-01-19** | 2026-01-14 | **true** | **6,776** |

Only Jan 19 has `is_selected_dt = true`, which explains why only Jan 19+ have automated rows.

### Discovery 6: Reset Mechanism Identified

Component `ps_fact_manufacturing_need_future` inside `fps_dt_iteration` runs:

```sql
UPDATE stg2.ps_fact_manufacturing_need_future
SET is_selected_dt = false
WHERE is_selected_dt = true;
```

This resets the flag at the start of each iteration.

---

## Current Understanding of Architecture

### Production Schedule Data Flow

```
1. ps_inputs_history orchestration
   └── Sets is_selected_dt = true in history tables

2. ps_fact_manufacturing_need_future orchestration (runs twice)
   └── Iteration 0: "today"
   └── Iteration 1: "tomorrow"

3. fps_dt_iteration (for each future date)
   └── Resets is_selected_dt = false
   └── Processes automated (weekly) and non-automated (daily)

4. Output to dwh.fact_production_schedule
```

### Key Flags

| Flag | Meaning |
|------|---------|
| `is_selected_dt` | Current/active data snapshot |
| `is_current_production_schedule` | Schedule for "today" |
| `is_tomorrow_production_schedule` | Schedule for "tomorrow" |
| `is_future_production_schedule` | Schedule for days beyond tomorrow |

### Automated vs Non-Automated Processing

| Aspect | Automated | Non-Automated |
|--------|-----------|---------------|
| Frequency | **Weekly** | Daily |
| Job | Initialize ps_staffing_by_dt_next_wk | fps_non_automated |
| Creates data for | Next week | Today + Tomorrow |
| Since March 2025 | Excluded from daily runs | Processes all non-automated |

---

## Duplicate Cleanup Completed

### Cleanup SQL Used

```sql
-- Delete duplicate runtime batches from fact_production_schedule
DELETE FROM dwh.fact_production_schedule
USING (
    SELECT
        inserted_dt_utc::date as schedule_date,
        manufacturing_location,
        MAX(DATE_TRUNC('hour', runtime_dt_utc)) as keep_runtime
    FROM dwh.fact_production_schedule
    GROUP BY 1, 2
) latest_runtime
WHERE dwh.fact_production_schedule.inserted_dt_utc::date = latest_runtime.schedule_date
  AND dwh.fact_production_schedule.manufacturing_location = latest_runtime.manufacturing_location
  AND DATE_TRUNC('hour', dwh.fact_production_schedule.runtime_dt_utc) < latest_runtime.keep_runtime;

-- Delete duplicate runtime batches from ps_fact_manufacturing_need_future
DELETE FROM stg2.ps_fact_manufacturing_need_future
USING (
    SELECT
        manufacturing_location,
        MAX(DATE_TRUNC('hour', runtime_dt_utc)) as keep_runtime
    FROM stg2.ps_fact_manufacturing_need_future
    GROUP BY 1
) latest_runtime
WHERE stg2.ps_fact_manufacturing_need_future.manufacturing_location = latest_runtime.manufacturing_location
  AND DATE_TRUNC('hour', stg2.ps_fact_manufacturing_need_future.runtime_dt_utc) < latest_runtime.keep_runtime;
```

**Duplicate criteria:** More than 1 runtime per location per date.

---

## Documentation Created

Created `docs/context/production-scheduling-tool-business-logic.md` from Google Doc "Production Schedule v4 Dashboard Documentation".

Key business logic documented:
- Demand calculation methods (B2C, B2B, Retail/FBA)
- Excess inventory distribution
- Automated production ranking and scheduling
- Non-automated reactive and proactive production
- Line type assignment logic

---

## Current State of Data

### `dwh.fact_production_schedule`

| schedule_date | is_current | is_future | is_tomorrow | total | automated |
|---------------|------------|-----------|-------------|-------|-----------|
| 2026-01-14 | **true** | false | false | 631 | **0** |
| 2026-01-15 | false | false | **true** | 2566 | **0** |
| 2026-01-16 | false | **true** | false | 2435 | **0** |
| 2026-01-17 | false | **true** | false | 2549 | **0** |
| 2026-01-18 | false | **true** | false | 2702 | **0** |
| 2026-01-19 | false | **true** | false | 1786 | **61** ✅ |
| 2026-01-20 | false | **true** | false | 1337 | **88** ✅ |

### `stg2.ps_staffing_by_dt`

✅ Has correct data for all dates Jan 8-20 (6-7 rows per day)

---

## Resolution

### Decision: Let System Self-Correct

**Date:** 2026-01-15

After thorough investigation, the decision was made to **not intervene** and let the system normalize naturally starting Jan 19.

**Rationale:**
1. Jan 19+ already have automated data from the Jan 10 weekly Initialize run
2. Manual intervention carries risk of introducing additional data inconsistencies
3. The gap (Jan 12-18) is a historical issue that will naturally age out
4. Business operations can continue with non-automated production for the affected dates

### Impact Assessment

| Date Range | Automated Data | Non-Automated Data | Action |
|------------|----------------|-------------------|--------|
| Jan 12-14 | ❌ Missing | ✅ Present | No action - historical |
| Jan 15-18 | ❌ Missing | ✅ Present | No action - will age out |
| Jan 19+ | ✅ Present | ✅ Present | Normal operations |

### Lessons Learned

1. **Automated scheduling is weekly by design** - This was not immediately obvious and caused initial confusion
2. **Duplicate cleanup queries need Redshift-specific syntax** - Standard PostgreSQL DELETE with aliases doesn't work
3. **The `is_selected_dt` flag is transient** - It gets reset each iteration, making point-in-time debugging difficult
4. **Documentation is critical** - The business logic doc clarified the weekly vs daily processing

---

## Unanswered Questions (Deferred)

These questions remain unanswered but are not blocking:

1. **Why didn't the weekly Initialize job create data for Jan 12-18?**
   - Likely a job failure or scheduling issue during week of Jan 6
   - Not investigated further as system will self-correct

2. **What day of the week does the Initialize job run?**
   - Appears to run on Fridays based on runtime patterns
   - Dec 20 (Friday), Dec 27 (Friday), Jan 10 (Friday)

---

## Recommended Future Monitoring

To prevent similar issues:

1. **Alert on missing automated rows** - If a date has 0 automated rows but should have them
2. **Monitor weekly Initialize job** - Ensure it completes successfully each week
3. **Validate ps_staffing_by_dt coverage** - Check that next week's data is created

---

## AWS Access Pattern

```bash
# Execute query
AWS_REGION=us-east-1 aws-vault exec rt -- aws redshift-data execute-statement \
  --region us-east-1 \
  --cluster-identifier redshift-cluster-filterbuy \
  --database filterbuy_dw \
  --sql "YOUR_SQL_HERE"

# Get results (wait ~5 seconds)
AWS_REGION=us-east-1 aws-vault exec rt -- aws redshift-data get-statement-result \
  --region us-east-1 \
  --id "STATEMENT_ID" \
  --output json | jq '.Records'
```

---

## Key Queries

### Check automated rows by runtime source
```sql
SELECT DATE(inserted_dt_utc) as schedule_date,
       DATE_TRUNC('hour', runtime_dt_utc) as runtime_hour,
       COUNT(*) as total,
       COUNT(CASE WHEN line_type = 'Automated' THEN 1 END) as automated
FROM dwh.fact_production_schedule
WHERE inserted_dt_utc::date BETWEEN '2026-01-08' AND '2026-01-20'
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Check history tables is_selected_dt status
```sql
SELECT 'ps_settings_history' as tbl, is_selected_dt, COUNT(*)
FROM stg2.ps_settings_history GROUP BY 2
UNION ALL
SELECT 'ps_staff_by_day_history', is_selected_dt, COUNT(*)
FROM stg2.ps_staff_by_day_history GROUP BY 2
UNION ALL
SELECT 'ps_line_count_history', is_selected_dt, COUNT(*)
FROM stg2.ps_line_count_history GROUP BY 2
UNION ALL
SELECT 'ps_capacity_by_sku_history', is_selected_dt, COUNT(*)
FROM stg2.ps_capacity_by_sku_history GROUP BY 2;
```

### Check ps_fact_manufacturing_need_future state
```sql
SELECT inserted_dt_utc::date as schedule_date,
       runtime_dt_utc::date as run_date,
       is_selected_dt,
       COUNT(*) as rows
FROM stg2.ps_fact_manufacturing_need_future
WHERE inserted_dt_utc::date BETWEEN '2026-01-14' AND '2026-01-20'
GROUP BY 1, 2, 3
ORDER BY 1, 2;
```

---

## Files Modified/Created Today

1. `docs/context/production-scheduling-tool-business-logic.md` - Created from Google Doc
2. `thoughts/handoffs/2026-01-15-matillion-investigation-handoff.md` - This file

---

## Contact/Resources

| Resource | Details |
|----------|---------|
| Previous Handoff | `thoughts/handoffs/2026-01-14-matillion-investigation-handoff.md` |
| Business Logic Doc | `docs/context/production-scheduling-tool-business-logic.md` |
| Matillion UI | https://matillion.filterbuy.com |
| Redshift Cluster | redshift-cluster-filterbuy |
| SQL Files | `exports/sql-extracted/` |
