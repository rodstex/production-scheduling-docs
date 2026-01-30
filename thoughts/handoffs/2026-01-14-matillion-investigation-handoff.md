# Matillion Production Schedule Investigation Handoff

**Created:** 2026-01-14 21:45 UTC
**Session:** Duplicate rows cleanup and automated rows investigation
**Status:** IN PROGRESS - Cleanup partially complete, root cause of is_selected_dt reset IDENTIFIED

---

## Executive Summary

Continued investigation from 2026-01-13. The user attempted to force `fps_automated` to run for Jan 15-18 by:
1. Manually updating `is_selected_dt = true` in `ps_fact_manufacturing_need_future`
2. Disabling 3 guard components in `run_production_schedule_v4`
3. Running the job multiple times

This created **duplicate rows** in `fact_production_schedule` and the `is_selected_dt` flag was **reset back to false** by an unknown component, causing `automated_rows = 0` for Jan 15-18.

---

## Changes Made Today

### 1. Deleted duplicate batches from `fact_production_schedule` for Jan 15

The user deleted the 3 older runtime batches, keeping only the 22:00 UTC batch:

```sql
DELETE FROM dwh.fact_production_schedule
WHERE inserted_dt_utc::date = '2026-01-15'
  AND DATE_TRUNC('hour', runtime_dt_utc) < '2026-01-14 22:00:00+00';
```

**Rows deleted:** ~5,753 (746 + 2,475 + 2,532)

### 2. Components Temporarily Disabled in Matillion (NOW RE-ENABLED)

The user temporarily disabled these 3 components in `run_production_schedule_v4` to allow re-running:

1. `Initialize stg2.ps_fact_manufacturing_need_max_dt`
2. `fact_manufacturing_need Is Not Updated Today?`
3. `fact_manufacturing_need_trans v4`

**Status:** ✅ These components have been RE-ENABLED.

---

## Key Discoveries

### Discovery 1: `fact_production_schedule` Jan 15 had 4 duplicate batches

| Runtime Hour (UTC) | Rows | Status |
|--------------------|------|--------|
| 2026-01-14 01:00 | 746 | ❌ Deleted |
| 2026-01-14 15:00 | 2,475 | ❌ Deleted |
| 2026-01-14 20:00 | 2,532 | ❌ Deleted |
| 2026-01-14 22:00 | 2,566 | ✅ Kept |

Jan 16-18 only had the 22:00 batch (NOT duplicated).

### Discovery 2: `ps_staffing_by_dt` is NOT duplicated

Verified that `ps_staffing_by_dt` has correct data for Jan 15-18:

| Date | Automated Capacity | Non-Automated Capacity |
|------|-------------------|------------------------|
| Jan 15 | 14 | 46 |
| Jan 16 | 4 | 8 |
| Jan 17 | 4 | 8 |
| Jan 18 | 4 | 8 |

Jan 16-18 have lower capacity (Friday-Sunday schedule).

### Discovery 3: `ps_fact_manufacturing_need_future` has duplicate batches AND wrong flag

Each schedule date (Jan 15-18) has 4 runtime batches:

| Runtime Date | Rows per schedule date | is_selected_dt |
|--------------|------------------------|----------------|
| 2026-01-10 | 6,775 | **false** |
| 2026-01-12 | 10,336 | **false** |
| 2026-01-13 | 8,384 | **false** |
| 2026-01-14 | 20,328 | **false** |
| **Total** | **45,823** | - |

**Critical:** ALL rows have `is_selected_dt = false`, which is why `fps_automated` produces 0 rows.

### Discovery 4: The `is_selected_dt` UPDATE was reset - ROOT CAUSE FOUND

The user ran:
```sql
UPDATE stg2.ps_fact_manufacturing_need_future
SET is_selected_dt = true
WHERE inserted_dt_utc::date BETWEEN '2026-01-15' AND '2026-01-18'
  AND runtime_dt_utc::date = '2026-01-14';
```

**ROOT CAUSE IDENTIFIED:** The component `ps_fact_manufacturing_need_future` inside `fps_dt_iteration` contains a SQL script that resets the flag:

```sql
UPDATE ${stg2_schema}.ps_fact_manufacturing_need_future
SET is_selected_dt = false
WHERE is_selected_dt = true;
```

This runs at the start of each iteration, resetting any `is_selected_dt = true` rows back to `false`. This is why the manual UPDATE didn't persist - the job reset it.

### Discovery 5: Why `automated_rows = 0` for Jan 15-18

The `fps_automated` transform requires `is_selected_dt = true` in `ps_fact_manufacturing_need_future`. Since all rows are `false`, the transform returns no results.

The non-automated transforms (`fps_non_automated`, etc.) apparently do NOT require this flag, which is why Double Loader, Single Loader, and Manual rows exist.

---

## Current State of Data (End of Day Jan 14)

### `dwh.fact_production_schedule` - STILL HAS DUPLICATES

Checked by `runtime_dt_utc` (multiple runs on Jan 14):

| Runtime Hour (UTC) | Rows |
|--------------------|------|
| 2026-01-14 01:00 | 1,001 |
| 2026-01-14 15:00 | 627 |
| 2026-01-14 20:00 | 630 |
| 2026-01-14 22:00 | 10,883 |
| **Total** | **13,141** |

**Note:** The earlier cleanup deleted duplicates by `inserted_dt_utc`, but there are still multiple runtime batches.

### `stg2.ps_fact_manufacturing_need_future` - MULTIPLE RUNTIME BATCHES

| Run Date | Run Hour | Rows |
|----------|----------|------|
| 2026-01-10 | 15:00 | 88,075 |
| 2026-01-12 | 15:00 | 15,504 |
| 2026-01-12 | 17:00 | 20,672 |
| 2026-01-12 | 23:00 | 20,672 |
| 2026-01-13 | 15:00 | 15,504 |
| 2026-01-13 | 17:00 | 4,824 |
| 2026-01-14 | 01:00 | 4,824 |
| 2026-01-14 | 15:00 | 40,656 |
| 2026-01-14 | 20:00 | 40,656 |
| 2026-01-14 | 22:00 | 40,656 |

This table has accumulated runs from Jan 10, 12, 13, and 14 with multiple batches per day. ALL have `is_selected_dt = false`.

### `stg2.ps_staffing_by_dt`

- ✅ Correct data for Jan 12-20
- ✅ No duplicates

---

## Remaining Issues

### Issue 1: `ps_fact_manufacturing_need_future` duplicate batches
Need to delete old runtime batches, keep only the latest (Jan 14 22:00):

```sql
-- Preview what will be deleted
SELECT runtime_dt_utc::date as run_date, DATE_TRUNC('hour', runtime_dt_utc) as run_hour, COUNT(*)
FROM stg2.ps_fact_manufacturing_need_future
WHERE runtime_dt_utc < '2026-01-14 22:00:00+00'
GROUP BY 1, 2 ORDER BY 1, 2;

-- Delete old runtime batches
DELETE FROM stg2.ps_fact_manufacturing_need_future
WHERE runtime_dt_utc < '2026-01-14 22:00:00+00';
```

### Issue 2: `is_selected_dt = false` - ROOT CAUSE NOW KNOWN
The component `ps_fact_manufacturing_need_future` inside `fps_dt_iteration` resets the flag. Options:
- **Option A:** Disable that specific component before running the UPDATE
- **Option B:** Run the UPDATE AFTER the full job completes (won't help for fps_automated which already ran)
- **Option C:** Run only `fps_automated` directly (not the full orchestration) after setting the flag

### Issue 3: `fact_production_schedule` still has duplicate runtime batches
Need to clean up using `runtime_dt_utc`:

```sql
-- Preview
SELECT DATE_TRUNC('hour', runtime_dt_utc) as run_hour, COUNT(*)
FROM dwh.fact_production_schedule
WHERE runtime_dt_utc::date = '2026-01-14'
GROUP BY 1 ORDER BY 1;

-- Delete old batches, keep only 22:00
DELETE FROM dwh.fact_production_schedule
WHERE runtime_dt_utc::date = '2026-01-14'
  AND DATE_TRUNC('hour', runtime_dt_utc) < '2026-01-14 22:00:00+00';
```

### Issue 4: Missing automated rows for Jan 15-18
After cleanup, need to regenerate automated rows. Options:
1. **Keep existing non-automated data** and add automated (suboptimal - scheduling order matters)
2. **Delete and regenerate** entire schedule for those dates

---

## Recommended Next Steps for Tomorrow (Jan 15)

### Step 1: Clean up `fact_production_schedule` duplicates
```sql
DELETE FROM dwh.fact_production_schedule
WHERE runtime_dt_utc::date = '2026-01-14'
  AND DATE_TRUNC('hour', runtime_dt_utc) < '2026-01-14 22:00:00+00';
```

### Step 2: Clean up `ps_fact_manufacturing_need_future` duplicates
```sql
DELETE FROM stg2.ps_fact_manufacturing_need_future
WHERE runtime_dt_utc < '2026-01-14 22:00:00+00';
```

### Step 3: Decide on automated rows strategy

**Option A: Wait for natural job run**
- Let Jan 15's scheduled job run normally
- It will create proper data for Jan 16-22 (next week)
- Jan 15 schedule may remain incomplete

**Option B: Force regeneration**
1. Delete Jan 15-18 data from `fact_production_schedule`
2. Disable the `ps_fact_manufacturing_need_future` component in `fps_dt_iteration` (the one that resets is_selected_dt)
3. Set `is_selected_dt = true` for the latest batch
4. Run only the fps components manually
5. Re-enable the component

**Option C: Accept incomplete data**
- Keep Jan 15-18 as-is (missing automated rows)
- Focus on ensuring Jan 19+ runs correctly

---

## SQL Files to Investigate

| File | Purpose | Why relevant |
|------|---------|--------------|
| `fact_manufacturing_need_trans*` | Creates manufacturing need data | May reset is_selected_dt |
| `fps_automated*` | Automated line scheduling | Requires is_selected_dt = true |
| `Initialize_ps_fact_manufacturing_need*` | Initializes the table | May reset flags |

Search command:
```bash
grep -r "is_selected_dt" exports/sql-extracted/
```

---

## Queries for Tomorrow

### Check duplicate status (use runtime_dt_utc)
```sql
-- Check ps_fact_manufacturing_need_future runtime batches
SELECT runtime_dt_utc::date as run_date,
       DATE_TRUNC('hour', runtime_dt_utc) as run_hour,
       COUNT(*) as rows
FROM stg2.ps_fact_manufacturing_need_future
GROUP BY 1, 2 ORDER BY 1, 2;

-- Check fact_production_schedule runtime batches
SELECT runtime_dt_utc::date as run_date,
       DATE_TRUNC('hour', runtime_dt_utc) as run_hour,
       COUNT(*) as rows
FROM dwh.fact_production_schedule
WHERE runtime_dt_utc >= '2026-01-14'
GROUP BY 1, 2 ORDER BY 1, 2;
```

### Check automated rows status
```sql
SELECT DATE(inserted_dt_utc) as schedule_date,
       COUNT(*) as total,
       COUNT(CASE WHEN line_type = 'Automated' THEN 1 END) as automated
FROM dwh.fact_production_schedule
WHERE inserted_dt_utc::date BETWEEN '2026-01-15' AND '2026-01-22'
GROUP BY 1 ORDER BY 1;
```

### Verify is_selected_dt after job run
```sql
SELECT is_selected_dt, COUNT(*) as rows
FROM stg2.ps_fact_manufacturing_need_future
GROUP BY 1;
```

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

## Important Reminders

1. ✅ **The 3 disabled components have been re-enabled**
2. ⚠️ **The production schedule for Jan 15-18 is incomplete** - missing automated rows
3. ⚠️ **Both tables still have duplicate runtime batches** - need cleanup
4. ✅ **Root cause of is_selected_dt reset is now known** - component in `fps_dt_iteration`
5. **Don't run the job multiple times** without the guard components - it creates duplicates

---

## Contact/Resources

| Resource | Details |
|----------|---------|
| Previous Handoff | `thoughts/handoffs/2026-01-13-matillion-investigation-handoff.md` |
| Matillion UI | https://matillion.filterbuy.com |
| Redshift Cluster | redshift-cluster-filterbuy |
| SQL Files | `exports/sql-extracted/` |
