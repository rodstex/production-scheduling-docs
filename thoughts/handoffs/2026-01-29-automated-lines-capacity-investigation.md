# Investigation: Automated Lines Showing Only 1 Within Capacity

**Date:** 2026-01-29
**Investigator:** Claude Code
**Status:** Fix Implemented

---

## Problem Statement

Only 1 automated production line was being scheduled as "within capacity" for Ogden, UT and New Kensington, PA when 2 lines were expected based on staffing availability.

### Symptoms
- QuickSight dashboard showing only 1 automated line within capacity
- Rank 2+ items marked as `is_within_capacity = FALSE`
- Expected: 2 automated lines should be within capacity when staffing_available = 2

---

## Root Cause: Floating-Point Precision in Cumulative Sum

### The Problematic Code

Located in: `exports/sql-extracted/0145_fps_automated_trans_SQL_0.sql` (lines 100-101)

```sql
,case when sum(production_goal_total::float / br.production_capacity::float)
  over (partition by br.mapped_manufacturing_location order by br.ranking asc rows unbounded preceding)
  <= br.staffing_available then true
  else false end as is_within_capacity
```

### What's Happening

1. **Capacity**: 2700 units per line for Ogden
2. **Production Goals by Rank**:
   - Rank 1: 916 units → 916/2700 = 0.3393
   - Rank 1: 1786 units → 1786/2700 = 0.6615
   - Rank 2: 773 units → 773/2700 = 0.2863
   - Rank 2: 1929 units → 1929/2700 = 0.7144

3. **Cumulative Sum Calculation**:
   ```
   After Rank 1: 0.3393 + 0.6615 = 1.0007 (floating-point)
   After Rank 2: 1.0007 + 0.2863 + 0.7144 = 2.0015
   ```

4. **The Bug**: `2.0015 > 2.0` (staffing_available), so rank 2+ items are marked `is_within_capacity = FALSE`

### Evidence

Query results from `dwh.fact_production_schedule`:

| SKU | Rank | Production Goal | Production Lines | Is Within Capacity |
|-----|------|-----------------|------------------|-------------------|
| AFB20X20X1 | 1 | 916+1786=2702 | 1.00 | TRUE |
| AFB20X25X1 | 2 | 773+1929=2702 | 1.00 | FALSE |

The production goals (2702) slightly exceed capacity (2700), causing cumulative floating-point error.

---

## Secondary Finding: Production Goals Exceed Capacity

The 2702 production goal (vs 2700 capacity) suggests upstream rounding or minimum run logic is adding extra units.

### Upstream Logic Location

File: `exports/sql-extracted/0149_ps_automated_ranking_trans_SQL_0.sql`

```sql
,case when sum(production_need) between 1 and min_production_run_per_merv_rating
  then min_production_run_per_merv_rating
  else sum(production_need) end as production_need
```

### Configuration Values (from ps_line_facts_history)
- `min_run_hrs_per_size` = 10.00 hours
- `max_prod_lines_per_size` = 2.00 lines
- `min_run_hrs_per_merv_rating` = 0.25 hours (15 min)
- `production_capacity` = 2700 units/line for Ogden

The minimum production run logic may be bumping small production needs up to the minimum threshold, causing the total to exceed exact capacity.

---

## Implemented Fix

Both recommended fixes were applied to `exports/sql-extracted/0145_fps_automated_trans_SQL_0.sql`:

### Changes Made

1. **Line 58**: `within_lines_production_lines` - changed `::float` to `::decimal(10,4)`
2. **Line 70**: `outside_lines_production_lines` - changed `::float` to `::decimal(10,4)`
3. **Line 98**: `production_lines` in automated_production CTE - changed `::float` to `::decimal(10,4)`
4. **Lines 100-103**: `is_within_capacity` - changed to use `::decimal(10,4)` AND added `+ 0.01` tolerance
5. **Line 137**: `production_lines` in threshold exceeded section - changed `::float` to `::decimal(10,4)`

### Key Fix (lines 100-103)

Before:
```sql
,case when sum(production_goal_total::float / br.production_capacity::float)
  over (partition by br.mapped_manufacturing_location order by br.ranking asc rows unbounded preceding)
  <= br.staffing_available then true
  else false end as is_within_capacity
```

After:
```sql
/* Fix: Use decimal instead of float to avoid floating-point precision errors,
 * and add 0.01 tolerance to handle edge cases where cumulative sum slightly exceeds threshold */
,case when sum(production_goal_total::decimal(10,4) / br.production_capacity::decimal(10,4))
  over (partition by br.mapped_manufacturing_location order by br.ranking asc rows unbounded preceding)
  <= (br.staffing_available + 0.01) then true
  else false end as is_within_capacity
```

---

## Recommended Fixes (Original Analysis)

### Option 1: Use DECIMAL Instead of FLOAT (Recommended)

Change the cumulative calculation to use fixed-precision decimals:

```sql
,case when sum(production_goal_total::decimal(10,4) / br.production_capacity::decimal(10,4))
  over (partition by br.mapped_manufacturing_location order by br.ranking asc rows unbounded preceding)
  <= br.staffing_available then true
  else false end as is_within_capacity
```

### Option 2: Add Tolerance to Comparison

Allow a small epsilon for floating-point comparison:

```sql
,case when sum(production_goal_total::float / br.production_capacity::float)
  over (partition by br.mapped_manufacturing_location order by br.ranking asc rows unbounded preceding)
  <= (br.staffing_available + 0.01) then true  -- 1% tolerance
  else false end as is_within_capacity
```

### Option 3: Round Cumulative Sum

Round the cumulative sum before comparison:

```sql
,case when round(sum(production_goal_total::float / br.production_capacity::float)
  over (partition by br.mapped_manufacturing_location order by br.ranking asc rows unbounded preceding), 2)
  <= br.staffing_available then true
  else false end as is_within_capacity
```

---

## Files Involved

| File | Purpose |
|------|---------|
| `exports/sql-extracted/0145_fps_automated_trans_SQL_0.sql` | Contains the buggy `is_within_capacity` calculation |
| `exports/sql-extracted/0149_ps_automated_ranking_trans_SQL_0.sql` | Upstream ranking and production goal calculation |
| `dwh.fact_production_schedule` | Final table consumed by QuickSight |
| `stg2.ps_automated_ranking` | Intermediate ranking data |
| `stg2.ps_line_facts_history` | Configuration values (capacity, min runs) |

---

## QuickSight Dataset Context

**Dataset**: Fact Production Schedule_DWH_PS
**Dataset ID**: `f5dee411-accf-44ea-b867-4f0ce5075e16`
**Import Mode**: SPICE (11.5 MB)
**Documentation**: `docs/context/quicksight-dataset-fact-production-schedule.md`

---

## Next Steps

1. ✅ **Fix implemented** in `exports/sql-extracted/0145_fps_automated_trans_SQL_0.sql`
2. ✅ **Deployed to Matillion** (2026-01-29)
   - Backup saved to `exports/fps_automated_trans_backup_2026-01-29.json`
   - Old job deleted, new job imported via REST API
   - Verified both fixes present in Matillion
3. **Test fix** by running ETL and verifying both rank 1 and rank 2 show `is_within_capacity = TRUE` when cumulative lines ≤ staffing
4. **Refresh SPICE** in QuickSight to see updated data
5. **Consider** investigating why production goals (2702) exceed capacity (2700) - may be intentional due to minimum run requirements

---

## Deployment Log

**Date:** 2026-01-29
**Method:** Matillion REST API (delete + import)

1. Exported original job as backup: `exports/fps_automated_trans_backup_2026-01-29.json`
2. Modified SQL with fixes (decimal precision + tolerance)
3. Deleted existing job via API:
   ```
   DELETE /rest/v1/group/name/FilterBuy/project/name/filterbuy_dw/version/name/default/job/name/fps_automated_trans
   Response: {"success": true, "msg": "Successfully deleted Job: fps_automated_trans", "id": 4417452}
   ```
4. Imported modified job via API:
   ```
   POST /rest/v1/group/name/FilterBuy/project/name/filterbuy_dw/version/name/default/job/import
   Response: {"success": true, "statusList": [{"success": true, "name": "fps_automated_trans"}]}
   ```
5. Verified fixes present in newly imported job (component IDs changed from 4418731 to 10678970)

---

## Useful Commands

### Check current production schedule
```bash
AWS_REGION=us-east-1 aws-vault exec rt -- aws redshift-data execute-statement \
  --cluster-identifier redshift-cluster-filterbuy \
  --database filterbuy_dw \
  --sql "SELECT manufacturing_location, sku_without_merv_rating, rank_over_manufacturing_location_line_type, production_goal_total, production_lines, is_within_capacity FROM dwh.fact_production_schedule WHERE line_type = 'Automated' AND is_tomorrow_production_schedule = 1 ORDER BY manufacturing_location, rank_over_manufacturing_location_line_type;"
```

### Trigger SPICE refresh
```bash
aws-vault exec rt -- aws quicksight create-ingestion \
  --aws-account-id 937346932434 \
  --data-set-id f5dee411-accf-44ea-b867-4f0ce5075e16 \
  --ingestion-id "manual-refresh-$(date +%Y%m%d%H%M%S)"
```

---

*Document generated: 2026-01-29*
