# Current Problem: Production Schedule Tool Failure

> Source: Slack channel #data-engineer-support (January 2025)

## Summary

The Production Scheduling Tool runs to completion without errors, but the output table `fact_production_schedule` is empty (no manufacturing assignments) starting around January 11, 2025.

## Symptoms

1. Tool completes successfully (no component failures)
2. Dashboard shows "No data" for production schedule
3. `dwh.fact_production_schedule` has no `line_type` assignments after ~Jan 19
4. `dwh.fact_manufacturing_need` table is also returning empty

## Root Cause Investigation (by Rodrigo)

### Traced Path

1. **Output:** `fact_production_schedule` - empty
2. **Upstream:** `fact_manufacturing_need` - returns 0 rows
3. **Specific Issue:** A JOIN condition is failing

### The Problematic Query

In the ETL, there's a query where removing a specific JOIN comment causes the entire table to return empty:

```sql
-- If you comment out this condition, the table goes empty
-- The condition filters on line_type assignment
```

**Finding:** No production line (manufacturing order) is assigned to any SKU for future dates.

## Challenges

1. **No Documentation:** Only comments in Matillion components describe what each step does
2. **Complex Dependencies:** Each table is built from 4+ other tables
3. **Labyrinth Structure:** Hundreds of nested components, multiple versions (V1, V2, V3, V4)
4. **Disabled Components:** Some components are disabled without explanation
5. **Cannot Query Directly:** Unlike traditional IDE, can't run ad-hoc queries in Matillion

## Investigation Approach

Rodrigo is doing reverse engineering:
1. Find which output table is empty
2. Trace back to source tables
3. Find the JOIN condition causing empty results
4. Identify which component calculates the missing data

## Potential Causes

1. **Input Data Issue:** Something changed in Google Sheets inputs
2. **Component Logic:** A calculation step is producing no results
3. **Timing Issue:** Weekend job that sets production lines may have failed
4. **Data Quality:** Source data from Supplybuy may have issues

## Timeline

- **~Jan 11:** Tool stopped generating valid data
- **Jan 13:** Team discovers issue via empty dashboard
- **Jan 7:** Last successful data (per change history analysis)

## Key Contacts

- **David Ansel:** Original developer (left company)
- **Rodrigo Teixeira:** Current owner investigating
- **CJ Searcy / Spencer Nedved:** Business owners with input access
- **Kevin Kirkpatrick:** Matillion TAM (can help with technical support)
