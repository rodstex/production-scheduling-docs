# QuickSight Dataset: Fact Production Schedule_DWH_PS

**Dataset ID:** `f5dee411-accf-44ea-b867-4f0ce5075e16`
**Import Mode:** SPICE (11.5 MB)
**Created:** 2025-02-18
**Last Updated:** 2026-01-29

---

## Overview

This dataset powers the Production Schedule dashboard in QuickSight. It provides a view of the production schedule with all necessary dimensions and metrics for operations teams to monitor and manage manufacturing across all Filterbuy locations.

The dataset queries `dwh.fact_production_schedule` with additional joins to `stg2.ps_capacity_by_sku` for control values, filtering to show only data from yesterday onward.

---

## Source Query

**Data Source:** Redshift (`dcd87842-3dae-4bde-86f4-81e34692c3df`)

**Primary Table:** `dwh.fact_production_schedule`

**Additional Joins:**
- `stg2.ps_capacity_by_sku` - Joined 8 times for control values (per location/line type combination)

**Filter:** `WHERE fps.inserted_dt_utc >= (current_timestamp - interval '1 day')::date`

**Key Transformations in SQL:**
1. **Timezone conversion** - Converts `runtime_dt_utc` to local time per manufacturing location:
   - New Kensington, PA → America/New_York
   - Talladega, AL → America/Chicago
   - Ogden, UT → America/Denver

2. **Grouped Line Type** - Consolidates Single Loader, Double Loader, Manual → "Non-Automated"

3. **Shift calculation** - Mon-Thu vs Fri-Sun based on day of week

4. **Lines Available calculation** - Different logic for Automated vs Non-Automated lines

---

## Columns

### Dimensions (8 columns)

| Column Name | Type | Description |
|-------------|------|-------------|
| Manufacturing Location | STRING | Factory location (New Kensington PA, Ogden UT, Talladega AL variants) |
| Distribution Center | STRING | Target distribution location |
| Line Type | STRING | Automated, Single Loader, Double Loader, Manual |
| Grouped Line Type | STRING | Simplified: Automated or Non-Automated |
| SKU Without MERV Rating | STRING | Filter size (e.g., AFB20x20x1) |
| Non-Automated Logic Type | STRING | Reactive or Proactive |
| Shift | STRING | Mon-Thu or Fri-Sun |
| Action Field | STRING | Composite key: runtime + distribution + SKU |

### Facts - Production Goals (5 columns)

| Column Name | Type | Description |
|-------------|------|-------------|
| Production Goal (MERV 8) | INTEGER | Units to produce - MERV 8 rating |
| Production Goal (MERV 11) | INTEGER | Units to produce - MERV 11 rating |
| Production Goal (MERV 13) | INTEGER | Units to produce - MERV 13 rating |
| Production Goal (OE) | INTEGER | Units to produce - Odor Eliminator |
| Production Goal (Total) | INTEGER | Total units to produce |

### Facts - Inventory (4 columns)

| Column Name | Type | Description |
|-------------|------|-------------|
| Days of Inventory (MERV 8) | DECIMAL | Current inventory days for MERV 8 |
| Days of Inventory (MERV 11) | DECIMAL | Current inventory days for MERV 11 |
| Days of Inventory (MERV 13) | DECIMAL | Current inventory days for MERV 13 |
| Days of Inventory (OE) | DECIMAL | Current inventory days for Odor Eliminator |

### Facts - Production Lines (4 columns)

| Column Name | Type | Description |
|-------------|------|-------------|
| Production Lines | DECIMAL | Number of production lines allocated |
| Changeover Production Lines | DECIMAL | Lines needed for changeover |
| Production and Changeover Production Lines | DECIMAL | Total lines (production + changeover) |
| Reassigned Automated Production Lines | DECIMAL | Automated lines reassigned to non-automated |

### Facts - Dates (4 columns)

| Column Name | Type | Description |
|-------------|------|-------------|
| Runtime Date UTC | DATETIME | When the schedule was generated |
| Data Effective Date | DATETIME | Runtime in local timezone |
| Date | DATETIME | Scheduled production date |
| Week Date | DATETIME | Start of the week (Sunday) |
| Original Scheduled Date | DATETIME | For rescheduled automated items |

### Facts - Flags (5 columns)

| Column Name | Type | Description |
|-------------|------|-------------|
| Is Current Production Schedule Flag | INTEGER | 1 if scheduled for today |
| Is Tomorrow Production Schedule Flag | INTEGER | 1 if scheduled for tomorrow |
| Is Future Production Schedule Flag | INTEGER | 1 if scheduled for future |
| Is Within Capacity Flag | INTEGER | 1 if within line capacity |
| Is Within Capacity Flag (Prior to Rounding) | INTEGER | Pre-rounding capacity flag |

### Facts - Window Functions (4 columns)

| Column Name | Type | Description |
|-------------|------|-------------|
| Lines Available (Over Manufacturing Location and Line Type) | DECIMAL | Staffing available for location/line type |
| Rank (Over Manufacturing Location and Line Type) | INTEGER | Production priority rank |
| Filters per Pallet (Over SKU without MERV Rating) | INTEGER | Pallet capacity |
| Reassigned Automated Production Lines (Over Date and Manufacturing Location) | DECIMAL | Reassigned lines aggregate |

### Control Values (8 columns)

Used for dashboard filtering - shows SKU for each location/line type combination:

| Column Name | Purpose |
|-------------|---------|
| SKU Without MERV Rating (New Kensington, PA \| Automated) | Filter control |
| SKU Without MERV Rating (New Kensington, PA \| Non-Automated) | Filter control |
| SKU Without MERV Rating (Ogden, UT \| Automated) | Filter control |
| SKU Without MERV Rating (Ogden, UT \| Non-Automated) | Filter control |
| SKU Without MERV Rating (Talladega, AL (Newberry) \| Non-Automated) | Filter control |
| SKU Without MERV Rating (Talladega, AL (Pope) \| Non-Automated) | Filter control |
| SKU Without MERV Rating (Talladega, AL (Woodland) \| Non-Automated) | Filter control |
| SKU Without MERV Rating (Talladega, AL (TMS) \| Automated) | Filter control |

### Calculated Fields (1 column)

| Column Name | Expression |
|-------------|------------|
| Line Type Sort | `ifelse(Line Type = 'Automated', 1, Line Type = 'Non-Automated', 2, ...)` |

---

## Field Folders (Organization in QuickSight)

| Folder | Fields |
|--------|--------|
| **Dimensions** | Action Field, Manufacturing Location, Distribution Center, Line Type, SKU Without MERV Rating, Non-Automated Logic Type, Shift, Grouped Line Type |
| **Facts** | Runtime Date UTC, Data Effective Date, Date, Production Goals, Production Lines, Days of Inventory, Changeover Lines, Week Date, Original Scheduled Date |
| **Facts/Flags** | Is Current/Tomorrow/Future/Within Capacity flags |
| **Facts/Window Functions** | Lines Available, Rank, Filters per Pallet, Reassigned Lines |
| **Control Values** | All 8 location-specific SKU fields |
| **Calculated Fields** | Line Type Sort |

---

## Data Lineage

```
dwh.fact_production_schedule (Redshift)
        ↓
   + stg2.ps_capacity_by_sku (8 joins for control values)
        ↓
   QuickSight Custom SQL
        ↓
   SPICE Import (refreshed via schedule)
        ↓
   Production Schedule Dashboard
```

---

## Related Datasets

Based on search results, these are related production schedule datasets:

| Dataset | Purpose |
|---------|---------|
| `Production Schedule Facts_DWH_D_5` | Alternative/older version |
| `Production Performance_DWH_PS` | Performance metrics |
| `Automated Production Schedule` | Automated lines specific |
| `Non-Automated Production Schedule` | Non-automated lines specific |
| `[QA] Fact Production Schedule` | QA/testing version |

---

## Investigation Notes

1. **SPICE vs Direct Query:** This dataset uses SPICE (in-memory cache) at 11.5 MB. Changes to underlying Redshift data require SPICE refresh to appear in dashboard.

2. **Data Freshness:** Only shows data from yesterday onward (`>= current_timestamp - interval '1 day'`). Historical analysis requires a different dataset.

3. **Control Values Pattern:** The 8 control value columns are a workaround for dashboard filtering - they pull max SKU per location/line type from `ps_capacity_by_sku`.

4. **Lines Available Logic:** Different calculation for Automated (uses `lines_available_over_manufacturing_location_line_type`) vs Non-Automated (aggregates `production_and_changeover_lines_within_capacity`).

5. **Timezone Handling:** The `data_effective_dt` field converts UTC to local time per manufacturing location - important for operations viewing "today's" schedule.

---

## Useful Queries

### Check SPICE refresh status
```bash
aws-vault exec rt -- aws quicksight list-ingestions \
  --aws-account-id 937346932434 \
  --data-set-id f5dee411-accf-44ea-b867-4f0ce5075e16
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
