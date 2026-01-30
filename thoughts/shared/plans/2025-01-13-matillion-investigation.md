# Matillion Production Schedule Investigation Plan

## Overview

The Production Scheduling Tool (v4) stopped generating valid manufacturing schedules around January 11, 2025. The tool completes without errors but output tables are empty. This plan extracts Matillion job configurations for analysis, traces the data pipeline to identify the failure point, and establishes version control for future debugging.

## Current State Analysis

**What exists:**
- Matillion ETL running on EC2 (`i-0a5182d61a22ede85`)
- Production Scheduling Tool v4 with 100+ nested components
- Output tables: `dwh.fact_production_schedule`, `dwh.fact_manufacturing_need`
- Documentation extracted to `docs/context/`

**What's broken:**
- `fact_production_schedule` has no `line_type` assignments after Jan 19
- `fact_manufacturing_need` returns 0 rows
- A JOIN condition fails because no manufacturing orders exist for future dates

**Key constraints:**
- Cannot query Matillion directly like traditional IDE
- No comprehensive documentation (only inline comments)
- Multiple versions (V1-V4) with disabled components
- Critical for daily manufacturing operations

## Desired End State

1. All Matillion job definitions exported as JSON in `exports/`
2. Root cause of empty data identified and documented
3. Production Schedule Tool generating valid data again
4. Jobs version-controlled for future debugging

### Verification:
- `dwh.fact_production_schedule` shows `line_type` assignments for tomorrow
- `dwh.fact_manufacturing_need` has non-zero records
- Dashboard displays production schedule data
- Jobs committed to git repository

## What We're NOT Doing

- Rewriting the Production Schedule Tool
- Migrating to Matillion Cloud (separate initiative)
- Changing business logic or algorithms
- Modifying input Google Sheets

## Implementation Approach

1. **Extract** - Get all job JSONs via REST API
2. **Analyze** - Parse JSONs to understand component flow and find failing query
3. **Investigate** - Query Redshift to trace data gaps
4. **Fix** - Apply targeted fix in Matillion UI
5. **Prevent** - Version control and monitoring

---

## Phase 1: Extract Matillion Jobs via REST API

### Overview
Export all job configurations from Matillion to local JSON files for analysis. This enables grep-based debugging and version control.

### Changes Required:

#### 1. Create Export Script
**File**: `scripts/export-matillion.sh`

```bash
#!/bin/bash
set -e

MATILLION_HOST="${MATILLION_HOST:-matillion.filterbuy.com}"
MATILLION_USER="${MATILLION_USER}"
MATILLION_PASS="${MATILLION_PASS}"
OUTPUT_DIR="${OUTPUT_DIR:-./exports}"

if [ -z "$MATILLION_PASS" ]; then
  echo "Error: MATILLION_PASS environment variable not set"
  exit 1
fi

BASE_URL="https://$MATILLION_HOST/rest/v1"
AUTH="$MATILLION_USER:$MATILLION_PASS"

mkdir -p "$OUTPUT_DIR"

echo "Testing API connectivity..."
curl -sf -u "$AUTH" "$BASE_URL/group" > /dev/null || {
  echo "Error: Cannot connect to Matillion API"
  exit 1
}

echo "Fetching groups..."
groups=$(curl -s -u "$AUTH" "$BASE_URL/group" | jq -r '.[].name')

for group in $groups; do
  echo "Processing group: $group"
  projects=$(curl -s -u "$AUTH" "$BASE_URL/group/$group/project" | jq -r '.[].name')

  for project in $projects; do
    echo "  Exporting project: $project"
    mkdir -p "$OUTPUT_DIR/$group/$project"

    # Export as ZIP
    curl -s -u "$AUTH" \
      "$BASE_URL/group/$group/project/$project/version/default/export" \
      -o "$OUTPUT_DIR/$group/$project/export.zip"

    # Unzip
    unzip -q -o "$OUTPUT_DIR/$group/$project/export.zip" \
      -d "$OUTPUT_DIR/$group/$project/" 2>/dev/null || true
  done
done

echo ""
echo "Export complete: $OUTPUT_DIR"
echo "Total files: $(find "$OUTPUT_DIR" -name "*.json" | wc -l)"
```

#### 2. Test API Connectivity
```bash
# Set credentials (user will provide actual values)
export MATILLION_HOST="matillion.filterbuy.com"
export MATILLION_USER="your-username"
export MATILLION_PASS="your-password"

# Test
curl -s -u "$MATILLION_USER:$MATILLION_PASS" \
  "https://$MATILLION_HOST/rest/v1/group" | jq .
```

#### 3. Run Export
```bash
chmod +x scripts/export-matillion.sh
./scripts/export-matillion.sh
```

### Success Criteria:

#### Automated Verification:
- [ ] API test returns JSON array of groups: `curl -s -u "$AUTH" "https://matillion.filterbuy.com/rest/v1/group" | jq -e 'type == "array"'`
- [ ] Export script completes without errors
- [ ] ZIP files exist: `ls exports/*/export.zip`
- [ ] JSON files extracted: `find exports -name "*.json" | head`
- [ ] Production schedule jobs present: `find exports -name "*production_schedule*"`

#### Manual Verification:
- [ ] Confirm credentials work in Matillion UI
- [ ] Verify exported jobs match UI structure

**Implementation Note**: After completing this phase and all automated verification passes, pause for manual confirmation before proceeding to Phase 2.

---

## Phase 2: Analyze Job Structure

### Overview
Parse the exported JSON files to understand component flow, build a dependency map, and identify the SQL queries related to the failing tables.

### Changes Required:

#### 1. Create Analysis Script
**File**: `scripts/analyze-jobs.py`

```python
#!/usr/bin/env python3
"""
Analyze Matillion job exports to find:
1. Component flow for production_schedule jobs
2. SQL queries touching fact_manufacturing_need
3. Table dependencies
"""
import json
import os
import re
from pathlib import Path
from collections import defaultdict

EXPORTS_DIR = Path("exports")
TARGET_TABLES = [
    "fact_production_schedule",
    "fact_manufacturing_need",
    "ps_line_count",
    "ps_staff_by_day"
]

def find_job_files():
    """Find all JSON job files"""
    return list(EXPORTS_DIR.rglob("*.json"))

def extract_sql_from_component(component):
    """Extract SQL from various component types"""
    sql_params = ["sql", "query", "sqlQuery", "SQL", "Query"]
    for param in sql_params:
        if param in component.get("parameters", {}):
            return component["parameters"][param]
    return None

def analyze_job(job_path):
    """Analyze a single job file"""
    with open(job_path) as f:
        try:
            job = json.load(f)
        except json.JSONDecodeError:
            return None

    results = {
        "name": job.get("name", job_path.name),
        "type": job.get("type", "unknown"),
        "components": [],
        "tables_referenced": set(),
        "sql_queries": []
    }

    for component in job.get("components", []):
        comp_info = {
            "id": component.get("id"),
            "type": component.get("type"),
            "name": component.get("name", component.get("type")),
        }

        sql = extract_sql_from_component(component)
        if sql:
            comp_info["sql"] = sql
            results["sql_queries"].append({
                "component": comp_info["name"],
                "sql": sql
            })

            # Find table references
            for table in TARGET_TABLES:
                if table.lower() in sql.lower():
                    results["tables_referenced"].add(table)

        results["components"].append(comp_info)

    results["tables_referenced"] = list(results["tables_referenced"])
    return results

def main():
    print("Analyzing Matillion job exports...")
    print("=" * 60)

    job_files = find_job_files()
    print(f"Found {len(job_files)} JSON files")

    # Find production schedule related jobs
    ps_jobs = [f for f in job_files if "production" in f.name.lower() or "fps" in f.name.lower()]
    print(f"\nProduction schedule jobs: {len(ps_jobs)}")
    for job_file in ps_jobs:
        print(f"  - {job_file.name}")

    # Analyze each job
    print("\n" + "=" * 60)
    print("Jobs referencing target tables:")
    print("=" * 60)

    for job_file in job_files:
        result = analyze_job(job_file)
        if result and result["tables_referenced"]:
            print(f"\n{result['name']} ({result['type']})")
            print(f"  Tables: {', '.join(result['tables_referenced'])}")
            print(f"  Components: {len(result['components'])}")

            for query in result["sql_queries"][:3]:  # Show first 3 queries
                print(f"\n  Component: {query['component']}")
                # Print first 200 chars of SQL
                sql_preview = query['sql'][:200].replace('\n', ' ')
                print(f"  SQL: {sql_preview}...")

    # Search for specific patterns
    print("\n" + "=" * 60)
    print("Searching for JOIN on line_type...")
    print("=" * 60)

    for job_file in job_files:
        with open(job_file) as f:
            content = f.read()
            if "line_type" in content.lower() and "join" in content.lower():
                print(f"\n{job_file.relative_to(EXPORTS_DIR)}")
                # Find the context
                for match in re.finditer(r'.{0,50}line_type.{0,100}', content, re.IGNORECASE):
                    print(f"  ...{match.group()}...")

if __name__ == "__main__":
    main()
```

#### 2. Search for Specific Patterns
```bash
# Find all references to fact_manufacturing_need
grep -r "fact_manufacturing_need" exports/ --include="*.json" -l

# Find JOINs involving line_type
grep -r -i "join.*line_type\|line_type.*join" exports/ --include="*.json"

# Find components that write to the output tables
grep -r "fact_production_schedule" exports/ --include="*.json" | head -20
```

### Success Criteria:

#### Automated Verification:
- [ ] Analysis script runs: `python3 scripts/analyze-jobs.py`
- [ ] Production schedule jobs identified in output
- [ ] SQL queries found that reference target tables
- [ ] `line_type` JOIN patterns located

#### Manual Verification:
- [ ] Review identified SQL queries for logic issues
- [ ] Map out component flow on paper/whiteboard
- [ ] Identify candidate failure points

**Implementation Note**: After completing this phase, document the suspected failing component before proceeding to Phase 3.

---

## CRITICAL FINDING: Root Cause Identified

### Data Flow Chain
```
Google Sheets (ps_staff_by_day)
         ↓
ps_staff_by_day_history
         ↓
ps_staffing_by_dt  ← ← ← ← ← [SUSPECTED FAILURE POINT]
         ↓
ps_automated_ranking_trans → fact_production_schedule
```

### The Failing JOIN
In `ps_automated_ranking_trans`, this JOIN requires `ps_staffing_by_dt` to have data:
```sql
join ${stg2_schema}.ps_staffing_by_dt pl
    on pl.dt = fmn.inserted_dt_utc::date
    and pl.mapped_manufacturing_location = mdch.mapped_manufacturing_location
    and pl.grouped_line_type = 'Automated'
```

**If `ps_staffing_by_dt` has no rows for the current date, the entire production schedule will be empty.**

### Likely Causes (in order of probability)
1. `is_selected_dt` flag not set in `ps_staff_by_day_history`
2. Missing data in Google Sheets "Staff By Day" tab
3. Manufacturing location variable mismatch
4. Timezone/date calculation issue

---

## Phase 3: Data Investigation in Redshift

### Overview
Query Redshift directly to understand the data gap - when did data stop appearing, which upstream tables are empty, and what changed.

### Changes Required:

#### 1. Create Investigation Queries
**File**: `scripts/investigate-data.sql`

```sql
-- PRIORITY 1: Check ps_staffing_by_dt (most likely failure point)
SELECT dt, mapped_manufacturing_location, grouped_line_type, staffing_available
FROM stg2.ps_staffing_by_dt
WHERE dt >= CURRENT_DATE
ORDER BY dt, mapped_manufacturing_location, grouped_line_type;

-- PRIORITY 2: Check source data in ps_staff_by_day_history
SELECT *
FROM stg2.ps_staff_by_day_history
WHERE is_selected_dt
ORDER BY mapped_manufacturing_location, day_of_week_int;

-- PRIORITY 3: Check is_selected_dt flag status
SELECT is_selected_dt, COUNT(*)
FROM stg2.ps_staff_by_day_history
GROUP BY 1;

-- 1. Check fact_production_schedule timeline
SELECT
  DATE(inserted_dt_utc) as schedule_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT line_type) as line_types,
  COUNT(CASE WHEN line_type IS NOT NULL THEN 1 END) as with_line_type
FROM dwh.fact_production_schedule
WHERE inserted_dt_utc >= '2025-01-01'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20;

-- 2. Check fact_manufacturing_need
SELECT
  DATE(runtime_dt_utc) as run_date,
  COUNT(*) as records,
  SUM(manufacturing_need) as total_need
FROM dwh.fact_manufacturing_need
WHERE runtime_dt_utc >= '2025-01-01'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20;

-- 3. Check production line initialization
SELECT
  DATE(inserted_dt_utc) as date,
  manufacturing_location,
  line_type,
  SUM(production_lines) as total_lines
FROM stg2.ps_line_count_history
WHERE inserted_dt_utc >= '2025-01-01'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3
LIMIT 50;

-- 4. Check the specific JOIN condition
-- This traces whether line assignments exist
SELECT
  fps.inserted_dt_utc::date as schedule_date,
  fps.manufacturing_location,
  fps.line_type,
  COUNT(*) as records
FROM dwh.fact_production_schedule fps
WHERE fps.inserted_dt_utc >= '2025-01-07'
  AND fps.line_type IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

-- 5. Find last successful run
SELECT
  runtime_dt_utc,
  COUNT(*) as records,
  COUNT(DISTINCT sku_without_merv_rating) as unique_skus
FROM dwh.fact_production_schedule
WHERE runtime_dt_utc >= '2025-01-01'
  AND line_type IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;
```

#### 2. Redshift Connection Options

**Option A: Via AWS Console**
1. Go to Redshift Query Editor v2
2. Connect to `redshift-cluster-filterbuy`
3. Run investigation queries

**Option B: Via psql (if credentials available)**
```bash
# Get Redshift credentials from Secrets Manager
aws secretsmanager get-secret-value \
  --secret-id analytics/redshift_credentials \
  --query SecretString --output text | jq .

# Connect
psql -h redshift-cluster-filterbuy.xxxxx.us-east-1.redshift.amazonaws.com \
  -p 5439 -U analytics -d filterbuy_dw
```

### Success Criteria:

#### Automated Verification:
- [ ] Can connect to Redshift (via console or CLI)
- [ ] Query 1 returns data showing timeline of records
- [ ] Query 5 identifies last successful run date

#### Manual Verification:
- [ ] Identify exact date data stopped appearing
- [ ] Determine which upstream table is the source of empty data
- [ ] Document findings in `docs/context/investigation-findings.md`

**Implementation Note**: Document findings before proceeding. If root cause is identified, skip to Phase 4.

---

## Phase 4: Root Cause Fix

### Overview
Based on investigation findings, apply a targeted fix in Matillion and verify data flows correctly.

### Potential Fixes (to be determined by investigation):

#### Scenario A: Production Lines Not Initialized
The weekend job that sets production line counts may have failed.

**Fix:**
1. Navigate to `run_production_schedule_v4` in Matillion
2. Find the "Initialize production lines" component
3. Run this component manually for the current period
4. Re-run full orchestration

#### Scenario B: Input Data Issue
Google Sheets values changed in an unexpected way.

**Fix:**
1. Review Google Sheets change history (Jan 7-11)
2. Identify changed values
3. Work with CJ Searcy/Spencer to restore correct values
4. Re-run orchestration

#### Scenario C: Component Logic Error
A specific calculation is producing no results.

**Fix:**
1. Enable debug logging in Matillion
2. Run the failing component in isolation
3. Examine intermediate results
4. Fix the component logic or input

### Success Criteria:

#### Automated Verification:
- [ ] Matillion job completes without errors (existing behavior)
- [ ] `dwh.fact_production_schedule` has new records with `line_type`
- [ ] `dwh.fact_manufacturing_need` has non-zero records

#### Manual Verification:
- [ ] Dashboard shows production schedule for next day
- [ ] Production team confirms data looks correct
- [ ] No alerts in #production_schedule_etl_alerts

**Implementation Note**: Do NOT proceed to Phase 5 until production is confirmed working.

---

## Phase 5: Version Control and Prevention

### Overview
Commit extracted jobs to git and set up basic monitoring to catch future issues early.

### Changes Required:

#### 1. Git Repository Setup
```bash
cd /Users/partiu/workspace/filterbuy-2/matillion

# Initialize git if not already
git init

# Add files
git add docs/
git add exports/
git add scripts/
git add thoughts/

# Initial commit
git commit -m "Initial Matillion job extraction and investigation

- Extracted all jobs from Matillion REST API
- Created analysis scripts for job parsing
- Documented Production Scheduling Tool architecture
- Identified and fixed root cause of empty data

Root cause: [TO BE FILLED]
Fix applied: [TO BE FILLED]"
```

#### 2. Create Monitoring Script
**File**: `scripts/check-production-schedule.sql`

```sql
-- Run daily to verify production schedule is populated
SELECT
  CASE
    WHEN COUNT(*) > 0 THEN 'OK'
    ELSE 'ALERT: No production schedule for tomorrow!'
  END as status,
  COUNT(*) as records_for_tomorrow,
  COUNT(DISTINCT line_type) as line_types
FROM dwh.fact_production_schedule
WHERE DATE(inserted_dt_utc) = CURRENT_DATE + 1
  AND line_type IS NOT NULL;
```

#### 3. Document Root Cause
**File**: `docs/runbook/production-schedule-troubleshooting.md`

```markdown
# Production Schedule Troubleshooting Runbook

## Common Issues

### 1. Empty Production Schedule
**Symptoms:** Dashboard shows "No data", fact_production_schedule empty
**Root Cause:** [TO BE DOCUMENTED]
**Fix:** [TO BE DOCUMENTED]

### 2. Missing Line Type Assignments
**Symptoms:** Records exist but line_type is NULL
**Root Cause:** Production line initialization failed
**Fix:** Re-run "Initialize production lines" component

## Verification Queries
[Include queries from Phase 3]

## Contacts
- Matillion TAM: Kevin Kirkpatrick (kevin.kirkpatrick@matillion.com)
- Original Developer: David Ansel (contact via LinkedIn)
```

### Success Criteria:

#### Automated Verification:
- [ ] Git repository initialized: `git status`
- [ ] Files committed: `git log --oneline | head`
- [ ] Monitoring script syntax valid

#### Manual Verification:
- [ ] Runbook reviewed by Rodrigo
- [ ] Monitoring integrated into alerting system (future)

---

## Testing Strategy

### Unit Tests:
- None (Matillion jobs, not code)

### Integration Tests:
- Run full orchestration and verify output tables

### Manual Testing Steps:
1. Export jobs via API - verify ZIP contents
2. Run analysis script - verify SQL patterns found
3. Execute Redshift queries - verify data timeline
4. After fix, verify dashboard shows data
5. Wait 24 hours and verify next day's schedule appears

## Performance Considerations

- API export may take 5-10 minutes for large projects
- Analysis script is fast (seconds)
- Redshift queries should complete in <30 seconds
- Full Matillion orchestration takes 30-60 minutes

## References

- Confluence: [Production Scheduling Tool v4](https://filterbuy.atlassian.net/wiki/spaces/FA/pages/392331266)
- Confluence: [Data Warehouse ETL](https://filterbuy.atlassian.net/wiki/spaces/FA/pages/389447683)
- Matillion URL: https://matillion.filterbuy.com
- Google Sheets Inputs: https://docs.google.com/spreadsheets/d/1aBLKbfhf2k1R_gv-eWlq5opunWHlT6Fe-VbZVTeAlJ4

## Critical Resources

| Resource | Details |
|----------|---------|
| Matillion UI | https://matillion.filterbuy.com |
| EC2 Instance | `i-0a5182d61a22ede85` |
| SSH Host | `root@ec2-44-193-88-225.compute-1.amazonaws.com` |
| SSH Key Secret | `analytics/matillion_ec2_key` |
| Redshift Cluster | `redshift-cluster-filterbuy` |
| Main Job | `run_production_schedule_v4` |
| Slack Alerts | #production_schedule_etl_alerts |
