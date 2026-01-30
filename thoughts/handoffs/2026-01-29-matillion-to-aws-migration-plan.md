# Migration Plan: Matillion to AWS Native Services

**Document:** Production Scheduling Tool v4 Migration
**Date:** 2026-01-29
**Status:** Draft - Planning Phase
**Author:** Data Engineering Team

---

## Executive Summary

This document outlines the migration strategy for the Production Scheduling Tool from Matillion ETL (EC2-hosted) to AWS-native services. The migration aims to:

1. **Eliminate single points of failure** - Matillion v1.74.5 EOL is April 1, 2026
2. **Improve observability** - Current monitoring has significant gaps
3. **Reduce operational overhead** - Move from GUI-based to infrastructure-as-code
4. **Enhance reliability** - Better error handling and automated recovery

**Current State:** 147 SQL components across 89 jobs, processing ~2,900 rows daily
**Timeline Estimate:** 12-16 weeks for full migration with parallel run
**Risk Level:** Medium-High (production-critical system)

---

## 1. Component Mapping: Matillion → AWS

### 1.1 Orchestration & Workflow

| Matillion Component | Current Implementation | AWS Native Equivalent | Migration Complexity | Notes |
|---------------------|----------------------|----------------------|---------------------|-------|
| Orchestration Job | `run_production_schedule_v4` parent job | **AWS Step Functions** | Medium | State machine with parallel/sequential branches |
| Transformation Job | 89 child transformation jobs | **AWS Step Functions + Glue** | Medium | Each job becomes a Glue job or Lambda |
| Job Scheduler | Matillion built-in (cron) | **Amazon EventBridge** | Low | Rule-based scheduling with cron expressions |
| Job Dependencies | Matillion job flow connectors | **Step Functions workflow** | Low | Native state machine transitions |

### 1.2 Data Processing

| Matillion Component | Current Implementation | AWS Native Equivalent | Migration Complexity | Notes |
|---------------------|----------------------|----------------------|---------------------|-------|
| SQL Query | 147 SQL components | **AWS Glue (PySpark/SQL)** or **Redshift Stored Procedures** | High | Most complex migration - requires SQL refactoring |
| Table Input | Google Sheets loader | **AWS Lambda + Google Sheets API** | Medium | Custom Lambda to pull sheet data |
| Table Output | Redshift write | **Glue/Redshift native** | Low | Direct Redshift integration |
| S3 Load | Not currently used | **Glue S3 connector** | N/A | Future enhancement opportunity |

### 1.3 Control Flow

| Matillion Component | Current Implementation | AWS Native Equivalent | Migration Complexity | Notes |
|---------------------|----------------------|----------------------|---------------------|-------|
| Iterator | `fps_dt_iteration` (31 days) | **Step Functions Map state** | Medium | Parallel or sequential iteration |
| If/Else Conditional | Job enable/disable flags | **Step Functions Choice state** | Low | Native conditional branching |
| Try/Catch | Limited error handling | **Step Functions Catch/Retry** | Low | Better error handling than current |
| Variables | 18 job variables | **Step Functions input/context** or **SSM Parameter Store** | Medium | Centralized parameter management |

### 1.4 Notifications & Monitoring

| Matillion Component | Current Implementation | AWS Native Equivalent | Migration Complexity | Notes |
|---------------------|----------------------|----------------------|---------------------|-------|
| SNS Component | Slack alerts (limited) | **Amazon SNS + Lambda** | Low | Expanded notification channels |
| Email Notification | Manual/ad-hoc | **Amazon SES** | Low | Automated email alerts |
| Slack Integration | `#production_schedule_etl_alerts` | **SNS → Lambda → Slack** | Low | Webhook integration |
| Job Logging | Matillion task history | **CloudWatch Logs** | Low | Centralized, searchable logs |

### 1.5 Data Storage & Security

| Matillion Component | Current Implementation | AWS Native Equivalent | Migration Complexity | Notes |
|---------------------|----------------------|----------------------|---------------------|-------|
| Credentials | Matillion password manager | **AWS Secrets Manager** | Low | Rotation support, IAM integration |
| Environment Config | Matillion environments | **SSM Parameter Store** | Low | Hierarchical parameters |
| Audit Trail | `*_history` tables | **Same pattern + CloudTrail** | Low | Enhanced with AWS audit logs |

---

## 2. Current Monitoring Audit

### 2.1 What IS Currently Monitored ✅

| Check | Location | Alert Channel | Response |
|-------|----------|---------------|----------|
| Critical Error Count | `stg2.ps_fps_errors` | Slack bot | Manual review |
| Production + Changeover > Staffing | `fps_qa` Critical Error 00 | Error table | Job continues |
| Empty Production Schedule | `fps_qa` Critical Error 01 | Error table | Job continues |
| Missing Distribution Center Mapping | `ps_qa_inputs` Error 1 | Error table | Job continues |
| Unknown Production Capacity | `production_capacity_alerting` | Weekly email | Manual fix |
| Input Validation (8 checks) | `ps_qa_inputs` | Error table | Job continues |
| Missing Inputs (2 checks) | `ps_qa_missing_inputs` | Error table | Job continues |

### 2.2 What is NOT Currently Monitored ❌ (GAPS)

| Gap | Risk | Impact | Priority |
|-----|------|--------|----------|
| **Job Execution Failure** | High | Silent failure, no schedule generated | P1 |
| **Job Duration Anomaly** | Medium | Performance degradation undetected | P2 |
| **Row Count Deviation** | High | Partial schedule (Jan 2026 incident: 70% loss) | P1 |
| **Data Freshness** | High | Stale Google Sheets data used | P1 |
| **Schema Changes** | Medium | Column changes break queries | P2 |
| **Upstream Dependency Health** | High | Google Sheets unavailable | P1 |
| **Redshift Cluster Health** | Medium | Query failures, timeouts | P2 |
| **QuickSight SPICE Refresh** | Medium | Dashboard shows stale data | P2 |
| **History Table Growth** | Low | Storage costs, query performance | P3 |
| **Staffing Data Gaps** | Critical | `ps_staffing_by_dt` missing = total failure | P1 |
| **Cross-Table Reconciliation** | Medium | Data integrity issues | P2 |
| **Business Rule Violations** | Medium | Invalid schedules generated | P2 |

### 2.3 Monitoring Gap Analysis Summary

```
Current Coverage:  ~30% of failure modes detected
Target Coverage:   >95% of failure modes detected

Critical Gaps:
├── No job-level failure alerting (relies on manual checking)
├── No row count validation (discovered issues days later)
├── No data freshness checks (stale inputs processed silently)
└── No dependency health monitoring (upstream failures cascade)
```

---

## 3. Enhanced Monitoring Architecture

### 3.1 Job Execution Monitoring

```
┌─────────────────────────────────────────────────────────────────┐
│                    Job Execution Monitoring                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  EventBridge          Step Functions         CloudWatch          │
│  ┌─────────┐         ┌─────────────┐        ┌──────────────┐    │
│  │ Schedule│────────▶│  Workflow   │───────▶│ Metrics      │    │
│  │ Trigger │         │  Execution  │        │ - Duration   │    │
│  └─────────┘         └──────┬──────┘        │ - Status     │    │
│                             │               │ - Step count │    │
│                             ▼               └──────┬───────┘    │
│                      ┌─────────────┐               │            │
│                      │  Execution  │               ▼            │
│                      │  History    │        ┌──────────────┐    │
│                      │  (DynamoDB) │        │ Alarms       │    │
│                      └─────────────┘        │ - Failure    │    │
│                                             │ - Duration   │    │
│                                             │ - Anomaly    │    │
│                                             └──────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Metrics to Track:**
- `job.execution.status` - SUCCESS/FAILED/TIMEOUT
- `job.execution.duration_seconds` - Total runtime
- `job.execution.step_count` - Steps completed
- `job.execution.retry_count` - Retry attempts
- `job.execution.start_time` - Execution start timestamp

**Anomaly Detection:**
- Duration > 2x rolling 7-day average → Alert
- Duration < 0.5x rolling 7-day average → Alert (may indicate early failure)

### 3.2 Data Quality Monitoring

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Quality Monitoring                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  After Each Job Step:                                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                                                          │    │
│  │  1. ROW COUNT CHECK                                      │    │
│  │     ├── Compare to 7-day rolling average                 │    │
│  │     ├── Alert if deviation > 20%                         │    │
│  │     └── Store count in metrics table                     │    │
│  │                                                          │    │
│  │  2. SCHEMA VALIDATION                                    │    │
│  │     ├── Compare column list to baseline                  │    │
│  │     ├── Check data types match expected                  │    │
│  │     └── Alert on any schema drift                        │    │
│  │                                                          │    │
│  │  3. NULL/EMPTY CHECK                                     │    │
│  │     ├── Critical columns must not be NULL                │    │
│  │     ├── Percentage thresholds for optional columns       │    │
│  │     └── Alert if thresholds exceeded                     │    │
│  │                                                          │    │
│  │  4. REFERENTIAL INTEGRITY                                │    │
│  │     ├── Foreign keys must exist in parent tables         │    │
│  │     ├── No orphan records                                │    │
│  │     └── Alert on integrity violations                    │    │
│  │                                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Data Quality Rules for Key Tables:**

| Table | Check | Threshold | Alert |
|-------|-------|-----------|-------|
| `dwh.fact_production_schedule` | Row count | ±20% of 7-day avg | P1 |
| `dwh.fact_production_schedule` | NULL in `production_goal_total` | 0% | P1 |
| `dwh.fact_production_schedule` | `is_within_capacity` = FALSE > 50% | 50% | P2 |
| `stg2.ps_staffing_by_dt` | Row count for next 7 days | > 0 per day | P1 |
| `stg2.ps_automated_ranking` | Row count | ±30% of 7-day avg | P2 |
| All `*_history` tables | `is_selected_dt = TRUE` count | = 1 per entity | P1 |

### 3.3 Business Logic Validation

```sql
-- Post-execution validation queries (run after each schedule generation)

-- Check 1: Production lines don't exceed staffing
SELECT manufacturing_location, SUM(production_lines) as total_lines,
       MAX(lines_available) as staffing_available
FROM dwh.fact_production_schedule
WHERE is_tomorrow_production_schedule = 1
  AND is_within_capacity = 1
GROUP BY manufacturing_location
HAVING SUM(production_lines) > MAX(lines_available) + 0.1;
-- Expected: 0 rows (any rows = ALERT)

-- Check 2: All manufacturing locations have schedules
SELECT ml.manufacturing_location
FROM (SELECT DISTINCT manufacturing_location FROM stg2.ps_staffing_by_dt
      WHERE dt = CURRENT_DATE + 1) ml
LEFT JOIN dwh.fact_production_schedule fps
  ON fps.manufacturing_location = ml.manufacturing_location
  AND fps.is_tomorrow_production_schedule = 1
WHERE fps.manufacturing_location IS NULL;
-- Expected: 0 rows (any rows = ALERT)

-- Check 3: No duplicate rankings
SELECT manufacturing_location, line_type, rank_over_manufacturing_location_line_type, COUNT(*)
FROM dwh.fact_production_schedule
WHERE is_tomorrow_production_schedule = 1
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1;
-- Expected: 0 rows (any rows = ALERT)
```

### 3.4 Infrastructure Monitoring

```
┌─────────────────────────────────────────────────────────────────┐
│                  Infrastructure Monitoring                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Redshift Cluster                                               │
│  ├── CPU Utilization > 80% for 15 min    → P2 Alert            │
│  ├── Disk Space > 80%                     → P2 Alert            │
│  ├── Query Queue Depth > 10               → P3 Alert            │
│  ├── Long Running Queries > 30 min        → P2 Alert            │
│  └── Connection Count > 80% limit         → P2 Alert            │
│                                                                  │
│  QuickSight SPICE                                               │
│  ├── Refresh Failure                      → P2 Alert            │
│  ├── Refresh Duration > 2x normal         → P3 Alert            │
│  └── Dataset Size > 90% quota             → P3 Alert            │
│                                                                  │
│  Google Sheets API                                              │
│  ├── API Errors                           → P1 Alert            │
│  ├── Rate Limiting                        → P2 Alert            │
│  └── Authentication Failures              → P1 Alert            │
│                                                                  │
│  Step Functions                                                 │
│  ├── Execution Throttling                 → P2 Alert            │
│  ├── State Machine Errors                 → P1 Alert            │
│  └── Lambda Timeout                       → P2 Alert            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Alarm System Design

### 4.1 Severity Tiers

| Severity | Definition | Response Time | Notification | Auto-Remediation |
|----------|------------|---------------|--------------|------------------|
| **P1 - Critical** | Production schedule not generated or severely impacted | 15 minutes | Slack + SMS + PagerDuty | Retry 3x, then escalate |
| **P2 - High** | Partial failure or significant data quality issue | 1 hour | Slack + Email | Retry 2x, log for review |
| **P3 - Medium** | Performance degradation or warning threshold | 4 hours | Slack | Log only |
| **P4 - Low** | Informational or minor anomaly | Next business day | Email digest | None |

### 4.2 Specific Alarm Definitions

#### P1 - Critical Alarms

| Alarm Name | Trigger Condition | Notification | Auto-Remediation |
|------------|------------------|--------------|------------------|
| `prod-schedule-job-failed` | Step Functions execution FAILED | Slack #alerts + SMS on-call | Retry 3x with exponential backoff |
| `prod-schedule-empty-output` | `fact_production_schedule` row count = 0 for tomorrow | Slack #alerts + SMS | Halt pipeline, alert immediately |
| `prod-schedule-row-count-critical` | Row count < 50% of 7-day average | Slack #alerts + SMS | Halt pipeline, require manual review |
| `staffing-data-missing` | `ps_staffing_by_dt` has no rows for next 3 days | Slack #alerts + SMS | Trigger staffing initialization job |
| `google-sheets-auth-failed` | Google Sheets API 401/403 error | Slack #alerts + SMS | Attempt token refresh |

#### P2 - High Alarms

| Alarm Name | Trigger Condition | Notification | Auto-Remediation |
|------------|------------------|--------------|------------------|
| `prod-schedule-row-count-warning` | Row count 20-50% below 7-day average | Slack #alerts + Email | Log for review |
| `prod-schedule-duration-anomaly` | Duration > 2x 7-day average | Slack #alerts | Log performance metrics |
| `capacity-overflow-high` | > 30% of items marked threshold exceeded | Slack #alerts | None (business decision) |
| `data-freshness-stale` | Input table `is_selected_dt` older than 24 hours | Slack #alerts | Trigger data refresh |
| `redshift-query-timeout` | Any query exceeds 30 minute timeout | Slack #alerts | Cancel query, retry with optimization |
| `quicksight-refresh-failed` | SPICE refresh returns error | Slack #alerts | Retry refresh |

#### P3 - Medium Alarms

| Alarm Name | Trigger Condition | Notification | Auto-Remediation |
|------------|------------------|--------------|------------------|
| `prod-schedule-duration-slow` | Duration 1.5-2x 7-day average | Slack #monitoring | None |
| `schema-drift-detected` | Column added/removed from monitored table | Slack #monitoring | Log change |
| `history-table-growth` | History table > 10,000 rows | Slack #monitoring | None |
| `redshift-cpu-high` | CPU > 80% for 15 minutes | Slack #monitoring | None |

#### P4 - Low Alarms

| Alarm Name | Trigger Condition | Notification | Auto-Remediation |
|------------|------------------|--------------|------------------|
| `job-completed-successfully` | Daily job completes without errors | Email digest | None |
| `weekly-metrics-summary` | Weekly aggregation of all metrics | Email digest | None |

### 4.3 Alarm Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Alarm Flow Architecture                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   CloudWatch Metrics          CloudWatch Alarms           SNS Topics     │
│   ┌─────────────┐            ┌─────────────┐            ┌────────────┐  │
│   │ Job Metrics │───────────▶│ Alarm Rules │───────────▶│ P1-Critical│  │
│   │ Data Quality│            │ Thresholds  │            │ P2-High    │  │
│   │ Infra Stats │            │ Anomaly Det │            │ P3-Medium  │  │
│   └─────────────┘            └─────────────┘            │ P4-Low     │  │
│                                                          └─────┬──────┘  │
│                                                                │         │
│         ┌──────────────────────────────────────────────────────┘         │
│         │                                                                │
│         ▼                                                                │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    Lambda: Alert Router                          │   │
│   │  ┌─────────────────────────────────────────────────────────┐    │   │
│   │  │ 1. Parse alarm payload                                   │    │   │
│   │  │ 2. Enrich with context (job name, metrics, runbook link)│    │   │
│   │  │ 3. Route to appropriate channels based on severity       │    │   │
│   │  │ 4. Trigger auto-remediation if configured                │    │   │
│   │  │ 5. Log to incident tracking system                       │    │   │
│   │  └─────────────────────────────────────────────────────────┘    │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│         │                                                                │
│         ▼                                                                │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│   │   Slack     │  │   Email     │  │  PagerDuty  │  │  DynamoDB   │   │
│   │  #alerts    │  │  On-call    │  │  Escalation │  │  Incident   │   │
│   │  #monitoring│  │  Team       │  │             │  │  Log        │   │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Target Architecture

### 5.1 High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Production Scheduling Tool - AWS Native Architecture          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────┐                                                            │
│  │  DATA SOURCES   │                                                            │
│  ├─────────────────┤                                                            │
│  │ Google Sheets   │──┐                                                         │
│  │ (Staffing,      │  │                                                         │
│  │  Config)        │  │                                                         │
│  └─────────────────┘  │     ┌─────────────────────────────────────────────┐    │
│                       │     │           ORCHESTRATION LAYER               │    │
│  ┌─────────────────┐  │     ├─────────────────────────────────────────────┤    │
│  │ Redshift        │  │     │                                             │    │
│  │ (Inventory,     │──┼────▶│  ┌─────────────┐    ┌─────────────────┐    │    │
│  │  Sales Data)    │  │     │  │ EventBridge │───▶│ Step Functions  │    │    │
│  └─────────────────┘  │     │  │ (Scheduler) │    │ (Orchestrator)  │    │    │
│                       │     │  └─────────────┘    └────────┬────────┘    │    │
│  ┌─────────────────┐  │     │                              │             │    │
│  │ Supplybuy API   │──┘     │                              ▼             │    │
│  │ (Demand Data)   │        │  ┌───────────────────────────────────────┐ │    │
│  └─────────────────┘        │  │         PROCESSING LAYER              │ │    │
│                             │  ├───────────────────────────────────────┤ │    │
│                             │  │                                       │ │    │
│                             │  │  ┌─────────┐  ┌─────────┐  ┌───────┐ │ │    │
│                             │  │  │ Lambda  │  │  Glue   │  │Redshift│ │ │    │
│                             │  │  │ (Light) │  │ (Heavy) │  │  SP   │ │ │    │
│                             │  │  └─────────┘  └─────────┘  └───────┘ │ │    │
│                             │  │       │            │           │      │ │    │
│                             │  │       └────────────┼───────────┘      │ │    │
│                             │  │                    ▼                  │ │    │
│                             │  │            ┌─────────────┐            │ │    │
│                             │  │            │  Redshift   │            │ │    │
│                             │  │            │  (DWH)      │            │ │    │
│                             │  │            └─────────────┘            │ │    │
│                             │  └───────────────────────────────────────┘ │    │
│                             └─────────────────────────────────────────────┘    │
│                                                      │                          │
│                                                      ▼                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                        MONITORING LAYER                                  │   │
│  ├─────────────────────────────────────────────────────────────────────────┤   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │   │
│  │  │ CloudWatch  │  │ CloudWatch  │  │    SNS      │  │  Lambda     │    │   │
│  │  │ Logs        │  │ Metrics     │  │  Topics     │  │  (Router)   │    │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘    │   │
│  │         │                │                │                │           │   │
│  │         └────────────────┴────────────────┴────────────────┘           │   │
│  │                                   │                                     │   │
│  │                                   ▼                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │   │
│  │  │   Slack     │  │   Email     │  │  PagerDuty  │  │  DynamoDB   │    │   │
│  │  │  Alerts     │  │  Digest     │  │  On-Call    │  │  Incidents  │    │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                      │                          │
│                                                      ▼                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                        CONSUMPTION LAYER                                 │   │
│  ├─────────────────────────────────────────────────────────────────────────┤   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │   │
│  │  │   QuickSight    │  │   API Gateway   │  │   S3 Export     │         │   │
│  │  │   Dashboard     │  │   (Future API)  │  │   (Backup)      │         │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Step Functions State Machine Design

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Production Scheduling - Step Functions Workflow               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  START                                                                          │
│    │                                                                            │
│    ▼                                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐           │
│  │ PHASE 1: Data Ingestion (Parallel)                              │           │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │           │
│  │  │Load Staffing │ │Load Config   │ │Load Demand   │            │           │
│  │  │(Lambda)      │ │(Lambda)      │ │(Glue)        │            │           │
│  │  └──────────────┘ └──────────────┘ └──────────────┘            │           │
│  └─────────────────────────────────────────────────────────────────┘           │
│    │                                                                            │
│    ▼                                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐           │
│  │ PHASE 2: Data Validation                                        │           │
│  │  ┌──────────────────────────────────────────────────────────┐  │           │
│  │  │ Validate Inputs (Lambda)                                  │  │           │
│  │  │ - Check row counts                                        │  │           │
│  │  │ - Verify is_selected_dt flags                             │  │           │
│  │  │ - Confirm staffing exists for next 7 days                 │  │           │
│  │  └──────────────────────────────────────────────────────────┘  │           │
│  └─────────────────────────────────────────────────────────────────┘           │
│    │                                                                            │
│    ▼                                                                            │
│  ┌─────────────┐                                                               │
│  │ CHOICE:     │──── Validation Failed ────▶ [Alert & Abort]                   │
│  │ Valid?      │                                                               │
│  └──────┬──────┘                                                               │
│         │ Yes                                                                   │
│         ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐           │
│  │ PHASE 3: Demand Calculation (Glue Job)                          │           │
│  │  - ps_fact_manufacturing_need                                   │           │
│  │  - ps_sales_forecast                                            │           │
│  │  - ps_excess_inventory                                          │           │
│  └─────────────────────────────────────────────────────────────────┘           │
│    │                                                                            │
│    ▼                                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐           │
│  │ PHASE 4: Date Iteration (Map State - 31 iterations)             │           │
│  │  ┌──────────────────────────────────────────────────────────┐  │           │
│  │  │ For each date (1-31):                                     │  │           │
│  │  │   ├── Automated Production (Glue/Redshift SP)             │  │           │
│  │  │   ├── Automated Reschedule (Glue/Redshift SP)             │  │           │
│  │  │   ├── Non-Automated Reactive (Glue/Redshift SP)           │  │           │
│  │  │   ├── Non-Automated Proactive (Glue/Redshift SP)          │  │           │
│  │  │   └── Line Rounding (Glue/Redshift SP)                    │  │           │
│  │  └──────────────────────────────────────────────────────────┘  │           │
│  └─────────────────────────────────────────────────────────────────┘           │
│    │                                                                            │
│    ▼                                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐           │
│  │ PHASE 5: Post-Processing                                        │           │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │           │
│  │  │Write to DWH  │ │Run QA Checks │ │Archive Old   │            │           │
│  │  │(Redshift)    │ │(Lambda)      │ │Data          │            │           │
│  │  └──────────────┘ └──────────────┘ └──────────────┘            │           │
│  └─────────────────────────────────────────────────────────────────┘           │
│    │                                                                            │
│    ▼                                                                            │
│  ┌─────────────┐                                                               │
│  │ CHOICE:     │──── QA Failed ────▶ [Alert with details]                      │
│  │ QA Pass?    │                     [Continue - schedule still usable]        │
│  └──────┬──────┘                                                               │
│         │ Yes                                                                   │
│         ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐           │
│  │ PHASE 6: Publish                                                │           │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │           │
│  │  │Refresh SPICE │ │Send Success  │ │Log Metrics   │            │           │
│  │  │(QuickSight)  │ │Notification  │ │(CloudWatch)  │            │           │
│  │  └──────────────┘ └──────────────┘ └──────────────┘            │           │
│  └─────────────────────────────────────────────────────────────────┘           │
│    │                                                                            │
│    ▼                                                                            │
│  END (Success)                                                                  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Migration Phases

### Phase 1: Foundation (Weeks 1-3)

**Objectives:**
- Set up AWS infrastructure
- Establish monitoring baseline
- Create development environment

**Tasks:**
| Task | Owner | Duration | Dependencies |
|------|-------|----------|--------------|
| Create AWS Step Functions skeleton | DE Team | 3 days | None |
| Set up CloudWatch Log Groups | DE Team | 1 day | None |
| Create SNS topics (P1-P4) | DE Team | 1 day | None |
| Build Lambda alert router | DE Team | 2 days | SNS topics |
| Configure Slack integration | DE Team | 1 day | SNS topics |
| Set up SSM Parameter Store | DE Team | 1 day | None |
| Migrate secrets to Secrets Manager | DE Team | 1 day | None |
| Create baseline monitoring dashboard | DE Team | 2 days | CloudWatch |
| Document current Matillion job metrics | DE Team | 2 days | None |

**Deliverables:**
- [ ] Step Functions state machine (empty shell)
- [ ] CloudWatch dashboard with baseline metrics
- [ ] Alert routing Lambda
- [ ] Slack channel integration
- [ ] Parameter Store configuration

### Phase 2: Component Migration (Weeks 4-8)

**Objectives:**
- Migrate SQL components to Glue/Redshift SPs
- Build Lambda functions for light processing
- Implement data quality checks

**Tasks:**
| Task | Owner | Duration | Dependencies |
|------|-------|----------|--------------|
| Convert data loaders to Lambda | DE Team | 5 days | Phase 1 |
| Migrate demand calculation SQL to Glue | DE Team | 5 days | Phase 1 |
| Migrate automated production SQL | DE Team | 5 days | Demand calc |
| Migrate non-automated SQL | DE Team | 5 days | Automated |
| Migrate QA checks to Lambda | DE Team | 3 days | All SQL |
| Build row count validation Lambda | DE Team | 2 days | Phase 1 |
| Build schema validation Lambda | DE Team | 2 days | Phase 1 |
| Create unit tests for each component | DE Team | 5 days | Ongoing |
| Integration testing | DE Team | 3 days | All components |

**Deliverables:**
- [ ] All 89 jobs migrated to AWS equivalents
- [ ] Data quality validation Lambdas
- [ ] Unit test suite
- [ ] Integration test results

### Phase 3: Parallel Run (Weeks 9-12)

**Objectives:**
- Run both systems simultaneously
- Compare outputs for accuracy
- Fine-tune alerting thresholds

**Tasks:**
| Task | Owner | Duration | Dependencies |
|------|-------|----------|--------------|
| Deploy Step Functions to production | DE Team | 1 day | Phase 2 |
| Configure EventBridge schedule (offset) | DE Team | 1 day | Step Functions |
| Build comparison Lambda | DE Team | 3 days | Both systems running |
| Daily output comparison | DE Team | 20 days | Comparison Lambda |
| Tune alert thresholds based on data | DE Team | Ongoing | Parallel run data |
| Document discrepancies and fixes | DE Team | Ongoing | Comparison results |
| Stakeholder review of new dashboards | Product | 2 days | Week 10 |

**Success Criteria for Parallel Run:**
- [ ] AWS output matches Matillion output within 0.1% for 10 consecutive days
- [ ] No P1 alerts triggered during parallel run
- [ ] All stakeholders approve new dashboards

### Phase 4: Cutover (Week 13)

**Objectives:**
- Switch primary system to AWS
- Keep Matillion as backup
- Monitor closely

**Tasks:**
| Task | Owner | Duration | Dependencies |
|------|-------|----------|--------------|
| Final parallel run validation | DE Team | 1 day | 10-day match |
| Disable Matillion schedule | DE Team | 1 hour | Validation passed |
| Set AWS as primary | DE Team | 1 hour | Matillion disabled |
| Update QuickSight to point to AWS | DE Team | 1 hour | AWS primary |
| Monitor first 3 production runs | DE Team | 3 days | Cutover |
| Document rollback procedure | DE Team | 1 day | Before cutover |

**Rollback Criteria:**
- P1 alert triggered during first 3 days → Rollback
- Row count deviation > 10% → Rollback
- Stakeholder escalation → Evaluate rollback

**Rollback Procedure:**
1. Re-enable Matillion schedule in Matillion UI
2. Disable EventBridge rule for Step Functions
3. Revert QuickSight data source
4. Post-mortem within 24 hours

### Phase 5: Decommission (Weeks 14-16)

**Objectives:**
- Fully decommission Matillion
- Archive historical data
- Update documentation

**Tasks:**
| Task | Owner | Duration | Dependencies |
|------|-------|----------|--------------|
| Export Matillion job definitions | DE Team | 1 day | Cutover stable |
| Archive to S3 | DE Team | 1 day | Export complete |
| Disable Matillion EC2 instance | DE Team | 1 hour | Archive complete |
| Update runbooks and documentation | DE Team | 3 days | Cutover stable |
| Knowledge transfer sessions | DE Team | 2 days | Documentation |
| Delete Matillion EC2 (after 30 days) | DE Team | 1 hour | 30-day wait |

---

## 7. Cost-Benefit Analysis

### Current Costs (Matillion)

| Item | Monthly Cost | Annual Cost |
|------|-------------|-------------|
| Matillion EC2 (m5.xlarge) | $150 | $1,800 |
| Matillion License | $500 | $6,000 |
| Operational overhead (est. 4 hrs/week) | $800 | $9,600 |
| **Total** | **$1,450** | **$17,400** |

### Projected Costs (AWS Native)

| Item | Monthly Cost | Annual Cost |
|------|-------------|-------------|
| Step Functions (1M state transitions) | $25 | $300 |
| Lambda (data loaders, validation) | $10 | $120 |
| Glue (ETL jobs, ~30 DPU-hours/day) | $200 | $2,400 |
| CloudWatch (logs, metrics, alarms) | $50 | $600 |
| SNS (notifications) | $5 | $60 |
| Secrets Manager | $5 | $60 |
| Operational overhead (est. 2 hrs/week) | $400 | $4,800 |
| **Total** | **$695** | **$8,340** |

### Cost Comparison

| Metric | Matillion | AWS Native | Savings |
|--------|-----------|------------|---------|
| Annual Infrastructure | $7,800 | $3,540 | $4,260 (55%) |
| Annual Operations | $9,600 | $4,800 | $4,800 (50%) |
| **Total Annual** | **$17,400** | **$8,340** | **$9,060 (52%)** |

### Non-Financial Benefits

| Benefit | Impact |
|---------|--------|
| Eliminated EOL risk | Matillion v1.74.5 EOL April 2026 |
| Improved observability | 30% → >95% failure detection |
| Reduced MTTR | Hours → Minutes with auto-remediation |
| Infrastructure as Code | Version control, reproducibility |
| Scalability | Auto-scaling vs fixed EC2 |
| Team skill alignment | AWS skills more transferable |

---

## 8. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| SQL logic errors during migration | Medium | High | Extensive parallel run, unit tests |
| Performance degradation in Glue | Medium | Medium | Optimize SQL, consider Redshift SPs |
| Cutover timing conflicts with production | Low | High | Schedule cutover for weekend |
| Team unfamiliar with Step Functions | Medium | Medium | Training, documentation, pair programming |
| Hidden Matillion features missed | Medium | High | Comprehensive audit, parallel run validation |
| Google Sheets API rate limiting | Low | Medium | Implement caching, exponential backoff |
| Stakeholder resistance | Low | Medium | Early involvement, demo new dashboards |

---

## 9. Success Criteria

### Technical Success
- [ ] All 89 jobs successfully migrated
- [ ] 10+ consecutive days of matching output during parallel run
- [ ] <1% row count deviation from Matillion baseline
- [ ] All P1/P2 alarms tested and functional
- [ ] MTTR for P1 incidents < 30 minutes

### Operational Success
- [ ] Zero production schedule misses during migration
- [ ] Operations team trained on new system
- [ ] Runbooks updated and reviewed
- [ ] On-call rotation established

### Business Success
- [ ] Stakeholder sign-off on new dashboards
- [ ] No degradation in schedule quality
- [ ] Cost savings realized within 6 months

---

## 10. Appendix

### A. Runbook Templates

#### Runbook: P1 - Empty Production Schedule

```
ALERT: prod-schedule-empty-output
SEVERITY: P1 - Critical
RESPONSE TIME: 15 minutes

SYMPTOMS:
- fact_production_schedule has 0 rows for tomorrow's date
- QuickSight dashboard shows no data

DIAGNOSTIC STEPS:
1. Check Step Functions execution history
   aws stepfunctions list-executions --state-machine-arn <ARN> --max-results 5

2. Review CloudWatch Logs for errors
   aws logs filter-log-events --log-group-name /aws/stepfunctions/prod-schedule

3. Verify input data exists
   SELECT COUNT(*) FROM stg2.ps_staffing_by_dt WHERE dt = CURRENT_DATE + 1;

4. Check for validation failures
   SELECT * FROM stg2.ps_fps_errors WHERE runtime_dt_utc::date = CURRENT_DATE;

REMEDIATION:
- If staffing data missing: Trigger Initialize_ps_staffing_by_dt job
- If validation failure: Review error details, fix source data, re-run
- If infrastructure issue: Check Redshift cluster health, restart if needed

ESCALATION:
- If not resolved in 30 minutes: Page secondary on-call
- If not resolved in 60 minutes: Page engineering manager
```

### B. SQL Migration Checklist

| SQL File | Complexity | Target Service | Status |
|----------|------------|----------------|--------|
| 0145_fps_automated_trans_SQL_0.sql | High | Redshift SP | ⬜ |
| 0149_ps_automated_ranking_trans_SQL_0.sql | High | Glue | ⬜ |
| 0159_fps_non_automated_trans_SQL_0.sql | High | Redshift SP | ⬜ |
| ... (147 total files) | | | |

### C. Contact Information

| Role | Name | Contact |
|------|------|---------|
| Project Lead | TBD | |
| Technical Lead | TBD | |
| On-Call Primary | TBD | |
| On-Call Secondary | TBD | |
| Stakeholder (Operations) | CJ Searcy | |
| Stakeholder (Configuration) | Spencer Nedved | |

---

*Document Version: 1.0*
*Last Updated: 2026-01-29*
*Next Review: 2026-02-05*
