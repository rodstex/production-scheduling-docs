---
name: investigate-quicksight-dataset
description: Investigate a QuickSight dataset and generate context documentation
allowed-tools: [Bash, Read, Write, Glob, Grep]
user-invocable: true
arguments: dataset_name
---

# Investigate QuickSight Dataset

Use this skill to investigate a QuickSight dataset and generate documentation for future reference.

## Input

The skill receives a dataset name (or partial name) as input. Fuzzy matching is supported.

**Example invocations:**
- `/investigate-quicksight-dataset production_schedule`
- `/investigate-quicksight-dataset fact_manufacturing`
- `/investigate-quicksight-dataset staff`

## Prerequisites

- AWS credentials configured via `aws-vault` with profile `rt`
- QuickSight access permissions

## Execution Steps

### Step 1: Get AWS Account ID

```bash
AWS_ACCOUNT_ID=$(aws-vault exec rt -- aws sts get-caller-identity --query Account --output text)
echo "Account ID: $AWS_ACCOUNT_ID"
```

### Step 2: List All Datasets and Find Matching Ones

Search for datasets matching the input name (case-insensitive):

```bash
aws-vault exec rt -- aws quicksight list-data-sets \
  --aws-account-id "$AWS_ACCOUNT_ID" \
  --query "DataSetSummaries[?contains(Name, 'SEARCH_TERM') || contains(Name, 'search_term')]" \
  --output json
```

If no exact match, list all datasets and filter with `jq` or `grep`:

```bash
aws-vault exec rt -- aws quicksight list-data-sets \
  --aws-account-id "$AWS_ACCOUNT_ID" \
  --output json | jq -r '.DataSetSummaries[] | "\(.DataSetId)\t\(.Name)"' | grep -i "SEARCH_TERM"
```

### Step 3: Describe the Dataset

Once you have the DataSetId, get full details:

```bash
aws-vault exec rt -- aws quicksight describe-data-set \
  --aws-account-id "$AWS_ACCOUNT_ID" \
  --data-set-id "DATASET_ID" \
  --output json
```

### Step 4: Extract Key Information

From the dataset description, extract:

1. **Dataset Name & ID**
2. **Physical Table Map** - Source tables/queries
3. **Logical Table Map** - Transformations and joins
4. **Column Groups** - Field groupings
5. **Data Set Usage Configuration** - Row-level security, etc.
6. **Import Mode** - SPICE or Direct Query

### Step 5: Generate Context Document

Create a markdown document at `docs/context/quicksight-dataset-{name}.md` with:

```markdown
# QuickSight Dataset: {Dataset Name}

**Dataset ID:** `{id}`
**Import Mode:** {SPICE/DIRECT_QUERY}
**Last Updated:** {date from CreatedTime/LastUpdatedTime}

## Overview

{Brief description of what this dataset contains and its purpose}

## Source Tables

| Source | Type | Details |
|--------|------|---------|
| {table/query name} | {CustomSql/RelationalTable} | {schema.table or SQL summary} |

## Columns

| Column Name | Type | Description |
|-------------|------|-------------|
| {name} | {STRING/INTEGER/DECIMAL/DATETIME} | {inferred purpose} |

## Transformations

{List any calculated fields, joins, or filters applied}

## Data Lineage

```
{Source} -> {Dataset} -> {Dashboards/Analyses that use it}
```

## Related Datasets

{Other datasets that share sources or are commonly used together}

## Investigation Notes

{Any findings, issues, or observations discovered during investigation}
```

## Useful Commands Reference

### List all datasets
```bash
aws-vault exec rt -- aws quicksight list-data-sets --aws-account-id "$AWS_ACCOUNT_ID" --output table
```

### List dashboards using a dataset
```bash
aws-vault exec rt -- aws quicksight list-dashboards --aws-account-id "$AWS_ACCOUNT_ID" --output json
```

### Describe a dashboard
```bash
aws-vault exec rt -- aws quicksight describe-dashboard --aws-account-id "$AWS_ACCOUNT_ID" --dashboard-id "DASHBOARD_ID"
```

### List data sources
```bash
aws-vault exec rt -- aws quicksight list-data-sources --aws-account-id "$AWS_ACCOUNT_ID" --output json
```

### Describe a data source
```bash
aws-vault exec rt -- aws quicksight describe-data-source --aws-account-id "$AWS_ACCOUNT_ID" --data-source-id "DATASOURCE_ID"
```

## Error Handling

### AccessDeniedException
You may need additional QuickSight permissions. Contact admin to grant:
- `quicksight:ListDataSets`
- `quicksight:DescribeDataSet`
- `quicksight:ListDashboards`

### ResourceNotFoundException
The dataset ID may be incorrect. Re-run the list command to verify.

### Throttling
If you hit rate limits, wait a few seconds between API calls.

## Output

After completing the investigation, you will have:
1. A context document at `docs/context/quicksight-dataset-{name}.md`
2. Understanding of the dataset's structure, sources, and purpose
3. Notes on any issues or observations for future reference
