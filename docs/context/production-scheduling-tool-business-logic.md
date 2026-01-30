# Production Schedule v4 - Business Logic Documentation

> **Source:** Production Schedule v4 Dashboard Documentation (Google Doc)
> **Last Updated:** January 2026

---

## Overview

The production scheduling tool uses a series of rules to identify:
- Which products to manufacture
- How many to manufacture
- What line type a product should be manufactured on
- What the rank of each product should be

To facilitate staffing decisions and production planning, the tool does this for future dates. The tool uses variables that may be controlled by an Operations supervisor to fit production needs.

### High-Level Process

1. **Calculate demand** for each product and distribution center
2. **Calculate excess inventory** distribution
3. **Create automated production schedule**
4. **Create non-automated production schedule**

---

## #1 - Demand Calculation

### Basic Formula

Demand is calculated for each product, distribution center, and day (days 1 through 31).

```
Demand = Sales for N days - Available Quantity

Available Quantity = Quantity in stock - Quantity on order + Quantity in transit
```

**Example:** If the 5-day sales are 2,000, 1,000 are in stock, 250 are on order, and 500 are in transit, the 5-day demand is 750.

### Demand Calculation Methods (as of October 2025)

#### 1. Business to Consumer (B2C) Demand
- Majority of demand
- Calculated using a **linear projection based on sales from the prior 28 days**

#### 2. Business to Business (B2B) Demand
Two methods:
1. Linear projection (same as B2C)
2. Pattern detection for large wholesale orders on regular cadence outside the 28-day period

#### 3. Retail / FBA Demand
- **Walmart:** Average quantity per SKU over prior 3 weeks
- **Amazon FBA:** Average quantity per SKU from last 5 orders
- Estimated order date = most frequent ordering interval

---

## #2 - Excess Inventory Distribution

- **Excess inventory** = inventory at a manufacturing location above target level that is needed at distribution center(s)
- May be used to reduce distribution center demand (configurable by Operations supervisor)

**Example:** Fresno needs 1,500 AFB20x20x1M8. If Ogden has 700 excess of this SKU, Fresno's demand becomes 800.

---

## #3 - Automated Production

After demand is calculated and excess inventory adjustments are made, an automated production schedule is created.

### Key Characteristics
- Automated schedules **completely fill an automated line's daily capacity**
- Example: If 3,000 20x20x1 filters can be made on an automated line in a 10-hour shift, 3,000 filters are scheduled

### Automated Ranking

SKUs are ranked based on:
1. **Days of production need (ascending)**
2. **Production need (descending)**

Only days where production need >= production capacity are considered.

**Example:**

| Size | 2-Day Need | 3-Day Need | 4-Day Need | Rank |
|------|------------|------------|------------|------|
| AFB16x20x1 | **3,100** | 6,200 | 9,300 | 1 |
| AFB20x25x1 | 1,900 | **3,800** | 5,700 | 3 |
| AFB16x25x1 | 2,000 | **4,000** | 6,000 | 2 |

- 16x20x1 ranked 1st: only size with 2-day need >= capacity
- 16x25x1 ranked 2nd: 3-day need > 20x25x1's 3-day need

### Automated Re-Scheduling (as of March 2025)

- Automated sizes are **excluded from non-automated production**
- Automated schedule is **generated once per week**
- Schedule is re-organized to minimize changes between days
  - Example: If 20x20x1 is scheduled for Monday and Wednesday, it's rescheduled to Monday and Tuesday

### Automated Staff Reassignment

When automated production isn't needed to meet target inventory goals:
- May result in inconsistent automated staff over a week
- Automated staff are reassigned to non-automated lines to prevent inconsistent staffing

**Example:** Monday/Tuesday have 2 automated lines. Wednesday/Thursday have 1 automated line. An additional non-automated line is run by automated staff on Wednesday/Thursday.

---

## #4 - Non-Automated Production

Created after demand calculation, excess inventory adjustment, and automated schedule adjustment.

### Two Types of Demand

| Type | Description |
|------|-------------|
| **Reactive** | Product is out of stock or about to run out |
| **Proactive** | Product falls below target days of inventory |

### Reactive Production

Products that meet both criteria:
1. Less than N days of inventory remaining
2. X-day demand greater than Y

**Example:** Products with < 2 days inventory AND 5-day need > 30

#### Reactive Ranking

Ranked by (in order):
1. Is out of stock at distribution center(s)
2. Is out of stock at manufacturing location
3. Days of inventory (ascending)
4. Production need (descending)

**Example:**

| SKU | Location | Is Out of Stock | Days Remaining | 5-Day Need | Rank |
|-----|----------|-----------------|----------------|------------|------|
| AFB20x20x1M8 | Fresno, CA | **Yes** | 0 | 750 | 1 |
| AFB16x25x1M11 | Fresno, CA | No | 1 | 1,200 | 4 |
| AFB15x25x1M13 | Ogden, UT | **Yes** | 0 | 500 | 2 |
| AFB15x25x1M11 | Ogden, UT | No | 1 | **2,000** | 3 |

### Proactive Production

- Ranked by **production need descending**
- Production needs aggregated across all distribution centers fed by manufacturing location
- Production need aggregated across all MERV ratings for a size

#### Proactive Ranking Example

| SKU | SKU without MERV | Location | 21-Day Need | Rank |
|-----|------------------|----------|-------------|------|
| AFB16x25x1M8 | AFB16x25x1 | Ogden, UT | 750 | 3 |
| AFB16x25x1M11 | AFB16x25x1 | Fresno, CA | 250 | 3 |
| AFB16x25x1M8 | AFB16x25x1 | Fresno, CA | 1,000 | 3 |
| AFB20x20x1M8 | AFB20x20x1 | Ogden, UT | 4,200 | 2 |
| AFB15x25x1M13 | AFB15x25x1 | Ogden, UT | 750 | 1 |
| AFB15x25x1M8 | AFB15x25x1 | Fresno, CA | 3,500 | 1 |

- AFB15x25x1: 4,250 total (Rank 1)
- AFB20x20x1: 4,200 total (Rank 2)
- AFB16x25x1: 2,000 total (Rank 3)

---

## Non-Automated Line Type Assignment

Products are assigned based on:
1. Product's rank
2. Remaining capacity on a line type
3. Efficiency of making a product on each line type

**Assignment Rule:** Products go to (1) the most efficient line type that (2) still has capacity (3) based on their ranking.

---

## Line Types

| Line Type | Description |
|-----------|-------------|
| **Automated** | Fully automated production lines |
| **Single Loader** | Non-automated, single loader |
| **Double Loader** | Non-automated, double loader |
| **Manual** | Manual production |
| **Automated Production Threshold Exceeded** | Product exceeded max automated lines allowed |
| **Non-Automated Production Threshold Exceeded** | Product exceeded max non-automated lines allowed |
| **Non-Automated Reactive Threshold Not Meet** | Product didn't meet minimum production goal for reactive logic |

---

## QuickSight Controls

**Important:** QuickSight controls **DO NOT** influence the production schedule in the data warehouse.

- Users can exclude sizes from the QuickSight dashboard display
- This changes what's **shown** but **does not update** the actual production schedule
- Manufacturing based on QuickSight display (not actual schedule) will impact production performance

---

## QuickSight vs Supplybuy Differences

### Supplybuy Production Page Issues (as of February 2025)

1. **Multi-location calculation error:** Calculates demand based on combined inventory of all selected locations, obscuring location-specific needs
2. **In-transit inventory:** Not accounted for when multiple locations selected

### Sales Calculation Differences

| Aspect | Supplybuy | QuickSight |
|--------|-----------|------------|
| Sales forecast | Linear, prior 28 days | Linear, prior 28 days |
| Includes | Shipments + missed opportunities | Shipments + missed opportunities |
| Rounding | **Rounds to nearest whole number** | **Includes decimal points** |

---

## Special Cases

### SKU Assigned to Multiple Line Types

This is intentional in these situations:

1. **Automated + Non-Automated:** When demand exceeds automated capacity
2. **Multiple Non-Automated Types:** When most efficient line type lacks capacity for full demand
3. **Capacity Overflow:** Products outside capacity are assigned to all possible line types for QuickSight flexibility

**Important:** Production MUST occur on the line types specified.

### SKU With Multiple Ranks

For the same manufacturing location and line type, a SKU may have multiple ranks because:
- Products may have different urgencies at different distribution centers
- Reactive and Proactive production may rank the same product differently

**Example:**
- 15x25x1 urgently needed in Orlando (Reactive) → Rank 2 on Double Loader
- 15x25x1 below target in Talladega (Proactive) → Rank 6 on Double Loader

**Important:** Manufacture products **in the order they are ranked** to meet demand adequately.
