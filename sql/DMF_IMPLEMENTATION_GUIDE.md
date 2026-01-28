# Snowflake Data Metric Functions (DMF) Implementation Guide

A comprehensive guide to implementing automated data quality checks using Snowflake's Data Metric Functions. Use this document to add DMF-based monitoring to any report in your data pipeline.

**Who this is for:** Report owners, data engineers, and data quality teams who want to add automated, scheduled DQ checks (source vs target count, min/max bounds, null checks, etc.) to Snowflake tables or views.

**Markdown:** Compatible with Azure DevOps Wiki. The table of contents uses **[[_TOC_]]** for auto-generated section links.

---

## Table of Contents

[[_TOC_]]

---

## 1. What Are DMFs?

**Data Metric Functions (DMFs)** are Snowflake's built-in way to measure data quality on tables and views. They:

- Run on a **schedule** (cron, interval, or on changes)
- Compute **numeric metrics** (e.g. row count, min, max, null count, custom logic)
- Support **expectations**: pass/fail rules (e.g. "count difference = 0", "min >= 0")
- Store results in **event tables** for monitoring and alerting

**Key constraint:** DMFs accept **at most 2 TABLE arguments**. Your target is the first; any extra input (e.g. source count) must be the second — typically via a **view** that encapsulates the logic.

```text
+-----------------------------------------------------------------------+
|  DMF = METRIC + EXPECTATION                                            |
|  - Metric:  e.g. abs(COUNT(target) - COUNT(source)), MIN(col), MAX(col)|
|  - Expectation:  e.g. VALUE = 0, VALUE >= 0, VALUE <= 730              |
|  - Schedule:  when the DMF runs (e.g. 8am, 2pm, 8pm UTC daily)         |
+-----------------------------------------------------------------------+
```

**What you get with DMFs:**

- **Automated DQ checks** on your report output (tables/views)
- **Scheduled runs** with results stored in Snowflake event tables
- **Expectations** for pass/fail (count match, bounds checks, null checks)
- **Reusable pattern**: config + source view + custom DMF + system DMFs

---

## 2. High-Level Architecture

```text
+-------------------------------------------------------------------------------------------+
|                        REPORT PERIOD CONFIGURATION (optional)                             |
|  report_period_config (table) -> report_period_all_frequencies (view)                     |
|  - Stores periods (MONTHLY, WEEKLY, DAILY, etc.)                                          |
|  - report_start_date = 00:00:00.000, report_end_date = 23:59:59.997                       |
+-------------------------------------------------------------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------------------+
|  SOURCE VIEW (for custom DMFs)                                                            |
|  - Encapsulates source logic (joins, filters, dedup)                                      |
|  - Reads from config view for report period                                               |
|  - Single output column for counting (e.g. id)                                            |
+-------------------------------------------------------------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------------------+
|  YOUR REPORT OUTPUT (table/view)                                                          |
|  - Target for DMF attachment                                                              |
|  - DMFs attached:                                                                         |
|    1. Custom DMF (e.g. source vs target count)                                            |
|    2. System DMFs: MIN, MAX, NULL_COUNT, ROW_COUNT, etc.                                  |
+-------------------------------------------------------------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------------------+
|  EXPECTATIONS (pass/fail rules)                                                           |
|  - e.g. source_target_count_match: VALUE = 0                                              |
|  - e.g. column_min_non_negative: VALUE >= 0                                               |
|  - e.g. column_max_bounded: VALUE <= threshold                                            |
+-------------------------------------------------------------------------------------------+
```

**Data flow (custom DMF example):**

```text
 source_table_1 ------+
                      +--> source_count_view --> COUNT(*) --+
 source_table_2 ------+       (view)                       |
                                                           +--> abs(delta) --> EXPECT = 0
 your_report_output (target) ------------------> COUNT(*) -+
```

---

## 3. DMF Types

### 3.1 System DMFs (Built-in)

Snowflake provides these system DMFs in `SNOWFLAKE.CORE`:

| DMF | Purpose | Example Use |
|-----|---------|-------------|
| `NULL_COUNT` | Count NULLs in a column | Required field validation |
| `ROW_COUNT` | Count rows | Data volume monitoring |
| `UNIQUE_COUNT` | Count distinct values | Uniqueness checks |
| `DUPLICATE_COUNT` | Count duplicates (single column) | Key validation |
| `FRESHNESS` | Time since last update | Staleness detection |
| `MIN` | Minimum value | Lower bound validation |
| `MAX` | Maximum value | Upper bound validation |
| `AVG` | Average value | Statistical monitoring |
| `STDDEV` | Standard deviation | Anomaly detection |

### 3.2 Custom DMFs

Create your own DMF when system DMFs don't cover your use case:

- **Source vs target count comparison**
- **Cross-table validation**
- **Complex business rules**

**Constraint:** Custom DMFs accept **at most 2 TABLE arguments**.

---

## 4. Implementation Steps

### Step 1: Prerequisites

Before implementing DMFs:

- [ ] Snowflake **Enterprise Edition** (required for DMFs)
- [ ] Role has **EXECUTE DATA METRIC FUNCTION ON ACCOUNT** privilege
- [ ] Target **table or view** exists (your report creates it)
- [ ] Identify **DQ checks** to implement (from existing test SQL or business rules)

### Step 2: Set Schedule on Target

DMFs require a schedule **before** you can attach them:

```sql
-- Set schedule on target table/view
ALTER TABLE your_schema.your_report_output
SET DATA_METRIC_SCHEDULE = 'USING CRON 0 8,14,20 * * * UTC';
-- Runs at 8am, 2pm, 8pm UTC daily
```

**Schedule options:**
- Interval: `'5 MINUTE'`, `'1 HOUR'`
- Cron: `'USING CRON 0 8 * * * UTC'` (8am daily)
- On changes: `'TRIGGER_ON_CHANGES'`

### Step 3: Add System DMFs

Add built-in DMFs for common checks:

```sql
-- Add system DMFs to target
ALTER TABLE your_schema.your_report_output
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (required_column),
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (numeric_column),
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX ON (numeric_column);
```

### Step 4: Create Source View (for custom DMFs)

If you need source vs target comparison, create a view that encapsulates source logic:

```sql
CREATE OR REPLACE VIEW your_schema.source_count_view AS
WITH
config AS (
    SELECT report_start_date, report_end_date
    FROM your_schema.report_period_config
    WHERE frequency = 'MONTHLY'
    LIMIT 1
),
source_data AS (
    SELECT id
    FROM your_schema.source_table s, config c
    WHERE s.created_date BETWEEN c.report_start_date AND c.report_end_date
    -- Add your source logic here (joins, filters, dedup)
)
SELECT id FROM source_data;
```

### Step 5: Create Custom DMF

Create DMF with **at most 2 TABLE arguments**:

```sql
CREATE OR REPLACE DATA METRIC FUNCTION your_schema.source_target_count_diff(
    arg_target TABLE(arg_id VARCHAR),
    arg_source TABLE(arg_id VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT ABS(
        (SELECT COUNT(*) FROM arg_target) -
        (SELECT COUNT(*) FROM arg_source)
    )
$$;
```

### Step 6: Attach Custom DMF to Target

```sql
-- Important: Use two-part names (schema.object) after USE DATABASE
USE DATABASE your_database;

ALTER TABLE your_schema.your_report_output
ADD DATA METRIC FUNCTION your_schema.source_target_count_diff ON (
    id,
    TABLE your_schema.source_count_view(id)
);
```

### Step 7: Create Expectations

Define pass/fail rules for each DMF:

```sql
-- Custom DMF expectation
CREATE OR REPLACE EXPECTATION source_target_count_match
ON TABLE your_schema.your_report_output
FOR DATA METRIC FUNCTION your_schema.source_target_count_diff(
    id,
    TABLE your_schema.source_count_view(id)
)
EXPECT VALUE = 0
WITH COMMENT 'Source count must match target count.';

-- System DMF expectations
CREATE OR REPLACE EXPECTATION column_min_non_negative
ON TABLE your_schema.your_report_output
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN(numeric_column)
EXPECT VALUE >= 0
WITH COMMENT 'No negative values allowed.';

CREATE OR REPLACE EXPECTATION column_max_bounded
ON TABLE your_schema.your_report_output
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX(numeric_column)
EXPECT VALUE <= 1000
WITH COMMENT 'Values must not exceed 1000.';

CREATE OR REPLACE EXPECTATION required_field_not_null
ON TABLE your_schema.your_report_output
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT(required_column)
EXPECT VALUE = 0
WITH COMMENT 'Required field must not be NULL.';
```

---

## 5. Report Period Configuration (Optional)

If your DMFs need report period dates, create a configuration view:

### 5.1 Static Config Table

```sql
CREATE TABLE IF NOT EXISTS your_schema.report_period_config (
    frequency VARCHAR(50) NOT NULL,
    report_start_date TIMESTAMP_NTZ NOT NULL,
    report_end_date TIMESTAMP_NTZ NOT NULL,
    as_of_run_dt DATE NOT NULL,
    carrier_name VARCHAR(255),
    PRIMARY KEY (frequency, report_start_date, report_end_date, carrier_name)
);
```

### 5.2 Dynamic Config View (All Frequencies)

Creates 6 rows with dynamic dates based on `CURRENT_DATE()`:

```sql
CREATE OR REPLACE VIEW your_schema.report_period_all_frequencies AS
WITH cd AS (SELECT CURRENT_DATE() AS d)
SELECT 'DAILY' AS frequency,
       (DATEADD('day', -1, d))::TIMESTAMP_NTZ AS report_start_date,
       DATEADD('millisecond', -3, (DATEADD('day', 1, DATEADD('day', -1, d)))::TIMESTAMP_NTZ) AS report_end_date,
       d AS as_of_run_dt
FROM cd
UNION ALL
SELECT 'WEEKLY',
       (DATEADD('day', -7, DATE_TRUNC('week', d)))::TIMESTAMP_NTZ,
       DATEADD('millisecond', -3, (DATEADD('day', 1, DATEADD('day', -1, DATE_TRUNC('week', d))))::TIMESTAMP_NTZ),
       d
FROM cd
UNION ALL
SELECT 'MONTHLY',
       (ADD_MONTHS(DATE_TRUNC('month', d), -1))::TIMESTAMP_NTZ,
       DATEADD('millisecond', -3, (DATEADD('day', 1, LAST_DAY(ADD_MONTHS(DATE_TRUNC('month', d), -1))))::TIMESTAMP_NTZ),
       d
FROM cd
UNION ALL
SELECT 'QUARTERLY',
       (ADD_MONTHS(DATE_TRUNC('quarter', d), -3))::TIMESTAMP_NTZ,
       DATEADD('millisecond', -3, (DATEADD('day', 1, LAST_DAY(ADD_MONTHS(DATE_TRUNC('quarter', d), -1))))::TIMESTAMP_NTZ),
       d
FROM cd
UNION ALL
SELECT 'YEARLY',
       (DATE_FROM_PARTS(YEAR(d) - 1, 1, 1))::TIMESTAMP_NTZ,
       DATEADD('millisecond', -3, (DATEADD('day', 1, DATE_FROM_PARTS(YEAR(d) - 1, 12, 31)))::TIMESTAMP_NTZ),
       d
FROM cd;
```

**Timestamp precision:**
- `report_start_date`: 00:00:00.000 (start of day)
- `report_end_date`: 23:59:59.997 (end of day)

---

## 6. Common DQ Check Patterns

### 6.1 Source vs Target Count Match

**Use case:** Ensure all source records made it to the target.

```sql
-- Source view encapsulates source logic
CREATE OR REPLACE VIEW schema.source_count_view AS
SELECT id FROM source_table WHERE ...;

-- Custom DMF
CREATE OR REPLACE DATA METRIC FUNCTION schema.count_diff(
    arg_target TABLE(arg_id VARCHAR),
    arg_source TABLE(arg_id VARCHAR)
) RETURNS NUMBER AS $$
    SELECT ABS((SELECT COUNT(*) FROM arg_target) - (SELECT COUNT(*) FROM arg_source))
$$;

-- Expectation: difference = 0
CREATE OR REPLACE EXPECTATION count_match
ON TABLE schema.target
FOR DATA METRIC FUNCTION schema.count_diff(id, TABLE schema.source_count_view(id))
EXPECT VALUE = 0;
```

### 6.2 No Negative Values

**Use case:** Numeric column should never be negative.

```sql
ALTER TABLE schema.target
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (amount);

CREATE OR REPLACE EXPECTATION amount_non_negative
ON TABLE schema.target
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN(amount)
EXPECT VALUE >= 0;
```

### 6.3 Value Within Range

**Use case:** Column must be within expected bounds.

```sql
ALTER TABLE schema.target
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (score),
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX ON (score);

CREATE OR REPLACE EXPECTATION score_min
ON TABLE schema.target
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN(score)
EXPECT VALUE >= 0;

CREATE OR REPLACE EXPECTATION score_max
ON TABLE schema.target
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX(score)
EXPECT VALUE <= 100;
```

### 6.4 Required Field Not Null

**Use case:** Required column must not have NULLs.

```sql
ALTER TABLE schema.target
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (customer_id);

CREATE OR REPLACE EXPECTATION customer_id_required
ON TABLE schema.target
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT(customer_id)
EXPECT VALUE = 0;
```

### 6.5 Flag Column (0 or 1 only)

**Use case:** Boolean flag stored as integer must be 0 or 1.

```sql
ALTER TABLE schema.target
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (is_active),
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX ON (is_active);

CREATE OR REPLACE EXPECTATION flag_min
ON TABLE schema.target
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN(is_active)
EXPECT VALUE >= 0;

CREATE OR REPLACE EXPECTATION flag_max
ON TABLE schema.target
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX(is_active)
EXPECT VALUE <= 1;
```

---

## 7. Monitoring and Results

### 7.1 List DMFs on Target

```sql
SELECT 
    ref_entity_name,
    metric_name,
    metric_schema,
    schedule_status,
    last_execution_time,
    next_execution_time
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'your_schema.your_report_output',
    REF_ENTITY_DOMAIN => 'TABLE'  -- or 'VIEW'
))
ORDER BY metric_name;
```

### 7.2 View Results and Expectation Status

```sql
SELECT 
    metric_name,
    metric_value,
    expectation_name,
    expectation_status,
    timestamp
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_RESULTS(
    'your_schema.your_report_output'
))
ORDER BY timestamp DESC
LIMIT 20;
```

### 7.3 Check Schedule

```sql
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE your_db.your_schema.your_report_output;
```

### 7.4 Manual Execution (Testing)

```sql
-- Test custom DMF manually
SELECT your_schema.source_target_count_diff(
    TABLE(your_schema.your_report_output(id)),
    TABLE(your_schema.source_count_view(id))
) AS count_difference;

-- Test system DMF manually
SELECT SNOWFLAKE.CORE.MIN(SELECT amount FROM your_schema.your_report_output) AS min_amount;
```

---

## 8. Troubleshooting

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| "Unexpected database name" | Three-part name in ADD DMF TABLE clause | Use `USE DATABASE` + two-part names (schema.object) |
| "Invalid number of arguments" | More than 2 TABLE args in DMF | Use at most 2; fold extra inputs into a view as second arg |
| "Is not a view" or wrong object type | Target is table but using ALTER VIEW | Use `ALTER TABLE` (and vice versa) |
| "Does not exist or not authorized" | Missing privs or wrong object name | Grant `EXECUTE DATA METRIC FUNCTION ON ACCOUNT`; check names |
| "Invalid schedule" | Bad cron or interval format | Use `'5 MINUTE'` or `'USING CRON 0 8 * * * UTC'` |
| DMF not running | Schedule not set before ADD | Set `DATA_METRIC_SCHEDULE` before adding DMFs |

---

## 9. Maintenance

### Drop Expectation

```sql
DROP EXPECTATION expectation_name ON TABLE your_schema.your_target;
```

### Drop DMF

The `ON` clause must match the `ADD` exactly:

```sql
-- Drop system DMF
ALTER TABLE your_schema.your_target
DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (amount);

-- Drop custom DMF
ALTER TABLE your_schema.your_target
DROP DATA METRIC FUNCTION your_schema.source_target_count_diff ON (
    id,
    TABLE your_schema.source_count_view(id)
);
```

### Update Schedule

```sql
ALTER TABLE your_schema.your_target
SET DATA_METRIC_SCHEDULE = 'USING CRON 0 6 * * * UTC';  -- Change to 6am daily
```

---

## 10. File Structure Template

For each report that uses DMFs:

```text
your_report/
  report_period_config.sql       # (optional) report period table/view
  setup_dmf_data_quality.sql     # schedule, source view, DMFs, expectations
  run_dmf_setup.py               # (optional) Python runner for deployment
  test_data_quality.sql          # (optional) legacy SQL DQ checks
```

---

## 11. Key Design Decisions

| Decision | Reason |
|----------|--------|
| **Source view for multi-table logic** | DMFs accept at most 2 TABLE arguments. Use a view to encapsulate joins/filters. |
| **Config view for report periods** | DMF bodies must be deterministic (no session variables). A view is fixed at query time. |
| **Timestamps 00:00:00.000 / 23:59:59.997** | Exact start/end of day for range checks. Avoids off-by-day or timezone issues. |
| **Two-part names + USE DATABASE** | Three-part names in ADD DMF can cause "unexpected database name" errors. |
| **System MIN/MAX for bounds** | Simple rules map directly to system DMFs. No custom DMF needed. |
| **One schedule per target** | Snowflake uses one DATA_METRIC_SCHEDULE per table/view. All DMFs share it. |

---

## 12. Reference Links

- [Snowflake Data Quality Introduction](https://docs.snowflake.com/en/user-guide/data-quality-intro)
- [Custom DMFs](https://docs.snowflake.com/en/user-guide/data-quality-custom-dmfs)
- [Expectations](https://docs.snowflake.com/en/user-guide/data-quality-expectations)
- [CREATE DATA METRIC FUNCTION](https://docs.snowflake.com/en/sql-reference/sql/create-data-metric-function)
- [System DMFs](https://docs.snowflake.com/en/user-guide/data-quality-system-dmfs)

---

## 13. Quick Start Checklist

- [ ] Verify Snowflake Enterprise Edition
- [ ] Grant `EXECUTE DATA METRIC FUNCTION ON ACCOUNT` to role
- [ ] Target table/view exists
- [ ] Set `DATA_METRIC_SCHEDULE` on target
- [ ] (If needed) Create source view for custom DMF
- [ ] (If needed) Create custom DMF (max 2 TABLE args)
- [ ] Add DMFs to target (`ALTER TABLE/VIEW ... ADD DATA METRIC FUNCTION`)
- [ ] Create expectations (`CREATE EXPECTATION ... EXPECT VALUE ...`)
- [ ] Verify with `DATA_METRIC_FUNCTION_REFERENCES`
- [ ] Check results with `DATA_METRIC_FUNCTION_RESULTS`

---

*Use this guide to implement DMF-based data quality monitoring on any Snowflake report.*
