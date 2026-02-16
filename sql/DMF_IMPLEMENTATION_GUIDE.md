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

## 14. Snowflake-Native Notifications for Failed DMF Checks

Snowflake can send notifications when DMF expectations **fail** (or when anomaly detection finds an issue). You can use either:

- **Database-level notifications (recommended)** – Enable notifications on the database; Snowflake sends them automatically when an expectation is violated. No Alert or view required.
- **Alert-based notifications** – Use a view of violations plus an Alert that runs on a schedule and calls a notification (see 14.1–14.4).

**Primary doc:** [Sending notifications for data quality issues](https://docs.snowflake.com/en/user-guide/data-quality-notifications).

---

### 14.0 Database-level notifications (automatic, no Alert)

Snowflake can send a notification **whenever** an expectation is violated (or an anomaly is detected) for any DMF on any table/view in a database. You enable this at the **database** level; no Alert or violation view is needed.

**Workflow:**

1. Create a notification integration (email or webhook) if you do not have one.
2. Grant the database owner: `MANAGE DATA QUALITY ON ACCOUNT` and `USAGE ON INTEGRATION <integration_name>`.
3. Run `ALTER DATABASE <db_name> SET DATA_QUALITY_MONITORING_SETTINGS = ...` to turn on notifications and attach the integration(s).

**Example (email integration `my_email_int`, database `dev_snowflake_warehouse`):**

```sql
-- 1. Grant privileges to the role that owns the database (replace your_db_owner with the actual role)
GRANT MANAGE DATA QUALITY ON ACCOUNT TO ROLE your_db_owner;
GRANT USAGE ON INTEGRATION my_email_int TO ROLE your_db_owner;

-- 2. Enable notifications for the database
ALTER DATABASE dev_snowflake_warehouse SET DATA_QUALITY_MONITORING_SETTINGS =
$$
notification:
  enabled: TRUE
  integrations:
    - my_email_int
  metadata_included: TRUE
$$;
```

- **metadata_included: TRUE** – Notifications include the table/view (and DMF) that had the issue.
- You can list multiple integrations (e.g. email + webhook) under `integrations:`.

**Turn off notifications for one DMF association:**

```sql
ALTER VIEW dev_snowflake_warehouse.your_schema.your_view
  MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (your_column)
    SET DATA_QUALITY_NOTIFICATION = FALSE;
```

**Check whether notifications are on:** Query `DATA_METRIC_FUNCTION_REFERENCES`; the column `data_quality_notification_status` indicates if notifications are enabled for each association.

**Doc:** [Sending notifications for data quality issues](https://docs.snowflake.com/en/user-guide/data-quality-notifications).

---

### 14.1 Detecting Failed Expectations (for Alert-based approach)

Snowflake provides two ways to see when an expectation was violated:

**Option A – `DATA_QUALITY_MONITORING_EXPECTATION_STATUS` (recommended for alerts)**

Returns one row per DMF run that had an expectation. Use `expectation_violated = TRUE` for failures. Requires role with `SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER` or `DATA_QUALITY_MONITORING_ADMIN`.

```sql
-- Violations for a single table/view (fully qualified name; use 'TABLE' or 'VIEW')
SELECT 
    ref_entity_name,
    metric_name,
    expectation_name,
    expectation_violated,
    metric_value,
    evaluation_time
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_QUALITY_MONITORING_EXPECTATION_STATUS(
    REF_ENTITY_NAME   => 'your_database.your_schema.your_report_output',
    REF_ENTITY_DOMAIN => 'TABLE'
))
WHERE expectation_violated = TRUE
  AND evaluation_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY evaluation_time DESC;
```

**Option B – `DATA_METRIC_FUNCTION_RESULTS`**

Returns DMF results and expectation status; filter by `expectation_status = 'FAILED'`.

```sql
SELECT ref_entity_name, metric_name, expectation_name, expectation_status, metric_value, timestamp
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_RESULTS(
    REF_ENTITY_NAME => 'your_schema.your_report_output'
))
WHERE expectation_status = 'FAILED'
  AND timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP());
```

Use either in a **view** that returns rows only when there are violations; the Alert condition will be `IF (EXISTS (SELECT 1 FROM that_view))`.

---

### 14.2 Create a View That Returns Rows Only When There Are Violations

The Alert’s condition must be a query that returns rows when you want to notify. Create a view that selects violations (last 24 hours or your chosen window):

```sql
-- Using DATA_QUALITY_MONITORING_EXPECTATION_STATUS (fully qualified ref entity)
CREATE OR REPLACE VIEW your_schema.dmf_violations AS
SELECT ref_entity_name, metric_name, expectation_name, metric_value, evaluation_time
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_QUALITY_MONITORING_EXPECTATION_STATUS(
    REF_ENTITY_NAME   => 'your_database.your_schema.your_report_output',
    REF_ENTITY_DOMAIN => 'TABLE'
))
WHERE expectation_violated = TRUE
  AND evaluation_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP());
```

If you prefer `DATA_METRIC_FUNCTION_RESULTS`, use the same pattern with `expectation_status = 'FAILED'` and `timestamp`.

---

### 14.3 Notification Integration (Webhook for Teams, Slack, PagerDuty)

Create a **notification integration** (one-time, typically by ACCOUNTADMIN) so Alerts can send to a webhook.

**Webhook (Teams, Slack, PagerDuty):**

- **Slack:** `https://hooks.slack.com/services/...`
- **Microsoft Teams:** Incoming Webhook URL from the channel connector.
- **PagerDuty:** `https://events.pagerduty.com/v2/enqueue`

```sql
CREATE OR REPLACE NOTIFICATION INTEGRATION dmf_alert_webhook
  TYPE = WEBHOOK
  ENABLED = TRUE
  WEBHOOK_URL = 'https://your-org.webhook.office.com/webhookb2/...';   -- Teams example; use your URL
```

If the URL contains a secret, use a **secret** and reference it (see [CREATE NOTIFICATION INTEGRATION – webhooks](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-webhooks) and [Sending webhook notifications](https://docs.snowflake.com/en/user-guide/notifications/webhook-notifications)).

**Email:** Use an existing **email integration** (e.g. created in Admin > Notifications). No extra CREATE for the Alert; use `SYSTEM$SEND_EMAIL` in the action.

---

### 14.4 Create an Alert That Sends a Notification When Violations Exist

When the condition query returns at least one row, the Alert runs the action once (e.g. send one email or one webhook call).

**Email action:**

```sql
CREATE OR REPLACE ALERT your_schema.dmf_failure_alert
  WAREHOUSE = your_warehouse
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (SELECT 1 FROM your_schema.dmf_violations))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'your_email_integration',
      'team@example.com',
      'DMF data quality failure',
      'One or more DMF expectations were violated. Query dmf_violations view for details.'
    );
```

**Webhook action (Teams/Slack):**

Use `SYSTEM$SEND_SNOWFLAKE_NOTIFICATION` with the webhook integration and a message helper (`TEXT_PLAIN`, `TEXT_HTML`, or `APPLICATION_JSON`). See [SYSTEM$SEND_SNOWFLAKE_NOTIFICATION](https://docs.snowflake.com/en/sql-reference/stored-procedures/system_send_snowflake_notification) and [Sending webhook notifications](https://docs.snowflake.com/en/user-guide/notifications/webhook-notifications).

```sql
CREATE OR REPLACE ALERT your_schema.dmf_failure_alert
  WAREHOUSE = your_warehouse
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (SELECT 1 FROM your_schema.dmf_violations))
  THEN
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
      INTEGRATION('dmf_alert_webhook'),
      TEXT_PLAIN('DMF data quality failure: one or more expectations violated. Check dmf_violations view.')
    );
```

**Activate the Alert:** New Alerts are created in **SUSPENDED** state. Resume to start checking and notifying:

```sql
ALTER ALERT your_schema.dmf_failure_alert RESUME;
```

---

### 14.5 Reference Links (Snowflake Documentation)

- [Sending notifications for data quality issues](https://docs.snowflake.com/en/user-guide/data-quality-notifications) (database-level notifications; recommended)
- [Introduction to data quality and DMFs](https://docs.snowflake.com/en/user-guide/data-quality-intro)
- [Using expectations to implement data quality checks](https://docs.snowflake.com/en/user-guide/data-quality-expectations)
- [Monitoring data quality checks in Snowsight](https://docs.snowflake.com/en/user-guide/data-quality-ui-monitor)
- [DATA_QUALITY_MONITORING_EXPECTATION_STATUS](https://docs.snowflake.com/en/sql-reference/functions/data_quality_monitoring_expectation_status)
- [Alerts](https://docs.snowflake.com/en/user-guide/alerts)
- [Sending webhook notifications](https://docs.snowflake.com/en/user-guide/notifications/webhook-notifications)
- [CREATE NOTIFICATION INTEGRATION (webhooks)](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-webhooks)
- [SYSTEM$SEND_SNOWFLAKE_NOTIFICATION](https://docs.snowflake.com/en/sql-reference/stored-procedures/system_send_snowflake_notification)

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
