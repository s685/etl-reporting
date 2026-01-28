-- =====================================================================
-- SNOWFLAKE DMF SETUP SCRIPT
-- new_rfb_and_total_claimants_active_detail Report
-- =====================================================================
-- Purpose: Set up Data Metric Functions (DMFs) for automated data quality monitoring
--
-- Logic aligned with Snowflake documentation. See references at end of file.
--
-- SIMPLIFIED APPROACH:
-- - NULL checks: system SNOWFLAKE.CORE.NULL_COUNT (one per critical column).
-- - ROW_COUNT, UNIQUE_COUNT: system (single-column only per docs).
-- - DUPLICATE_COUNT: system single-column only; composite-key uses custom
--   duplicate_composite_key_count. Other logic via custom DMFs.
--
-- Prerequisites:
--   1. Enterprise Edition (docs: data-quality-intro)
--   2. EXECUTE DATA METRIC FUNCTION on account (data-quality-access-control)
--   3. Owner is custom or system role, not database role
-- =====================================================================

-- Replace with your actual database and schema.
SET target_table = '{{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail';

-- Use ALTER VIEW for views, ALTER TABLE for tables. Update the keyword below.
-- Target new_rfb_and_total_claimants_active_detail is a VIEW.

-- =====================================================================
-- STEP 0: SET SCHEDULE (REQUIRED BEFORE ADDING DMFs)
-- =====================================================================
-- Per docs (data-quality-working): Set DATA_METRIC_SCHEDULE before associating
-- any DMF. All DMFs on this object share the same schedule.

ALTER VIEW IDENTIFIER($target_table)
SET DATA_METRIC_SCHEDULE = 'USING CRON 0 8,14,20 * * * UTC';

-- =====================================================================
-- STEP 1: ADD SYSTEM DMFs
-- =====================================================================
-- System DMFs: docs.snowflake.com sql-reference functions-data-metric
-- NULL_COUNT, UNIQUE_COUNT, DUPLICATE_COUNT: single column only (dmf_null_count, etc.).
-- ROW_COUNT: ON() per dmf_row_count.

-- ROW_COUNT (dmf_row_count): empty ON()
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- NULL_COUNT (dmf_null_count): one per critical column, single column only
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ("Policy Number");
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ("Status");
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ("EDB Decision Date");
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ("Statistical Start Date");
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (carrier_name);
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (DAYS);
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (NumDaysResolvedWithinTwoWeeks);

-- UNIQUE_COUNT (dmf_unique_count): single column only
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON ("Policy Number");

-- DUPLICATE_COUNT (dmf_duplicate_count): single column only. Composite-key
-- duplicate check uses custom DMF duplicate_composite_key_count (STEP 2 + 3).

-- =====================================================================
-- STEP 2: CREATE CUSTOM DMFs FOR BUSINESS LOGIC
-- =====================================================================
-- Custom DMFs: CREATE DATA METRIC FUNCTION with TABLE args (docs: create-data-metric-function,
-- user-guide data-quality-custom-dmfs). Same-table columns only; expression deterministic.

-- Custom DMF: Count negative DAYS values
CREATE OR REPLACE DATA METRIC FUNCTION negative_days_count(
    arg_t TABLE(arg_days NUMBER)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE arg_days < 0
$$;

-- Custom DMF: Count future dates (unreasonably far in future or invalid dates)
-- Note: DMF expressions must be deterministic (docs: create-data-metric-function).
-- CURRENT_DATE() is non-deterministic, so we check for dates beyond a fixed threshold
-- (year 2100) or invalid/unparseable dates instead.
CREATE OR REPLACE DATA METRIC FUNCTION future_dates_count(
    arg_t TABLE(arg_edb_decision_date VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE TRY_TO_DATE(arg_edb_decision_date, 'MM/DD/YYYY') > DATE '2100-01-01'
       OR (TRY_TO_DATE(arg_edb_decision_date, 'MM/DD/YYYY') IS NULL 
           AND arg_edb_decision_date IS NOT NULL)
$$;

-- Custom DMF: Composite-key duplicate count (DQ-004). System DUPLICATE_COUNT
-- supports single column only (dmf_duplicate_count); use custom for composite key.
CREATE OR REPLACE DATA METRIC FUNCTION duplicate_composite_key_count(
    arg_t TABLE(
        arg_policy_number VARCHAR,
        arg_status VARCHAR,
        arg_edb_date VARCHAR,
        arg_stat_date VARCHAR
    )
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM (
        SELECT arg_policy_number, arg_status, arg_edb_date, arg_stat_date, COUNT(*) AS cnt
        FROM arg_t
        GROUP BY arg_policy_number, arg_status, arg_edb_date, arg_stat_date
        HAVING COUNT(*) > 1
    )
$$;

-- Custom DMF: Count two-week flag inconsistencies
CREATE OR REPLACE DATA METRIC FUNCTION two_week_flag_inconsistency_count(
    arg_t TABLE(arg_days NUMBER, arg_flag NUMBER)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE (arg_flag = 1 AND arg_days >= 14)
       OR (arg_flag = 0 AND arg_days < 14)
$$;

-- Custom DMF: Count invalid date formats
CREATE OR REPLACE DATA METRIC FUNCTION invalid_date_format_count(
    arg_t TABLE(arg_date VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE TRY_TO_DATE(arg_date, 'MM/DD/YYYY') IS NULL
      AND arg_date IS NOT NULL
$$;

-- Custom DMF: Count unrealistic turnaround times (>730 days)
CREATE OR REPLACE DATA METRIC FUNCTION unrealistic_turnaround_count(
    arg_t TABLE(arg_days NUMBER)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE arg_days > 730
$$;

-- Custom DMF: Count decision dates before start dates
CREATE OR REPLACE DATA METRIC FUNCTION decision_before_start_count(
    arg_t TABLE(arg_start_date VARCHAR, arg_decision_date VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE TRY_TO_DATE(arg_start_date, 'MM/DD/YYYY') > 
          TRY_TO_DATE(arg_decision_date, 'MM/DD/YYYY')
$$;

-- Custom DMF: Calculate SLA compliance rate
CREATE OR REPLACE DATA METRIC FUNCTION sla_compliance_rate(
    arg_t TABLE(arg_flag NUMBER)
)
RETURNS NUMBER
AS $$
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 0
            ELSE (SUM(arg_flag) * 100.0 / COUNT(*))
        END
    FROM arg_t
$$;

-- Custom DMF: Count empty strings in critical fields (DQ-003)
CREATE OR REPLACE DATA METRIC FUNCTION empty_strings_count(
    arg_t TABLE(
        arg_policy_number VARCHAR,
        arg_status VARCHAR,
        arg_insurance_group VARCHAR,
        arg_carrier_name VARCHAR
    )
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE arg_policy_number = ''
       OR arg_status = ''
       OR arg_insurance_group = ''
       OR arg_carrier_name = ''
$$;

-- Custom DMF: Count excessive policy duplicates (>10 times) (DQ-005)
CREATE OR REPLACE DATA METRIC FUNCTION excessive_policy_duplicates_count(
    arg_t TABLE(arg_policy_number VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(DISTINCT arg_policy_number)
    FROM (
        SELECT arg_policy_number, COUNT(*) AS cnt
        FROM arg_t
        GROUP BY arg_policy_number
        HAVING COUNT(*) > 10
    )
$$;

-- Custom DMF: Count invalid numeric values (DQ-007)
CREATE OR REPLACE DATA METRIC FUNCTION invalid_numeric_values_count(
    arg_t TABLE(arg_days NUMBER, arg_flag NUMBER)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE TRY_CAST(arg_days AS INTEGER) IS NULL
       OR TRY_CAST(arg_flag AS INTEGER) IS NULL
$$;

-- Custom DMF: Count multiple carrier names (should be 1) (DQ-016)
CREATE OR REPLACE DATA METRIC FUNCTION multiple_carrier_names_count(
    arg_t TABLE(arg_carrier_name VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(DISTINCT arg_carrier_name) - 1
    FROM arg_t
$$;

-- Custom DMF: Count missing state data (DQ-017)
CREATE OR REPLACE DATA METRIC FUNCTION missing_state_data_count(
    arg_t TABLE(arg_residence_state VARCHAR, arg_issue_state VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE arg_residence_state IS NULL
       OR arg_issue_state IS NULL
       OR arg_residence_state = ''
       OR arg_issue_state = ''
$$;

-- Custom DMF: Count Modified By format issues (DQ-018)
CREATE OR REPLACE DATA METRIC FUNCTION modified_by_format_issues_count(
    arg_t TABLE(arg_modified_by VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE arg_modified_by LIKE '%\\%'
$$;

-- Custom DMF: Count missing Insurance Group (DQ-019)
CREATE OR REPLACE DATA METRIC FUNCTION missing_insurance_group_count(
    arg_t TABLE(arg_insurance_group VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT COUNT(*)
    FROM arg_t
    WHERE arg_insurance_group IS NULL
       OR arg_insurance_group = ''
$$;

-- =====================================================================
-- STEP 3: ADD CUSTOM DMFs
-- =====================================================================
-- Use ALTER VIEW for views; use ALTER TABLE for tables.
-- NULL checks use system NULL_COUNT in STEP 1 (one per critical column).

-- DQ-004: Composite-key duplicate check (system DUPLICATE_COUNT is single-column only)
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION duplicate_composite_key_count ON (
    "Policy Number",
    "Status",
    "EDB Decision Date",
    "Statistical Start Date"
);

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION negative_days_count ON (DAYS);

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION future_dates_count ON ("EDB Decision Date");

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION two_week_flag_inconsistency_count ON (DAYS, NumDaysResolvedWithinTwoWeeks);

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION invalid_date_format_count ON ("Statistical Start Date");

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION invalid_date_format_count ON ("EDB Decision Date");

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION unrealistic_turnaround_count ON (DAYS);

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION decision_before_start_count ON ("Statistical Start Date", "EDB Decision Date");

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION sla_compliance_rate ON (NumDaysResolvedWithinTwoWeeks);

-- Additional DMFs from test_data_quality.sql
ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION empty_strings_count ON ("Policy Number", "Status", "Insurance Group", carrier_name);

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION excessive_policy_duplicates_count ON ("Policy Number");

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION invalid_numeric_values_count ON (DAYS, NumDaysResolvedWithinTwoWeeks);

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION multiple_carrier_names_count ON (carrier_name);

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION missing_state_data_count ON ("Residence State", "Issue State");

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION modified_by_format_issues_count ON ("Modified By");

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION missing_insurance_group_count ON ("Insurance Group");

-- =====================================================================
-- STEP 4: CREATE EXPECTATIONS (PASS/FAIL CRITERIA)
-- =====================================================================
-- NULL checks: system NULL_COUNT, one expectation per critical column (DQ-001/002).

CREATE OR REPLACE EXPECTATION no_null_policy_number
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT("Policy Number")
EXPECT VALUE = 0 WITH COMMENT 'DQ-001: Policy Number must not be NULL';

CREATE OR REPLACE EXPECTATION no_null_status
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT("Status")
EXPECT VALUE = 0 WITH COMMENT 'DQ-001: Status must not be NULL';

CREATE OR REPLACE EXPECTATION no_null_edb_decision_date
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT("EDB Decision Date")
EXPECT VALUE = 0 WITH COMMENT 'DQ-001: EDB Decision Date must not be NULL';

CREATE OR REPLACE EXPECTATION no_null_statistical_start_date
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT("Statistical Start Date")
EXPECT VALUE = 0 WITH COMMENT 'DQ-001: Statistical Start Date must not be NULL';

CREATE OR REPLACE EXPECTATION no_null_carrier_name
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT(carrier_name)
EXPECT VALUE = 0 WITH COMMENT 'DQ-001: carrier_name must not be NULL';

CREATE OR REPLACE EXPECTATION no_null_days
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT(DAYS)
EXPECT VALUE = 0 WITH COMMENT 'DQ-002: DAYS must not be NULL';

CREATE OR REPLACE EXPECTATION no_null_two_week_flag
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT(NumDaysResolvedWithinTwoWeeks)
EXPECT VALUE = 0 WITH COMMENT 'DQ-002: NumDaysResolvedWithinTwoWeeks must not be NULL';

-- DQ-004: No duplicate policy records (composite key); uses custom DMF (system DUPLICATE_COUNT is single-column only)
CREATE OR REPLACE EXPECTATION no_duplicate_policies
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION duplicate_composite_key_count(
    "Policy Number",
    "Status",
    "EDB Decision Date",
    "Statistical Start Date"
)
EXPECT VALUE = 0
WITH COMMENT 'DQ-004: No duplicate policy records allowed';

-- Row count expectation (between 10 and 1,000,000)
CREATE OR REPLACE EXPECTATION row_count_reasonable
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT()
EXPECT VALUE BETWEEN 10 AND 1000000
WITH COMMENT 'DQ-015: Row count should be between 10 and 1,000,000';

-- Business logic expectations
CREATE OR REPLACE EXPECTATION no_negative_days
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION negative_days_count(DAYS)
EXPECT VALUE = 0
WITH COMMENT 'DQ-008: Turnaround time cannot be negative';

CREATE OR REPLACE EXPECTATION no_future_dates
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION future_dates_count("EDB Decision Date")
EXPECT VALUE = 0
WITH COMMENT 'DQ-010: Decision dates must be valid and not beyond year 2100 (DMF expressions must be deterministic; cannot use CURRENT_DATE())';

CREATE OR REPLACE EXPECTATION two_week_flag_consistent
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION two_week_flag_inconsistency_count(DAYS, NumDaysResolvedWithinTwoWeeks)
EXPECT VALUE = 0
WITH COMMENT 'DQ-012: TwoWeek flag must match DAYS value';

CREATE OR REPLACE EXPECTATION valid_date_formats_start
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION invalid_date_format_count("Statistical Start Date")
EXPECT VALUE = 0
WITH COMMENT 'DQ-006: Statistical Start Date must be in MM/DD/YYYY format';

CREATE OR REPLACE EXPECTATION valid_date_formats_decision
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION invalid_date_format_count("EDB Decision Date")
EXPECT VALUE = 0
WITH COMMENT 'DQ-006: EDB Decision Date must be in MM/DD/YYYY format';

CREATE OR REPLACE EXPECTATION no_decision_before_start
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION decision_before_start_count("Statistical Start Date", "EDB Decision Date")
EXPECT VALUE = 0
WITH COMMENT 'DQ-011: Decision cannot happen before RFB start';

-- Warning-level expectations (informational, not blocking)
CREATE OR REPLACE EXPECTATION realistic_turnaround_times
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION unrealistic_turnaround_count(DAYS)
EXPECT VALUE < 100
WITH COMMENT 'DQ-009: WARNING - Unrealistic turnaround times (>730 days) should be < 100';

-- SLA compliance expectation (should be >= 50%)
CREATE OR REPLACE EXPECTATION sla_compliance_acceptable
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION sla_compliance_rate(NumDaysResolvedWithinTwoWeeks)
EXPECT VALUE >= 50
WITH COMMENT 'DQ-022: SLA compliance rate should be at least 50%';

-- Additional expectations from test_data_quality.sql
CREATE OR REPLACE EXPECTATION no_empty_strings
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION empty_strings_count("Policy Number", "Status", "Insurance Group", carrier_name)
EXPECT VALUE = 0
WITH COMMENT 'DQ-003: No empty strings in critical fields';

CREATE OR REPLACE EXPECTATION no_excessive_duplicates
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION excessive_policy_duplicates_count("Policy Number")
EXPECT VALUE = 0
WITH COMMENT 'DQ-005: No policy should appear more than 10 times';

CREATE OR REPLACE EXPECTATION valid_numeric_values
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION invalid_numeric_values_count(DAYS, NumDaysResolvedWithinTwoWeeks)
EXPECT VALUE = 0
WITH COMMENT 'DQ-007: DAYS and NumDaysResolvedWithinTwoWeeks must be valid integers';

CREATE OR REPLACE EXPECTATION single_carrier_name
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION multiple_carrier_names_count(carrier_name)
EXPECT VALUE = 0
WITH COMMENT 'DQ-016: Report should have single carrier_name';

CREATE OR REPLACE EXPECTATION no_missing_state_data
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION missing_state_data_count("Residence State", "Issue State")
EXPECT VALUE = 0
WITH COMMENT 'DQ-017: Residence State and Issue State must be populated';

CREATE OR REPLACE EXPECTATION no_modified_by_format_issues
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION modified_by_format_issues_count("Modified By")
EXPECT VALUE = 0
WITH COMMENT 'DQ-018: Modified By should not contain domain prefix (DOMAIN\user)';

CREATE OR REPLACE EXPECTATION no_missing_insurance_group
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION missing_insurance_group_count("Insurance Group")
EXPECT VALUE = 0
WITH COMMENT 'DQ-019: Insurance Group is required for grouping and analysis';

-- =====================================================================
-- STEP 5: SCHEDULE (Already set in STEP 0; modify here if needed)
-- =====================================================================
-- Schedule is required before adding DMFs (docs: data-quality-working).
-- To change: ALTER VIEW IDENTIFIER($target_table) SET DATA_METRIC_SCHEDULE = '<schedule>';

-- Formats: '5 MINUTE' | '1 HOUR' | '1 DAY' | 'USING CRON 0 8 * * * UTC' | 'TRIGGER_ON_CHANGES'
-- To disable: SET DATA_METRIC_SCHEDULE = NULL

-- =====================================================================
-- STEP 6: VIEW DMF RESULTS
-- =====================================================================

-- Query to view all DMF results for this table
SELECT 
    metric_name,
    metric_value,
    expectation_name,
    expectation_status,
    timestamp,
    table_name
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_RESULTS(
    $target_table
))
ORDER BY timestamp DESC, expectation_name;

-- Query to view only failed expectations
SELECT 
    metric_name,
    metric_value,
    expectation_name,
    expectation_status,
    timestamp
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_RESULTS(
    $target_table
))
WHERE expectation_status = 'FAILED'
  AND timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY timestamp DESC;

-- =====================================================================
-- STEP 7: SET UP ALERTS (OPTIONAL)
-- =====================================================================

-- Example: Alert when any critical NULL expectation fails (system NULL_COUNT)
CREATE OR REPLACE ALERT dq_alert_critical_nulls
WAREHOUSE = 'YOUR_WAREHOUSE'
IF EXISTS (
    SELECT 1
    FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_RESULTS(
        $target_table
    ))
    WHERE expectation_name IN (
        'no_null_policy_number', 'no_null_status', 'no_null_edb_decision_date',
        'no_null_statistical_start_date', 'no_null_carrier_name', 'no_null_days',
        'no_null_two_week_flag'
    )
      AND expectation_status = 'FAILED'
      AND timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour'
)
THEN CALL SYSTEM$SEND_EMAIL(
    'your-email@example.com',
    'Data Quality Alert: Critical NULLs Detected',
    'One or more critical fields have NULL values in ' || $target_table || '. Please investigate immediately.'
);

-- =====================================================================
-- MAINTENANCE QUERIES
-- =====================================================================
-- Detach DMF: use DROP DATA METRIC FUNCTION (docs: alter-table, alter-view).
-- ON (...) must match the columns used when adding. For views use ALTER VIEW.

-- View all DMFs on the object
SHOW DATA METRIC FUNCTIONS ON TABLE IDENTIFIER($target_table);

-- View all expectations
SHOW EXPECTATIONS ON TABLE IDENTIFIER($target_table);

-- Detach a DMF (use DROP, not REMOVE; for views use ALTER VIEW)
-- ALTER VIEW IDENTIFIER($target_table) DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ("Policy Number");
-- ALTER VIEW IDENTIFIER($target_table) DROP DATA METRIC FUNCTION duplicate_composite_key_count ON ("Policy Number", "Status", "EDB Decision Date", "Statistical Start Date");

-- Drop an expectation (if needed)
-- DROP EXPECTATION no_null_policy_number ON TABLE IDENTIFIER($target_table);

-- View DMF usage and costs
SELECT 
    start_time,
    end_time,
    credits_used,
    metric_name,
    table_name
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_USAGE_HISTORY
WHERE table_name = $target_table
ORDER BY start_time DESC
LIMIT 100;

-- =====================================================================
-- NOTES
-- =====================================================================
-- 1. All DMFs on the object share the same schedule (data-quality-working).
-- 2. DMFs use serverless compute; results in event tables.
-- 3. Billing only for scheduled runs, not ad-hoc SELECT of DMFs.
-- 4. Max 10,000 DMF associations per account (data-quality-intro).
-- 5. For tables, use ALTER TABLE instead of ALTER VIEW throughout.
-- 6. NULL checks use system SNOWFLAKE.CORE.NULL_COUNT (one per column). Reuse on
--    other tables: ADD NULL_COUNT ON (col); CREATE EXPECTATION ... EXPECT VALUE = 0.
-- 7. Custom DMF expressions must be deterministic (docs: create-data-metric-function).
--    Non-deterministic functions like CURRENT_DATE(), CURRENT_TIMESTAMP() are NOT allowed.
--    For "future date" checks, use a fixed threshold (e.g., DATE '2100-01-01' or '2100-01-01'::DATE) instead.

-- =====================================================================
-- SNOWFLAKE DMF DOCUMENTATION REFERENCES
-- =====================================================================
-- All logic follows official Snowflake documentation. Verify at docs.snowflake.com.
--
-- Overview & prerequisites
--   https://docs.snowflake.com/en/user-guide/data-quality-intro
--
-- Use DMFs, schedule, add/drop
--   https://docs.snowflake.com/en/user-guide/data-quality-working
--
-- Custom DMFs (TABLE args, same-table columns; overloading by TABLE signature)
--   https://docs.snowflake.com/en/user-guide/data-quality-custom-dmfs
--   https://docs.snowflake.com/en/sql-reference/sql/create-data-metric-function
--
-- System DMFs (single-column unless noted)
--   https://docs.snowflake.com/en/sql-reference/functions-data-metric
--   ROW_COUNT:  https://docs.snowflake.com/en/sql-reference/functions/dmf_row_count  (ON())
--   NULL_COUNT: https://docs.snowflake.com/en/sql-reference/functions/dmf_null_count
--   UNIQUE_COUNT: https://docs.snowflake.com/en/sql-reference/functions/dmf_unique_count
--   DUPLICATE_COUNT: https://docs.snowflake.com/en/sql-reference/functions/dmf_duplicate_count  (single column only)
--
-- Expectations (VALUE = DMF result, REQUIRE DMF)
--   https://docs.snowflake.com/en/user-guide/data-quality-expectations
--
-- ALTER TABLE / ALTER VIEW (ADD / DROP DATA METRIC FUNCTION, SET DATA_METRIC_SCHEDULE)
--   https://docs.snowflake.com/en/sql-reference/sql/alter-table
--   https://docs.snowflake.com/en/sql-reference/sql/alter-view
--
-- Access control (EXECUTE DATA METRIC FUNCTION on account, role type)
--   https://docs.snowflake.com/en/user-guide/data-quality-access-control
--
-- =====================================================================
