-- =====================================================================
-- SNOWFLAKE DMF SETUP: Source vs Target Record Count Difference (DQ-024)
-- =====================================================================
-- Purpose: Production-grade DMF to compare total record count in target 
--          table vs source data count from episode_of_benefit and care_mgmt_service
-- 
-- Design Principles (Snowflake Best Practices):
--   1. Deterministic expressions only (no session variables in DMF body)
--   2. Early filter pushdown for performance
--   3. QUALIFY for efficient ROW_NUMBER filtering
--   4. Scalar subqueries instead of CROSS JOINs where beneficial
--   5. Minimal data movement and window function overhead
--   6. Proper NULL handling and edge case management
-- 
-- Prerequisites:
--   1. Enterprise Edition (required for DMFs)
--   2. EXECUTE DATA METRIC FUNCTION ON ACCOUNT privilege
--   3. Object owner must be custom/system role (not database role)
--   4. report_period_all_frequencies view exists (report_period_config.sql)
--   5. source_rfb_count_monthly view created in STEP 1b (uses EOB, care_mgmt, config; MONTHLY)
-- 
-- Docs: https://docs.snowflake.com/en/user-guide/data-quality-intro
--       https://docs.snowflake.com/en/sql-reference/sql/create-data-metric-function
--       https://docs.snowflake.com/en/user-guide/data-quality-custom-dmfs
-- =====================================================================

-- =====================================================================
-- CONFIGURATION VARIABLES
-- =====================================================================
SET target_database = '{{TARGET_DATABASE}}';
SET target_schema = '{{TARGET_SCHEMA}}';
SET target_table = $target_schema || '.new_rfb_and_total_claimants_active_detail';
SET source_view = $target_schema || '.source_rfb_count_monthly';
SET source_database = '{{SOURCE_DATABASE}}';
SET config_view = $target_database || '.' || $target_schema || '.report_period_all_frequencies';

-- Use two-part names (schema.object) to avoid "unexpected database name" in ADD DMF TABLE clause.
-- USE DATABASE is required before ALTER VIEW / CREATE VIEW / ADD DMF when using two-part names.
USE DATABASE IDENTIFIER($target_database);

-- Object type: VIEW (change to TABLE if target is a table)
-- Snowflake DMFs accept only 1 or 2 TABLE arguments. We use: (1) target, (2) source view.
-- source_rfb_count_monthly embeds EOB + care_mgmt + report_period_all_frequencies (MONTHLY).

-- =====================================================================
-- STEP 1: SET SCHEDULE (REQUIRED BEFORE ADDING DMFs)
-- =====================================================================
-- Per Snowflake docs: DATA_METRIC_SCHEDULE must be set before associating DMFs
-- All DMFs on the object share the same schedule
-- Schedule format: interval ('5 MINUTE'), cron ('USING CRON ...'), or TRIGGER_ON_CHANGES

ALTER VIEW IDENTIFIER($target_table)
SET DATA_METRIC_SCHEDULE = 'USING CRON 0 8,14,20 * * * UTC';

-- =====================================================================
-- STEP 1b: CREATE SOURCE VIEW FOR DMF (REQUIRED: DMF accepts only 1 or 2 TABLE args)
-- =====================================================================
-- Encapsulates EOB + care_mgmt + report period (MONTHLY from report_period_all_frequencies).
-- Run report_period_config.sql first so report_period_all_frequencies exists.

CREATE OR REPLACE VIEW IDENTIFIER($source_view) AS
WITH
report_period AS (
    SELECT report_start_date AS report_start, report_end_date AS report_end
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.report_period_all_frequencies
    WHERE frequency = 'MONTHLY'
    LIMIT 1
),
eob_latest AS (
    SELECT rfb_id, eb_decision_dt
    FROM {{SOURCE_DATABASE}}.dbo.episode_of_benefit eob, report_period rp
    WHERE eob.last_mod_dt <= rp.report_end
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY episode_of_benefit_id
        ORDER BY last_mod_dt DESC, sequence_no DESC
    ) = 1
),
eob_ranking AS (
    SELECT
        rfb_id,
        MIN(eb_decision_dt) OVER (PARTITION BY rfb_id) AS firstebdecisiondt,
        ROW_NUMBER() OVER (PARTITION BY rfb_id ORDER BY eb_decision_dt) AS rank_val
    FROM eob_latest
),
eob_source AS (
    SELECT rfb_id
    FROM eob_ranking, report_period rp
    WHERE firstebdecisiondt BETWEEN rp.report_start AND rp.report_end
      AND rank_val = 1
),
care_mgmt_latest AS (
    SELECT rfb_id, contracted_service_id, cms_end_dt
    FROM {{SOURCE_DATABASE}}.dbo.care_mgmt_service cm, report_period rp
    WHERE cm.sequenced_at <= rp.report_end
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY rfb_id, contracted_service_id
        ORDER BY sequenced_at DESC, sequence_no DESC
    ) = 1
),
care_mgmt_source AS (
    SELECT rfb_id
    FROM care_mgmt_latest, report_period rp
    WHERE contracted_service_id IN (28, 31, 48, 47, 77)
      AND cms_end_dt BETWEEN rp.report_start AND rp.report_end
)
SELECT rfb_id FROM eob_source
UNION
SELECT rfb_id FROM care_mgmt_source;

-- =====================================================================
-- STEP 2: CREATE CUSTOM DMF (2 TABLE ARGS ONLY: target + source view)
-- =====================================================================
-- DMF has exactly 2 TABLE arguments (Snowflake limit). Source logic lives in source_rfb_count_monthly view.

CREATE OR REPLACE DATA METRIC FUNCTION source_target_count_difference(
    arg_target TABLE(arg_policy_number VARCHAR),
    arg_source TABLE(arg_rfb_id VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT ABS(
        (SELECT COUNT(*) FROM arg_target) -
        (SELECT COUNT(*) FROM arg_source)
    )
$$;

-- =====================================================================
-- STEP 3: ADD DMF TO TARGET TABLE/VIEW
-- =====================================================================
-- Exactly 2 TABLE arguments: (1) target, (2) source_rfb_count_monthly view
-- Column names in ON clause must match actual table/view column names
-- If you previously added this DMF with 4 table args, DROP it first (see MAINTENANCE below), then run ADD.

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION source_target_count_difference ON (
    "Policy Number",
    TABLE IDENTIFIER($source_view)(rfb_id)
);

-- =====================================================================
-- STEP 4: CREATE EXPECTATION
-- =====================================================================
-- Expectation defines pass/fail criteria for DMF result
-- VALUE keyword represents the DMF's returned number
-- Expression must be deterministic Boolean

CREATE OR REPLACE EXPECTATION source_target_count_match
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION source_target_count_difference(
    "Policy Number",
    TABLE IDENTIFIER($source_view)(rfb_id)
)
EXPECT VALUE = 0
WITH COMMENT 'DQ-024: Source count must match target count (eob_ranking + care_mgmt_ranking). Returns absolute difference: 0 = match, >0 = mismatch.';

-- =====================================================================
-- STEP 4b: STATISTICS DMFs (MIN/MAX) ON new_rfb_and_total_claimants_active_detail
-- =====================================================================
-- Numeric columns: DAYS (working days), NumDaysResolvedWithinTwoWeeks (0/1).
-- Aligns with test_data_quality: DQ-008 (no negative DAYS), DQ-009 (DAYS <= 730),
-- DQ-012 (flag 0 or 1 only).
-- Report creates a TABLE (save_as_table). Use ALTER TABLE if target is a table;
-- use ALTER VIEW if target is a view. Change IDENTIFIER target accordingly.

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (DAYS),
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX ON (DAYS),
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (NumDaysResolvedWithinTwoWeeks),
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX ON (NumDaysResolvedWithinTwoWeeks);

-- =====================================================================
-- STEP 4c: STATISTICS EXPECTATIONS
-- =====================================================================

CREATE OR REPLACE EXPECTATION days_min_non_negative
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN(DAYS)
EXPECT VALUE >= 0
WITH COMMENT 'DQ-008: No negative turnaround days.';

CREATE OR REPLACE EXPECTATION days_max_bounded
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX(DAYS)
EXPECT VALUE <= 730
WITH COMMENT 'DQ-009: No unrealistic turnaround times (> 730 days).';

CREATE OR REPLACE EXPECTATION two_week_flag_min
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN(NumDaysResolvedWithinTwoWeeks)
EXPECT VALUE >= 0
WITH COMMENT 'NumDaysResolvedWithinTwoWeeks must be 0 or 1 only (min >= 0).';

CREATE OR REPLACE EXPECTATION two_week_flag_max
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX(NumDaysResolvedWithinTwoWeeks)
EXPECT VALUE <= 1
WITH COMMENT 'DQ-012: NumDaysResolvedWithinTwoWeeks must be 0 or 1 only (max <= 1).';

-- =====================================================================
-- STEP 5: VERIFY DMF SETUP
-- =====================================================================
-- Check that DMF was added successfully

SELECT 
    ref_entity_name,
    metric_name,
    metric_schema,
    schedule_status,
    last_execution_time,
    next_execution_time
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => $target_table,
    REF_ENTITY_DOMAIN => 'VIEW'
))
ORDER BY metric_name;

-- =====================================================================
-- STEP 6: VIEW RESULTS (AFTER DMF EXECUTION)
-- =====================================================================
-- Query DMF results and expectation status
-- Results are available after scheduled execution or manual execution

SELECT 
    metric_name,
    metric_value,
    expectation_name,
    expectation_status,
    timestamp,
    CASE 
        WHEN expectation_name = 'source_target_count_match' AND expectation_status = 'PASSED' THEN '✓ Counts match'
        WHEN expectation_name = 'source_target_count_match' AND expectation_status = 'FAILED' THEN '✗ Count mismatch: ' || metric_value || ' records differ'
        WHEN expectation_name LIKE 'days_%' OR expectation_name LIKE 'two_week_%' THEN expectation_name || ': ' || expectation_status || ' (value=' || COALESCE(metric_value::VARCHAR, 'null') || ')'
        ELSE 'Status: ' || expectation_status
    END AS status_message
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_RESULTS(
    $target_table
))
ORDER BY timestamp DESC, expectation_name
LIMIT 20;

-- =====================================================================
-- STEP 7: MANUAL EXECUTION (FOR TESTING)
-- =====================================================================
-- Execute DMF immediately for testing (not billed, per Snowflake docs)
-- Replace with actual table/view name and column names

-- USE DATABASE {{TARGET_DATABASE}};  -- if not already in context
/*
-- Custom DMF (source vs target count):
SELECT source_target_count_difference(
    TABLE({{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail("Policy Number")),
    TABLE({{TARGET_SCHEMA}}.source_rfb_count_monthly(rfb_id))
) AS count_difference;

-- Statistics (MIN/MAX) — run directly for testing:
SELECT SNOWFLAKE.CORE.MIN(SELECT DAYS FROM {{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail) AS min_days;
SELECT SNOWFLAKE.CORE.MAX(SELECT DAYS FROM {{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail) AS max_days;
SELECT SNOWFLAKE.CORE.MIN(SELECT NumDaysResolvedWithinTwoWeeks FROM {{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail) AS min_flag;
SELECT SNOWFLAKE.CORE.MAX(SELECT NumDaysResolvedWithinTwoWeeks FROM {{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail) AS max_flag;
*/

-- =====================================================================
-- MAINTENANCE & TROUBLESHOOTING
-- =====================================================================

-- View all DMFs on the object
-- SHOW DATA METRIC FUNCTIONS ON TABLE IDENTIFIER($target_table);

-- View all expectations
-- SHOW EXPECTATIONS ON TABLE IDENTIFIER($target_table);

-- Check schedule parameter
-- SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE IDENTIFIER($target_table);

-- Drop DMF (if needed) - ON clause must match ADD statement exactly
-- ALTER VIEW IDENTIFIER($target_table)
-- DROP DATA METRIC FUNCTION source_target_count_difference ON (
--     "Policy Number",
--     TABLE IDENTIFIER($source_view)(rfb_id)
-- );

-- Drop expectation
-- DROP EXPECTATION source_target_count_match ON TABLE IDENTIFIER($target_table);

-- Drop statistics expectations (if needed)
-- DROP EXPECTATION days_min_non_negative ON TABLE IDENTIFIER($target_table);
-- DROP EXPECTATION days_max_bounded ON TABLE IDENTIFIER($target_table);
-- DROP EXPECTATION two_week_flag_min ON TABLE IDENTIFIER($target_table);
-- DROP EXPECTATION two_week_flag_max ON TABLE IDENTIFIER($target_table);

-- Drop statistics DMFs (ON clause must match ADD exactly)
-- ALTER VIEW IDENTIFIER($target_table) DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (DAYS);
-- ALTER VIEW IDENTIFIER($target_table) DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX ON (DAYS);
-- ALTER VIEW IDENTIFIER($target_table) DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (NumDaysResolvedWithinTwoWeeks);
-- ALTER VIEW IDENTIFIER($target_table) DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX ON (NumDaysResolvedWithinTwoWeeks);

-- =====================================================================
-- DESIGN NOTES & BEST PRACTICES
-- =====================================================================
-- 1. TABLE ARGUMENT LIMIT:
--    - Snowflake DMFs accept only 1 or 2 TABLE arguments. We use target + source_rfb_count_monthly.
--    - Source logic (EOB + care_mgmt + report period) lives in source_rfb_count_monthly view.
--
-- 2. DETERMINISTIC EXPRESSIONS:
--    - DMF body reads only from the two table arguments. No session variables.
--    - source_rfb_count_monthly uses report_period_all_frequencies (MONTHLY); deterministic at query time.
--
-- 3. ACCURACY:
--    - source_rfb_count_monthly replicates DQ-024 logic (eob_ranking + care_mgmt_ranking, UNION).
--    - DMF returns |COUNT(target) - COUNT(source)|; expectation VALUE = 0.
--
-- 4. SNOWFLAKE-SPECIFIC:
--    - QUALIFY in source view; follows DMF documentation patterns.
--
-- 5. STATISTICS (STEP 4b–4c):
--    - MIN/MAX on DAYS: no negative days (>= 0), no unrealistic turnaround (<= 730).
--    - MIN/MAX on NumDaysResolvedWithinTwoWeeks: flag 0 or 1 only.
--    - Maps to DQ-008, DQ-009, DQ-012 in test_data_quality.sql.
-- =====================================================================
