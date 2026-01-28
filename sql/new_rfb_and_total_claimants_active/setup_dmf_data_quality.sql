-- =====================================================================
-- SNOWFLAKE DMF SETUP: Source vs Target Record Count Difference (DQ-024)
-- =====================================================================
-- Purpose: Compare total record count in target table vs source data count
-- 
-- Prerequisites:
--   1. Enterprise Edition (docs: data-quality-intro)
--   2. EXECUTE DATA METRIC FUNCTION on account (data-quality-access-control)
--   3. Owner is custom or system role, not database role
-- 
-- Docs: https://docs.snowflake.com/en/user-guide/data-quality-intro
--       https://docs.snowflake.com/en/sql-reference/sql/create-data-metric-function
-- =====================================================================

-- Replace with your actual database and schema
SET target_table = '{{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail';
SET source_database = '{{SOURCE_DATABASE}}';

-- Use ALTER VIEW for views, ALTER TABLE for tables
-- Target new_rfb_and_total_claimants_active_detail is a VIEW

-- =====================================================================
-- STEP 1: SET SCHEDULE (REQUIRED BEFORE ADDING DMFs)
-- =====================================================================
-- Per docs: Set DATA_METRIC_SCHEDULE before associating any DMF
-- All DMFs on this object share the same schedule

ALTER VIEW IDENTIFIER($target_table)
SET DATA_METRIC_SCHEDULE = 'USING CRON 0 8,14,20 * * * UTC';

-- =====================================================================
-- STEP 2: CREATE CUSTOM DMF
-- =====================================================================
-- Custom DMF: Source vs Target count difference (DQ-024)
-- Compares total record count in target table vs source data count.
-- Returns absolute difference: 0 = match, >0 = mismatch.
-- 
-- Implementation (per Snowflake DMF docs):
-- - Target: COUNT(*) of all rows in target table
-- - Source: COUNT(*) from UNION of eob_ranking + care_mgmt_ranking
-- - Uses target table dates to infer report period (deterministic, no session vars)
-- - No views required - direct table access via multiple TABLE arguments
-- 
-- Docs: https://docs.snowflake.com/en/sql-reference/sql/create-data-metric-function
--       https://docs.snowflake.com/en/user-guide/data-quality-custom-dmfs

CREATE OR REPLACE DATA METRIC FUNCTION source_target_count_difference(
    arg_target TABLE(
        arg_policy_number VARCHAR,
        arg_statistical_start_date VARCHAR,
        arg_edb_decision_date VARCHAR
    ),
    arg_eob TABLE(
        arg_eob_id VARCHAR,
        arg_eob_rfb_id VARCHAR,
        arg_eob_decision_dt DATE,
        arg_eob_last_mod_dt DATE,
        arg_eob_sequence_no NUMBER
    ),
    arg_care_mgmt TABLE(
        arg_cm_rfb_id VARCHAR,
        arg_cm_service_id NUMBER,
        arg_cm_end_dt DATE,
        arg_cm_sequenced_at DATE,
        arg_cm_sequence_no NUMBER
    )
)
RETURNS NUMBER
AS $$
    WITH
    -- Determine report period from target table dates (deterministic)
    report_period AS (
        SELECT 
            MIN(TRY_TO_DATE(arg_statistical_start_date, 'MM/DD/YYYY')) AS report_start,
            MAX(TRY_TO_DATE(arg_edb_decision_date, 'MM/DD/YYYY')) AS report_end
        FROM arg_target
        WHERE arg_statistical_start_date IS NOT NULL 
          AND arg_edb_decision_date IS NOT NULL
    ),
    -- Episode of Benefit latest records (QUALIFY ROW_NUMBER logic)
    eob_latest AS (
        SELECT 
            arg_eob_id,
            arg_eob_rfb_id,
            arg_eob_decision_dt,
            arg_eob_last_mod_dt,
            arg_eob_sequence_no,
            ROW_NUMBER() OVER (
                PARTITION BY arg_eob_id 
                ORDER BY arg_eob_last_mod_dt DESC, arg_eob_sequence_no DESC
            ) AS rn
        FROM arg_eob
        CROSS JOIN report_period
        WHERE arg_eob_last_mod_dt <= report_period.report_end
    ),
    eob_filtered AS (
        SELECT * FROM eob_latest WHERE rn = 1
    ),
    -- EOB ranking with first decision date per RFB
    eob_ranking AS (
        SELECT
            arg_eob_rfb_id AS rfb_id,
            MIN(arg_eob_decision_dt) OVER (PARTITION BY arg_eob_rfb_id) AS firstebdecisiondt,
            ROW_NUMBER() OVER (PARTITION BY arg_eob_rfb_id ORDER BY arg_eob_decision_dt) AS firstebdecisiondt_rank
        FROM eob_filtered
    ),
    -- Care Management Service latest records
    care_mgmt_latest AS (
        SELECT 
            arg_cm_rfb_id,
            arg_cm_service_id,
            arg_cm_end_dt,
            arg_cm_sequenced_at,
            arg_cm_sequence_no,
            ROW_NUMBER() OVER (
                PARTITION BY arg_cm_rfb_id, arg_cm_service_id 
                ORDER BY arg_cm_sequenced_at DESC, arg_cm_sequence_no DESC
            ) AS rn
        FROM arg_care_mgmt
        CROSS JOIN report_period
        WHERE arg_cm_sequenced_at <= report_period.report_end
    ),
    care_mgmt_filtered AS (
        SELECT * FROM care_mgmt_latest WHERE rn = 1
    ),
    -- Care Management ranking for specific service types (28, 31, 48, 47, 77)
    care_mgmt_ranking AS (
        SELECT arg_cm_rfb_id AS rfb_id
        FROM care_mgmt_filtered
        CROSS JOIN report_period
        WHERE arg_cm_service_id IN (28, 31, 48, 47, 77)
          AND arg_cm_end_dt BETWEEN report_period.report_start AND report_period.report_end
    ),
    -- Source count: UNION of eob_ranking and care_mgmt_ranking (total records)
    source_data AS (
        SELECT rfb_id FROM eob_ranking
        CROSS JOIN report_period
        WHERE firstebdecisiondt BETWEEN report_period.report_start AND report_period.report_end
          AND firstebdecisiondt_rank = 1
        UNION
        SELECT rfb_id FROM care_mgmt_ranking
    ),
    -- Counts: Total records in target vs total records in source
    counts AS (
        SELECT 
            (SELECT COUNT(*) FROM arg_target) AS target_total_records,
            (SELECT COUNT(*) FROM source_data) AS source_total_records
    )
    SELECT ABS(target_total_records - source_total_records) FROM counts
$$;

-- =====================================================================
-- STEP 3: ADD DMF TO TARGET TABLE/VIEW
-- =====================================================================
-- Docs: alter-view, alter-table (ADD DATA METRIC FUNCTION with multiple TABLE arguments)

ALTER VIEW IDENTIFIER($target_table)
ADD DATA METRIC FUNCTION source_target_count_difference ON (
    "Policy Number",
    "Statistical Start Date",
    "EDB Decision Date",
    TABLE IDENTIFIER($source_database || '.dbo.episode_of_benefit')(
        episode_of_benefit_id,
        rfb_id,
        eb_decision_dt,
        last_mod_dt,
        sequence_no
    ),
    TABLE IDENTIFIER($source_database || '.dbo.care_mgmt_service')(
        rfb_id,
        contracted_service_id,
        cms_end_dt,
        sequenced_at,
        sequence_no
    )
);

-- =====================================================================
-- STEP 4: CREATE EXPECTATION
-- =====================================================================
-- Docs: data-quality-expectations (EXPECT VALUE = 0 means counts match)

CREATE OR REPLACE EXPECTATION source_target_count_match
ON TABLE IDENTIFIER($target_table)
FOR DATA METRIC FUNCTION source_target_count_difference(
    "Policy Number",
    "Statistical Start Date",
    "EDB Decision Date",
    TABLE IDENTIFIER($source_database || '.dbo.episode_of_benefit')(
        episode_of_benefit_id,
        rfb_id,
        eb_decision_dt,
        last_mod_dt,
        sequence_no
    ),
    TABLE IDENTIFIER($source_database || '.dbo.care_mgmt_service')(
        rfb_id,
        contracted_service_id,
        cms_end_dt,
        sequenced_at,
        sequence_no
    )
)
EXPECT VALUE = 0
WITH COMMENT 'DQ-024: Source count must match target count (eob_ranking + care_mgmt_ranking)';

-- =====================================================================
-- STEP 5: VIEW RESULTS
-- =====================================================================
-- Query DMF results and expectation status

SELECT 
    metric_name,
    metric_value,
    expectation_name,
    expectation_status,
    timestamp
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_RESULTS(
    $target_table
))
WHERE expectation_name = 'source_target_count_match'
ORDER BY timestamp DESC
LIMIT 10;

-- =====================================================================
-- NOTES
-- =====================================================================
-- 1. DMF compares total record counts: target table vs source (eob + care_mgmt)
-- 2. Returns absolute difference: 0 = match, >0 = mismatch
-- 3. Expression is deterministic (uses target table dates, no session variables)
-- 4. For tables, use ALTER TABLE instead of ALTER VIEW
-- 5. Schedule can be modified: ALTER VIEW ... SET DATA_METRIC_SCHEDULE = '...'
-- =====================================================================
