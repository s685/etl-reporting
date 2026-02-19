-- =====================================================================
-- DATA QUALITY TEST SUITE
-- claims_paid_by_process_time_detail Report
-- =====================================================================
-- Purpose: Validate data quality against actual report data
-- Strategy: Tests run against the target table only (no inline sample data)
-- Run after: Report execution completes
-- Target: {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claims_paid_by_process_time_detail
-- =====================================================================
-- Model Logic Reference:
--   "Group" = bucket from PROCESS_DAYS (0-5, 6-10, 11-20, over_20)
--   "Decision" = PAID when (MIN=1 AND MAX=1) OR (MIN=1 AND MAX=2), else UNPAID
--   "No of Days" = working days between PAYREQ_COMPLETE_DT and MIN_SD_EARLIEST_REPORT_DT
-- =====================================================================

SET report_table = '{{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claims_paid_by_process_time_detail';

-- =====================================================================
-- CATEGORY 1: BUCKET ("Group") VALIDATION
-- =====================================================================

-- Test 1.1: "Group" only contains defined bucket labels
SELECT
    'DQ-001: Bucket Label Domain' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Group" contains value outside 4 defined bucket labels' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Group" NOT IN (
    'PROCESSED_0_to_5_DAYS',
    'PROCESSED_6_to_10_DAYS',
    'PROCESSED_11_to_20_DAYS',
    'PROCESSED_over_20_DAYS'
);

-- Test 1.2: "Group" is never NULL or empty
SELECT
    'DQ-002: Bucket Non-NULL' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Group" is NULL — PROCESS_DAYS was NULL (missing PAYREQ_COMPLETE_DT or MIN_DECISION_DATE)' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Group" IS NULL OR TRIM("Group") = '';

-- Test 1.3: All four expected buckets present (informational)
SELECT
    'DQ-003: All Expected Buckets Present' AS TEST_ID,
    CASE WHEN COUNT(DISTINCT "Group") = 4 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    4 - COUNT(DISTINCT "Group") AS FAILED_ROWS,
    'Expected 4 distinct bucket labels; found ' || COUNT(DISTINCT "Group") AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table);

-- Test 1.4: Bucket vs "No of Days" reasonableness
-- Unusual divergence: PROCESSED_0_to_5_DAYS with |No of Days| > 60
SELECT
    'DQ-004: Bucket vs No of Days Reasonableness' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Records in PROCESSED_0_to_5_DAYS have |No of Days| > 60 — unusual divergence' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Group" = 'PROCESSED_0_to_5_DAYS'
  AND ABS("No of Days") > 60;

-- =====================================================================
-- CATEGORY 2: DECISION FIELD VALIDATION
-- =====================================================================

-- Test 2.1: "Decision" only contains PAID or UNPAID
SELECT
    'DQ-005: Decision Domain' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Decision" contains values outside {PAID, UNPAID}' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Decision" NOT IN ('PAID', 'UNPAID');

-- Test 2.2: "Decision" is never NULL
SELECT
    'DQ-006: Decision Non-NULL' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Decision" is NULL — derivation logic gap' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Decision" IS NULL;

-- =====================================================================
-- CATEGORY 3: DATE AND FORMAT VALIDATION
-- =====================================================================

-- Test 3.1: "Payreq Complete Date" parses as valid MM/DD/YYYY
SELECT
    'DQ-007: Payreq Complete Date Format' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Payreq Complete Date" cannot be parsed as MM/DD/YYYY' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Payreq Complete Date" IS NOT NULL
  AND TRY_TO_DATE("Payreq Complete Date", 'MM/DD/YYYY') IS NULL;

-- Test 3.2: "XOB Date" parses as valid MM/DD/YYYY
SELECT
    'DQ-008: XOB Date Format' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"XOB Date" cannot be parsed as MM/DD/YYYY' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "XOB Date" IS NOT NULL
  AND TRY_TO_DATE("XOB Date", 'MM/DD/YYYY') IS NULL;

-- Test 3.3: "Payreq Complete Date" is not in the future
SELECT
    'DQ-009: Future Payreq Complete Date' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Payreq Complete Date" in future — data integrity issue' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE TRY_TO_DATE("Payreq Complete Date", 'MM/DD/YYYY') > CURRENT_DATE();

-- Test 3.4: REPORT_DT is consistent across all rows
SELECT
    'DQ-010: Consistent REPORT_DT' AS TEST_ID,
    CASE WHEN COUNT(DISTINCT REPORT_DT) <= 1 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    GREATEST(0, COUNT(DISTINCT REPORT_DT) - 1) AS FAILED_ROWS,
    'Multiple distinct REPORT_DT values — session variable not applied uniformly' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table);

-- =====================================================================
-- CATEGORY 4: NULL / CRITICAL FIELD CHECKS
-- =====================================================================

-- Test 4.1: "Policy #" is never NULL
SELECT
    'DQ-011: NULL Policy #' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Policy #" is required for claim identification' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Policy #" IS NULL;

-- Test 4.2: CARRIER_NAME is never NULL or empty
SELECT
    'DQ-012: NULL or Empty CARRIER_NAME' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'CARRIER_NAME is required for all records' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE CARRIER_NAME IS NULL OR TRIM(CARRIER_NAME) = '';

-- Test 4.3: "Doc #" is never NULL
SELECT
    'DQ-013: NULL Doc #' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Doc #" is required for document tracking' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Doc #" IS NULL;

-- Test 4.4: "User ID" is never NULL
SELECT
    'DQ-014: NULL User ID' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"User ID" is required for audit trail' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "User ID" IS NULL;

-- Test 4.5: "Payreq Complete Date" is never NULL or empty
SELECT
    'DQ-015: NULL Payreq Complete Date' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Payreq Complete Date" is required for PROCESS_DAYS and NO_OF_DAYS calculations' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Payreq Complete Date" IS NULL OR TRIM("Payreq Complete Date") = '';

-- Test 4.6: "Group Name" is not NULL or empty (optional)
SELECT
    'DQ-016: NULL or Empty Group Name' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Group Name" is needed for group-level reporting' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "Group Name" IS NULL OR TRIM("Group Name") = '';

-- =====================================================================
-- CATEGORY 5: REASONABLENESS AND DEDUPLICATION
-- =====================================================================

-- Test 5.1: "No of Days" is not unreasonably large (> 365 working days)
SELECT
    'DQ-017: Unreasonable No of Days' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '|No of Days| exceeds 365 — investigate source data' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE ABS("No of Days") > 365;

-- Test 5.2: "No of Days" is not NULL
SELECT
    'DQ-018: NULL No of Days' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"No of Days" is NULL — source dates may be missing' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "No of Days" IS NULL;

-- Test 5.3: No duplicate rows (Policy + Doc + Payreq Complete Date + Decision)
SELECT
    'DQ-019: Duplicate Row Detection' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Duplicate rows detected — RANK filter or join fan-out issue' AS BUSINESS_IMPACT
FROM (
    SELECT "Policy #", "Doc #", "Payreq Complete Date", "Decision", COUNT(*) AS cnt
    FROM IDENTIFIER($report_table)
    GROUP BY "Policy #", "Doc #", "Payreq Complete Date", "Decision"
    HAVING COUNT(*) > 1
);

-- Test 5.4: "XOB Date" is never NULL or empty
SELECT
    'DQ-020: NULL XOB Date' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"XOB Date" is NULL — INNER JOIN on working days should prevent this' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE "XOB Date" IS NULL OR TRIM("XOB Date") = '';

-- Test 5.5: "XOB Date" falls on a working day (not weekend)
-- Sunday=0, Saturday=6 in Snowflake DAYOFWEEK
SELECT
    'DQ-021: XOB Date is a Working Day' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"XOB Date" falls on weekend — calendar working days join incorrect' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE DAYOFWEEK(TRY_TO_DATE("XOB Date", 'MM/DD/YYYY')) IN (0, 6);

-- =====================================================================
-- CATEGORY 6: INFORMATIONAL
-- =====================================================================

-- Test 6.1: Bucket record count distribution (informational)
SELECT
    'DQ-022: Bucket Record Counts' AS TEST_ID,
    'INFO' AS STATUS,
    0 AS FAILED_ROWS,
    'Per-bucket record counts: ' || COALESCE(counts_str, 'N/A') AS BUSINESS_IMPACT
FROM (
    SELECT LISTAGG(grp || '=' || cnt, ', ') WITHIN GROUP (ORDER BY grp) AS counts_str
    FROM (
        SELECT "Group" AS grp, COUNT(*) AS cnt
        FROM IDENTIFIER($report_table)
        GROUP BY "Group"
    ) t
);

-- Test 6.2: XOB Date before Payreq Complete Date (unusual)
SELECT
    'DQ-023: XOB Date Before Payreq Complete Date' AS TEST_ID,
    'INFO' AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'XOB Date before Payreq Complete Date — unusual but may be valid' AS BUSINESS_IMPACT
FROM IDENTIFIER($report_table)
WHERE TRY_TO_DATE("XOB Date", 'MM/DD/YYYY') < TRY_TO_DATE("Payreq Complete Date", 'MM/DD/YYYY');

-- =====================================================================
-- FINAL SUMMARY
-- =====================================================================
SELECT
    '=== DATA QUALITY TEST SUITE COMPLETE ===' AS MESSAGE,
    'Review all test results above' AS INSTRUCTIONS,
    'Tests with FAIL status must be addressed before production' AS CRITICAL_ACTION,
    'Tests with WARNING status should be investigated' AS RECOMMENDED_ACTION,
    'Tests with INFO status are informational only' AS INFORMATIONAL;
