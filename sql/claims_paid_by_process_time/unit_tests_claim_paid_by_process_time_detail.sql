-- =====================================================================
-- UNIT TEST SUITE: claim_paid_by_process_time_detail
-- =====================================================================
-- Purpose  : Validate individual CTE logic, calculated fields, and
--            bucket boundary conditions at the unit level
-- Model    : claim_paid_by_process_time_detail
-- Focus    : payment_request_paid_unpaid CTE, process time calculation,
--            bucket assignment logic, and NULL/edge-case handling
-- Run on   : {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claim_paid_by_process_time_detail
-- =====================================================================

SET report_table = '{{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claim_paid_by_process_time_detail';

-- =====================================================================
-- CATEGORY 1: PROCESS TIME CALCULATION LOGIC
-- =====================================================================

-- UT-001: Process time must equal (payment_dt - received_dt) in calendar days
-- Validates that the process_time_days field is arithmetically correct
SELECT
    'UT-001: Process Time Calculation Accuracy' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'process_time_days does not match DATEDIFF(day, received_dt, payment_dt)' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'received_dt',        received_dt,
        'payment_dt',         payment_dt,
        'stored_days',        process_time_days,
        'expected_days',      DATEDIFF('day', received_dt, payment_dt)
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE payment_dt IS NOT NULL
  AND received_dt IS NOT NULL
  AND process_time_days <> DATEDIFF('day', received_dt, payment_dt);

-- UT-002: Negative process time is impossible
-- payment_dt must always be >= received_dt; negative values indicate bad source data
SELECT
    'UT-002: Negative Process Time Detection' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'payment_dt is earlier than received_dt — data integrity violation' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'received_dt',        received_dt,
        'payment_dt',         payment_dt,
        'process_time_days',  process_time_days
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE process_time_days < 0;

-- UT-003: Zero-day (same-day) process time handling
-- Claims paid same day (process_time_days = 0) must be assigned to the
-- earliest bucket; verifies edge of bucket boundary at zero
SELECT
    'UT-003: Same-Day Payment Bucket Assignment' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Claims with process_time_days = 0 must map to the "0" or "0-5 days" bucket' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no',  payment_request_no,
        'process_time_days',   process_time_days,
        'process_time_bucket', process_time_bucket
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE process_time_days = 0
  AND process_time_bucket NOT IN ('0', '0 Days', '0-5 Days', '0-5', 'Same Day');

-- UT-004: Future payment date detection
-- payment_dt should not be later than today's date
SELECT
    'UT-004: Future Payment Date Detection' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'payment_dt is in the future — indicates system clock or data-load error' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'payment_dt',         payment_dt
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE payment_dt > CURRENT_DATE();

-- UT-005: Process time is NULL when either date is missing
-- If received_dt or payment_dt is NULL the process_time_days must also be NULL
-- (no implied zero or default)
SELECT
    'UT-005: Process Time NULL Consistency' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'process_time_days has a value when a required date is NULL — invalid derivation' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'received_dt',        received_dt,
        'payment_dt',         payment_dt,
        'process_time_days',  process_time_days
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE (received_dt IS NULL OR payment_dt IS NULL)
  AND process_time_days IS NOT NULL;

-- =====================================================================
-- CATEGORY 2: BUCKET BOUNDARY CONDITIONS
-- =====================================================================

-- UT-006: Mutually exclusive buckets
-- Each record must belong to exactly one process_time_bucket; if the
-- detail model exposes one row per record, a GROUP BY count > 1 is impossible
-- but we validate the bucket value itself is non-NULL and non-empty
SELECT
    'UT-006: Bucket Assignment is Non-NULL' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'process_time_bucket is NULL — every record must be classified into a bucket' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'process_time_days',  process_time_days
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE process_time_bucket IS NULL
   OR TRIM(process_time_bucket) = '';

-- UT-007: Exhaustive bucket coverage — all defined buckets are present
-- This test is informational; if a bucket is completely absent it may
-- indicate a CASE expression gap or no data for that range
SELECT
    'UT-007: All Expected Buckets Present' AS TEST_ID,
    CASE
        WHEN COUNT(DISTINCT process_time_bucket) >= expected_bucket_count THEN 'PASS'
        ELSE 'WARNING'
    END AS STATUS,
    expected_bucket_count - COUNT(DISTINCT process_time_bucket) AS MISSING_BUCKETS,
    'Some process time buckets contain no data — verify CASE logic covers all ranges' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'distinct_buckets_found',   COUNT(DISTINCT process_time_bucket),
        'expected_bucket_count',    expected_bucket_count
    ) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
CROSS JOIN (SELECT 6 AS expected_bucket_count) cfg; -- adjust to actual number of buckets

-- UT-008: Bucket label matches process_time_days value
-- Verifies CASE expression boundaries are internally consistent;
-- each stored bucket label must agree with the stored numeric process_time_days
-- IMPORTANT: adjust bucket boundary constants below to match the model's CASE statement
SELECT
    'UT-008: Bucket Label vs Days Consistency' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Bucket label does not agree with process_time_days — CASE expression boundary mismatch' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no',  payment_request_no,
        'process_time_days',   process_time_days,
        'process_time_bucket', process_time_bucket
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE process_time_days IS NOT NULL
  AND NOT (
        -- Adjust these ranges to match the exact CASE statement in the model
        (process_time_days = 0          AND process_time_bucket IN ('0', '0 Days', '0-5 Days', 'Same Day'))
     OR (process_time_days BETWEEN 1  AND 5   AND process_time_bucket IN ('1-5 Days',  '0-5 Days', '1-5'))
     OR (process_time_days BETWEEN 6  AND 10  AND process_time_bucket IN ('6-10 Days', '6-10'))
     OR (process_time_days BETWEEN 11 AND 15  AND process_time_bucket IN ('11-15 Days','11-15'))
     OR (process_time_days BETWEEN 16 AND 30  AND process_time_bucket IN ('16-30 Days','16-30'))
     OR (process_time_days >= 31               AND process_time_bucket IN ('31+ Days',  '31+', 'Over 30 Days'))
  );

-- UT-009: Exact lower boundary of each bucket (day = first day of bucket)
-- E.g., a record with process_time_days = 6 must land in the "6-10 Days" bucket
SELECT
    'UT-009: Lower Boundary of 6-10 Days Bucket' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Record with exactly 6 days does not fall in the 6-10 Days bucket' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no',  payment_request_no,
        'process_time_days',   process_time_days,
        'process_time_bucket', process_time_bucket
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE process_time_days = 6
  AND process_time_bucket NOT IN ('6-10 Days', '6-10');

-- UT-010: Exact upper boundary of each bucket (day = last day of bucket)
-- E.g., a record with process_time_days = 10 must land in the "6-10 Days" bucket,
-- NOT the "11-15 Days" bucket (off-by-one error detection)
SELECT
    'UT-010: Upper Boundary of 6-10 Days Bucket' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Record with exactly 10 days does not fall in the 6-10 Days bucket — off-by-one error' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no',  payment_request_no,
        'process_time_days',   process_time_days,
        'process_time_bucket', process_time_bucket
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE process_time_days = 10
  AND process_time_bucket NOT IN ('6-10 Days', '6-10');

-- UT-011: Exact upper boundary of 31+ bucket (first day of open-ended bucket)
SELECT
    'UT-011: Lower Boundary of 31+ Days Bucket' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Record with exactly 31 days does not fall in the 31+ bucket' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no',  payment_request_no,
        'process_time_days',   process_time_days,
        'process_time_bucket', process_time_bucket
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE process_time_days = 31
  AND process_time_bucket NOT IN ('31+ Days', '31+', 'Over 30 Days');

-- =====================================================================
-- CATEGORY 3: payment_request_paid_unpaid CTE LOGIC
-- =====================================================================

-- UT-012: Paid flag derivation — paid records must have a valid payment amount
-- Records marked as paid should carry a non-NULL, non-zero paid_amount
SELECT
    'UT-012: Paid Flag vs Paid Amount Consistency' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Record is flagged as PAID but has NULL or zero paid_amount — derivation error' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'paid_flag',          paid_flag,
        'paid_amount',        paid_amount
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE UPPER(TRIM(paid_flag)) = 'PAID'
  AND (paid_amount IS NULL OR paid_amount = 0);

-- UT-013: Unpaid records should not have a payment_dt
-- If a claim is unpaid, it should not carry a payment date
SELECT
    'UT-013: Unpaid Flag vs Payment Date Consistency' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Record is flagged as UNPAID but has a payment_dt — contradictory data' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'paid_flag',          paid_flag,
        'payment_dt',         payment_dt
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE UPPER(TRIM(paid_flag)) = 'UNPAID'
  AND payment_dt IS NOT NULL;

-- UT-014: No duplicate payment_request_no within the detail model
-- Each payment request should appear exactly once in the output
SELECT
    'UT-014: Duplicate Payment Request Detection' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Same payment_request_no appears more than once — fan-out in join or CTE' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'row_count',          cnt
    )) WITHIN GROUP (ORDER BY cnt DESC LIMIT 10) AS SAMPLE_RECORDS
FROM (
    SELECT payment_request_no, COUNT(*) AS cnt
    FROM IDENTIFIER($report_table)
    GROUP BY payment_request_no
    HAVING COUNT(*) > 1
);

-- UT-015: paid_flag only contains expected values
-- The flag should be one of a known set; unexpected values indicate upstream changes
SELECT
    'UT-015: paid_flag Domain Validation' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'paid_flag contains unexpected values outside of {PAID, UNPAID}' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'paid_flag',          paid_flag
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE UPPER(TRIM(paid_flag)) NOT IN ('PAID', 'UNPAID');

-- =====================================================================
-- CATEGORY 4: NULL / CRITICAL FIELD CHECKS
-- =====================================================================

-- UT-016: payment_request_no is never NULL
SELECT
    'UT-016: NULL payment_request_no' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'payment_request_no is the primary key — must never be NULL' AS BUSINESS_IMPACT,
    NULL AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE payment_request_no IS NULL;

-- UT-017: received_dt is never NULL for PAID records
SELECT
    'UT-017: NULL received_dt on PAID Records' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Paid records require received_dt to calculate process time' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'paid_flag',          paid_flag
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE UPPER(TRIM(paid_flag)) = 'PAID'
  AND received_dt IS NULL;

-- UT-018: carrier_name is never NULL or empty
SELECT
    'UT-018: NULL or Empty carrier_name' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'carrier_name is required for all records' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT('payment_request_no', payment_request_no))
        WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE carrier_name IS NULL OR TRIM(carrier_name) = '';

-- UT-019: policy_no is never NULL
SELECT
    'UT-019: NULL policy_no' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'policy_no is required to link to the policy dimension' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT('payment_request_no', payment_request_no))
        WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE policy_no IS NULL;

-- UT-020: paid_amount must be non-negative
SELECT
    'UT-020: Negative paid_amount' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'paid_amount cannot be negative — indicates a data-load or sign error' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'paid_amount',        paid_amount
    )) WITHIN GROUP (ORDER BY paid_amount LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE paid_amount < 0;

-- =====================================================================
-- SUMMARY: Unit Test Results Rollup
-- =====================================================================
SELECT
    '=== UNIT TEST SUMMARY ===' AS TEST_SUITE,
    SUM(CASE WHEN STATUS = 'FAIL'    THEN 1 ELSE 0 END) AS TOTAL_FAILED,
    SUM(CASE WHEN STATUS = 'PASS'    THEN 1 ELSE 0 END) AS TOTAL_PASSED,
    SUM(CASE WHEN STATUS = 'WARNING' THEN 1 ELSE 0 END) AS TOTAL_WARNINGS,
    SUM(FAILED_ROWS) AS TOTAL_ISSUE_ROWS,
    CASE
        WHEN SUM(CASE WHEN STATUS = 'FAIL' THEN 1 ELSE 0 END) = 0
        THEN 'ALL UNIT TESTS PASSED'
        ELSE 'CRITICAL: UNIT TEST FAILURES DETECTED — DO NOT PROMOTE TO PRODUCTION'
    END AS OVERALL_STATUS
FROM (
    SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS, COUNT(*) AS FAILED_ROWS FROM IDENTIFIER($report_table) WHERE process_time_days IS NOT NULL AND process_time_days <> DATEDIFF('day', received_dt, payment_dt) AND payment_dt IS NOT NULL AND received_dt IS NOT NULL
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE process_time_days < 0
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE process_time_days = 0 AND process_time_bucket NOT IN ('0', '0 Days', '0-5 Days', 'Same Day')
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE payment_dt > CURRENT_DATE()
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE (received_dt IS NULL OR payment_dt IS NULL) AND process_time_days IS NOT NULL
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE process_time_bucket IS NULL OR TRIM(process_time_bucket) = ''
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE process_time_days = 6 AND process_time_bucket NOT IN ('6-10 Days', '6-10')
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE process_time_days = 10 AND process_time_bucket NOT IN ('6-10 Days', '6-10')
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE process_time_days = 31 AND process_time_bucket NOT IN ('31+ Days', '31+', 'Over 30 Days')
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE UPPER(TRIM(paid_flag)) = 'PAID' AND (paid_amount IS NULL OR paid_amount = 0)
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE UPPER(TRIM(paid_flag)) = 'UNPAID' AND payment_dt IS NOT NULL
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM (SELECT payment_request_no, COUNT(*) AS cnt FROM IDENTIFIER($report_table) GROUP BY payment_request_no HAVING COUNT(*) > 1)
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE UPPER(TRIM(paid_flag)) NOT IN ('PAID', 'UNPAID')
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE payment_request_no IS NULL
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE UPPER(TRIM(paid_flag)) = 'PAID' AND received_dt IS NULL
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE carrier_name IS NULL OR TRIM(carrier_name) = ''
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE policy_no IS NULL
    UNION ALL SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*) FROM IDENTIFIER($report_table) WHERE paid_amount < 0
);
