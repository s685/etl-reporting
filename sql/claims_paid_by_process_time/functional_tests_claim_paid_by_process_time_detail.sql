-- =====================================================================
-- FUNCTIONAL TEST SUITE: claim_paid_by_process_time_detail
-- =====================================================================
-- Purpose  : Validate end-to-end business logic, data completeness,
--            referential integrity, and statistical reasonableness
-- Model    : claim_paid_by_process_time_detail
-- Focus    : Full pipeline correctness — source-to-target reconciliation,
--            bucket distribution, SLA metrics, and report period scoping
-- Run on   : {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claim_paid_by_process_time_detail
-- Depends  : Base model {{SOURCE_DATABASE}}.{{SOURCE_SCHEMA}}.claim_paid_activity
-- =====================================================================

SET report_table   = '{{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claim_paid_by_process_time_detail';
SET source_table   = '{{SOURCE_DATABASE}}.{{SOURCE_SCHEMA}}.claim_paid_activity';
SET carrier_name   = '{{CARRIER_NAME}}';
SET report_start   = '{{REPORT_START_DT}}';
SET report_end     = '{{REPORT_END_DT}}';

-- =====================================================================
-- CATEGORY 1: SOURCE-TO-TARGET COUNT RECONCILIATION
-- =====================================================================

-- FT-001: Total record count must match base model (claim_paid_activity)
-- Verifies no records were silently dropped or duplicated during the
-- join/CTE chain in claim_paid_by_process_time_detail
WITH
source_count AS (
    SELECT COUNT(*) AS cnt
    FROM IDENTIFIER($source_table)
    WHERE carrier_name = $carrier_name
      AND payment_dt BETWEEN TO_DATE($report_start) AND TO_DATE($report_end)
      AND UPPER(TRIM(paid_flag)) = 'PAID'
),
target_count AS (
    SELECT COUNT(*) AS cnt
    FROM IDENTIFIER($report_table)
)
SELECT
    'FT-001: Source vs Target Record Count' AS TEST_ID,
    CASE WHEN s.cnt = t.cnt THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    ABS(s.cnt - t.cnt) AS FAILED_ROWS,
    'Source count (' || s.cnt || ') must equal target count (' || t.cnt || ')' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'source_count', s.cnt,
        'target_count', t.cnt,
        'delta',        s.cnt - t.cnt
    ) AS SAMPLE_RECORDS
FROM source_count s
CROSS JOIN target_count t;

-- FT-002: Sum of paid_amount reconciles between source and target
-- Validates no monetary value is gained or lost during transformation
WITH
source_total AS (
    SELECT SUM(paid_amount) AS total_paid
    FROM IDENTIFIER($source_table)
    WHERE carrier_name = $carrier_name
      AND payment_dt BETWEEN TO_DATE($report_start) AND TO_DATE($report_end)
      AND UPPER(TRIM(paid_flag)) = 'PAID'
),
target_total AS (
    SELECT SUM(paid_amount) AS total_paid
    FROM IDENTIFIER($report_table)
    WHERE UPPER(TRIM(paid_flag)) = 'PAID'
)
SELECT
    'FT-002: Paid Amount Reconciliation' AS TEST_ID,
    CASE WHEN ABS(COALESCE(s.total_paid, 0) - COALESCE(t.total_paid, 0)) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    ABS(COALESCE(s.total_paid, 0) - COALESCE(t.total_paid, 0)) AS VARIANCE,
    'Total paid amount variance between source and target exceeds $0.01 threshold' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'source_total_paid', ROUND(s.total_paid, 2),
        'target_total_paid', ROUND(t.total_paid, 2),
        'variance',          ROUND(ABS(COALESCE(s.total_paid, 0) - COALESCE(t.total_paid, 0)), 2)
    ) AS SAMPLE_RECORDS
FROM source_total s
CROSS JOIN target_total t;

-- =====================================================================
-- CATEGORY 2: DATE RANGE SCOPING
-- =====================================================================

-- FT-003: All records must have payment_dt within the report period
-- Validates the date filter in the model does not pass through out-of-range claims
SELECT
    'FT-003: Records Within Report Period' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Records found with payment_dt outside [$report_start, $report_end]' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'payment_dt',         payment_dt
    )) WITHIN GROUP (ORDER BY payment_dt LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE payment_dt NOT BETWEEN TO_DATE($report_start) AND TO_DATE($report_end);

-- FT-004: No records from before the business start boundary (sanity check)
-- Protects against overly broad date filters admitting historical noise
SELECT
    'FT-004: No Pre-Period Records' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Records found with payment_dt before report_start_dt — date filter too wide' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'payment_dt',         payment_dt
    )) WITHIN GROUP (ORDER BY payment_dt LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE payment_dt < TO_DATE($report_start);

-- =====================================================================
-- CATEGORY 3: CARRIER FILTERING
-- =====================================================================

-- FT-005: Single carrier per report run
-- Report is parameterized by carrier; mixing carriers indicates filter failure
SELECT
    'FT-005: Single Carrier Consistency' AS TEST_ID,
    CASE WHEN COUNT(DISTINCT carrier_name) = 1 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(DISTINCT carrier_name) - 1 AS EXTRA_CARRIERS,
    'More than one carrier found — carrier_name session variable not applied' AS BUSINESS_IMPACT,
    ARRAY_AGG(DISTINCT carrier_name) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table);

-- FT-006: Carrier name matches the session variable value
SELECT
    'FT-006: Carrier Matches Session Variable' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Records found with carrier_name != $carrier_name' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'carrier_name',       carrier_name
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE carrier_name <> $carrier_name;

-- =====================================================================
-- CATEGORY 4: BUCKET DISTRIBUTION ANALYSIS
-- =====================================================================

-- FT-007: Process time bucket distribution is statistically reasonable
-- No single bucket should contain more than 90% of records;
-- extreme skew may indicate a CASE statement bug or data quality issue
SELECT
    'FT-007: Bucket Distribution Reasonableness' AS TEST_ID,
    CASE
        WHEN MAX(pct_of_total) > 90 THEN 'WARNING'
        ELSE 'PASS'
    END AS STATUS,
    SUM(CASE WHEN pct_of_total > 90 THEN record_count ELSE 0 END) AS FAILED_ROWS,
    'A single bucket contains > 90% of records — verify CASE expression' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'bucket_distribution', ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'bucket',        process_time_bucket,
                'record_count',  record_count,
                'pct_of_total',  ROUND(pct_of_total, 2)
            )
        )
    ) AS SAMPLE_RECORDS
FROM (
    SELECT
        process_time_bucket,
        COUNT(*)                                        AS record_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()       AS pct_of_total
    FROM IDENTIFIER($report_table)
    GROUP BY process_time_bucket
);

-- FT-008: Bucket total count equals overall total count
-- Validates buckets partition the full dataset with no gaps or overlaps
SELECT
    'FT-008: Bucket Partition Completeness' AS TEST_ID,
    CASE WHEN bucket_total = overall_total THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    ABS(bucket_total - overall_total) AS FAILED_ROWS,
    'Sum of per-bucket counts (' || bucket_total || ') != total rows (' || overall_total || ')' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'overall_total', overall_total,
        'bucket_total',  bucket_total,
        'delta',         bucket_total - overall_total
    ) AS SAMPLE_RECORDS
FROM (
    SELECT
        (SELECT COUNT(*) FROM IDENTIFIER($report_table))                              AS overall_total,
        (SELECT COUNT(*) FROM IDENTIFIER($report_table) WHERE process_time_bucket IS NOT NULL) AS bucket_total
);

-- =====================================================================
-- CATEGORY 5: REFERENTIAL INTEGRITY
-- =====================================================================

-- FT-009: All payment_request_no values exist in source base model
-- Detects phantom records that were created during transformation
SELECT
    'FT-009: Orphaned Payment Requests' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'payment_request_no in detail model does not exist in base model' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', t.payment_request_no
    )) WITHIN GROUP (ORDER BY t.payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table) t
WHERE NOT EXISTS (
    SELECT 1
    FROM IDENTIFIER($source_table) s
    WHERE s.payment_request_no = t.payment_request_no
);

-- FT-010: No records missing from target vs source (dropped claims)
-- Detects source records that were silently filtered out
SELECT
    'FT-010: Dropped Payment Requests' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Paid claim in base model is absent from the detail model' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', s.payment_request_no,
        'payment_dt',         s.payment_dt
    )) WITHIN GROUP (ORDER BY s.payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($source_table) s
WHERE s.carrier_name = $carrier_name
  AND s.payment_dt BETWEEN TO_DATE($report_start) AND TO_DATE($report_end)
  AND UPPER(TRIM(s.paid_flag)) = 'PAID'
  AND NOT EXISTS (
        SELECT 1
        FROM IDENTIFIER($report_table) t
        WHERE t.payment_request_no = s.payment_request_no
  );

-- =====================================================================
-- CATEGORY 6: SLA AND STATISTICAL METRICS
-- =====================================================================

-- FT-011: SLA compliance rate reporting
-- Informational: calculates the % of claims paid within the defined SLA
-- threshold (adjust threshold_days to match business SLA)
SELECT
    'FT-011: SLA Compliance Rate' AS TEST_ID,
    CASE
        WHEN compliance_rate < 50 THEN 'WARNING'
        ELSE 'INFO'
    END AS STATUS,
    ROUND(100 - compliance_rate, 2) AS NON_COMPLIANT_PCT,
    ROUND(compliance_rate, 2) || '% of claims paid within SLA threshold' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'sla_threshold_days',    threshold_days,
        'total_paid_claims',     total_claims,
        'within_sla',            within_sla,
        'compliance_rate_pct',   ROUND(compliance_rate, 2),
        'avg_process_time_days', ROUND(avg_days, 2),
        'median_days',           median_days
    ) AS SAMPLE_RECORDS
FROM (
    SELECT
        5                                                                              AS threshold_days,  -- adjust to SLA
        COUNT(*)                                                                       AS total_claims,
        COUNT(*) FILTER (WHERE process_time_days <= 5)                                AS within_sla,
        COUNT(*) FILTER (WHERE process_time_days <= 5) * 100.0 / NULLIF(COUNT(*), 0) AS compliance_rate,
        AVG(process_time_days)                                                        AS avg_days,
        MEDIAN(process_time_days)                                                     AS median_days
    FROM IDENTIFIER($report_table)
    WHERE UPPER(TRIM(paid_flag)) = 'PAID'
);

-- FT-012: Statistical outlier detection (>3 standard deviations)
-- Claims with extreme process times warrant investigation
SELECT
    'FT-012: Process Time Statistical Outliers' AS TEST_ID,
    'INFO' AS STATUS,
    outlier_count AS OUTLIER_COUNT,
    outlier_count || ' claims exceed 3 standard deviations from mean process time' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'mean_process_days',   ROUND(mean_days, 2),
        'stddev_process_days', ROUND(stddev_days, 2),
        'outlier_threshold',   ROUND(mean_days + (3 * stddev_days), 2),
        'max_process_days',    max_days,
        'outlier_count',       outlier_count
    ) AS SAMPLE_RECORDS
FROM (
    SELECT
        AVG(process_time_days)    AS mean_days,
        STDDEV(process_time_days) AS stddev_days,
        MAX(process_time_days)    AS max_days,
        COUNT(*) FILTER (
            WHERE process_time_days > (
                AVG(process_time_days) OVER () + 3 * STDDEV(process_time_days) OVER ()
            )
        )                          AS outlier_count
    FROM IDENTIFIER($report_table)
    WHERE process_time_days IS NOT NULL
);

-- FT-013: Paid amount total is non-zero for the period
-- Guarantees the report has meaningful financial content
SELECT
    'FT-013: Non-Zero Total Paid Amount' AS TEST_ID,
    CASE
        WHEN SUM(paid_amount) > 0 THEN 'PASS'
        WHEN SUM(paid_amount) = 0 THEN 'WARNING'
        ELSE 'FAIL'
    END AS STATUS,
    0 AS FAILED_ROWS,
    'Total paid amount for period: $' || TO_VARCHAR(ROUND(SUM(paid_amount), 2)) AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'total_paid_amount',  ROUND(SUM(paid_amount), 2),
        'avg_paid_per_claim', ROUND(AVG(paid_amount), 2),
        'min_paid',           MIN(paid_amount),
        'max_paid',           MAX(paid_amount)
    ) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE UPPER(TRIM(paid_flag)) = 'PAID';

-- =====================================================================
-- CATEGORY 7: ROW COUNT REASONABLENESS
-- =====================================================================

-- FT-014: Report is not empty
SELECT
    'FT-014: Row Count Reasonableness' AS TEST_ID,
    CASE
        WHEN record_count = 0  THEN 'FAIL'
        WHEN record_count < 10 THEN 'WARNING'
        ELSE 'PASS'
    END AS STATUS,
    record_count AS FAILED_ROWS,
    'Report returned ' || record_count || ' rows. Expected > 10 for a typical reporting period.' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'total_records',        record_count,
        'distinct_policies',    distinct_policies,
        'distinct_buckets',     distinct_buckets,
        'report_start',         $report_start,
        'report_end',           $report_end
    ) AS SAMPLE_RECORDS
FROM (
    SELECT
        COUNT(*)                         AS record_count,
        COUNT(DISTINCT policy_no)        AS distinct_policies,
        COUNT(DISTINCT process_time_bucket) AS distinct_buckets
    FROM IDENTIFIER($report_table)
);

-- FT-015: Unpaid claims do NOT appear in the paid process time detail
-- If this model is scoped to paid claims only, no UNPAID records should exist
SELECT
    'FT-015: No Unpaid Records in Paid Detail Model' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'UNPAID records found in a model scoped to PAID claims — review filter logic' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'paid_flag',          paid_flag,
        'payment_dt',         payment_dt
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE UPPER(TRIM(paid_flag)) = 'UNPAID';

-- =====================================================================
-- CATEGORY 8: DATA QUALITY SPOT CHECKS (NULL / FORMAT)
-- =====================================================================

-- FT-016: Critical fields are never NULL
SELECT
    'FT-016: NULL Critical Fields' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'One or more critical fields are NULL — will break downstream reporting' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'policy_no',          policy_no,
        'carrier_name',       carrier_name,
        'paid_flag',          paid_flag,
        'process_time_bucket', process_time_bucket
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE payment_request_no IS NULL
   OR policy_no          IS NULL
   OR carrier_name       IS NULL
   OR paid_flag          IS NULL
   OR process_time_bucket IS NULL;

-- FT-017: insurance_group is populated for all records
SELECT
    'FT-017: Missing Insurance Group' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'insurance_group is required for group-level aggregation' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'payment_request_no', payment_request_no,
        'policy_no',          policy_no
    )) WITHIN GROUP (ORDER BY payment_request_no LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE insurance_group IS NULL OR TRIM(insurance_group) = '';

-- =====================================================================
-- FINAL SUMMARY: Functional Test Results Rollup
-- =====================================================================
SELECT
    '=== FUNCTIONAL TEST SUMMARY ===' AS TEST_SUITE,
    'Review all FT-* results above' AS INSTRUCTIONS,
    'FAIL status tests must be resolved before production promotion' AS CRITICAL_ACTION,
    'WARNING status tests should be investigated and documented' AS RECOMMENDED_ACTION,
    'INFO status tests are diagnostic and informational' AS INFORMATIONAL_NOTE;
