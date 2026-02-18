-- =====================================================================
-- FUNCTIONAL TEST SUITE: claims_paid_by_process_time_detail
-- =====================================================================
-- Purpose  : Validate end-to-end business logic, data completeness,
--            referential integrity, and statistical reasonableness
-- Model    : claims_paid_by_process_time_detail
-- Focus    : Full pipeline correctness — source-to-target reconciliation,
--            bucket distribution, decision logic, and report period scoping
-- Run on   : {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claims_paid_by_process_time_detail
-- Depends  : Base model {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claims_paid_activity_base
-- =====================================================================
-- Column Reference (final output):
--   "Policy #"             — POLICY_NO
--   CARRIER_NAME           — carrier name (from session variable)
--   REPORT_DT              — $report_end_dt session variable
--   "Payreq Complete Date" — to_char(PAYREQ_COMPLETE_DT, 'MM/DD/YYYY')
--   "Group"                — process time bucket based on PROCESS_DAYS
--                            (working days between PAYREQ_COMPLETE_DT and MIN_DECISION_DATE)
--                            Values: PROCESSED_0_to_5_DAYS, PROCESSED_6_to_10_DAYS,
--                                    PROCESSED_11_to_20_DAYS, PROCESSED_over_20_DAYS
--   "XOB Date"             — to_char(next working day after MIN_DECISION_DATE, 'MM/DD/YYYY')
--   "No of Days"           — working days between PAYREQ_COMPLETE_DT and MIN_SD_EARLIEST_REPORT_DT
--   "Group Name"           — TOP_LEVEL_GRP_NM
--   "User ID"              — USER_ID
--   "Doc #"                — DOC_ID
--   "Decision"             — PAID or UNPAID (derived from SD_OK_TO_PAY_FLG min/max)
--   "Reason"               — SD_OK_REASON_DESC when MIN_SD_OK_REASON_CD <> 1, else ''
-- =====================================================================

SET report_table    = '{{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claims_paid_by_process_time_detail';
SET base_table      = '{{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.claims_paid_activity_base';
SET carrier_name    = '{{CARRIER_NAME}}';
SET report_start_dt = '{{REPORT_START_DT}}';
SET report_end_dt   = '{{REPORT_END_DT}}';

-- =====================================================================
-- CATEGORY 1: SOURCE-TO-TARGET RECONCILIATION
-- =====================================================================

-- FT-001: Target record count must reconcile with the base model
-- The detail model selects one row per PAYMENT_REQUEST_ID from the base
-- where SD_REPORTED_FLG = 1 and MIN_SD_EARLIEST_REPORT_DT is within the reporting period.
-- NOTE: Exact count match depends on additional joins (calendar, sd_ok_reason) and
-- the MIN_DECISION_DATE_RANK = 1 filter. This test compares distinct payment request
-- IDs in the eligible base population against the target row count.
WITH
source_count AS (
    SELECT COUNT(DISTINCT PAYMENT_REQUEST_ID) AS cnt
    FROM IDENTIFIER($base_table)
    WHERE SD_REPORTED_FLG = 1
      AND SD_EARLIEST_REPORT_DT <= TO_DATE($report_end_dt)
      AND SD_EARLIEST_REPORT_DT >= TO_DATE($report_start_dt)
),
target_count AS (
    SELECT COUNT(*) AS cnt
    FROM IDENTIFIER($report_table)
)
SELECT
    'FT-001: Source vs Target Record Count' AS TEST_ID,
    CASE WHEN s.cnt = t.cnt THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    ABS(s.cnt - t.cnt) AS FAILED_ROWS,
    'Source distinct PAYMENT_REQUEST_ID (' || s.cnt || ') vs target rows (' || t.cnt || ')' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'source_distinct_payment_request_ids', s.cnt,
        'target_row_count',                    t.cnt,
        'delta',                               s.cnt - t.cnt
    ) AS SAMPLE_RECORDS
FROM source_count s
CROSS JOIN target_count t;

-- FT-002: All Policy # values in target exist in the base model
-- Detects phantom records created during transformation
SELECT
    'FT-002: Orphaned Policy Numbers in Target' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Policy #" in detail model does not exist in base model — referential integrity violation' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_no', t."Policy #",
        'doc_id',    t."Doc #"
    )) WITHIN GROUP (ORDER BY t."Policy #" LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table) t
WHERE NOT EXISTS (
    SELECT 1
    FROM IDENTIFIER($base_table) s
    WHERE s.POLICY_NO = t."Policy #"
);

-- =====================================================================
-- CATEGORY 2: REPORT PERIOD SCOPING
-- =====================================================================

-- FT-003: REPORT_DT must match the session variable $report_end_dt
-- Validates the report date is correctly propagated from the session variable
SELECT
    'FT-003: Report Date Matches Session Variable' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'REPORT_DT does not match $report_end_dt — session variable not applied correctly' AS BUSINESS_IMPACT,
    ARRAY_AGG(DISTINCT OBJECT_CONSTRUCT(
        'report_dt_found', REPORT_DT
    )) WITHIN GROUP (ORDER BY REPORT_DT LIMIT 5) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE REPORT_DT <> TO_DATE($report_end_dt);

-- FT-004: No Payreq Complete Dates in the future
-- Claims should not have completion dates beyond the report end date
SELECT
    'FT-004: No Future Payreq Complete Dates' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Payreq Complete Date" is after report_end_dt — possible data integrity issue' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_no',          "Policy #",
        'payreq_complete_dt', "Payreq Complete Date"
    )) WITHIN GROUP (ORDER BY "Payreq Complete Date" DESC LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE TRY_TO_DATE("Payreq Complete Date", 'MM/DD/YYYY') > TO_DATE($report_end_dt);

-- =====================================================================
-- CATEGORY 3: CARRIER FILTERING
-- =====================================================================

-- FT-005: Single carrier per report run
-- Report is parameterized by carrier; mixing carriers indicates filter failure
SELECT
    'FT-005: Single Carrier Consistency' AS TEST_ID,
    CASE WHEN COUNT(DISTINCT CARRIER_NAME) <= 1 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(DISTINCT CARRIER_NAME) - 1 AS EXTRA_CARRIERS,
    'More than one carrier found — carrier_name session variable not applied' AS BUSINESS_IMPACT,
    ARRAY_AGG(DISTINCT CARRIER_NAME) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table);

-- FT-006: Carrier name matches the session variable value
SELECT
    'FT-006: Carrier Matches Session Variable' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Records found with CARRIER_NAME != $carrier_name' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_no',    "Policy #",
        'carrier_name', CARRIER_NAME
    )) WITHIN GROUP (ORDER BY "Policy #" LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE CARRIER_NAME <> $carrier_name;

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
                'bucket',       "Group",
                'record_count', record_count,
                'pct_of_total', ROUND(pct_of_total, 2)
            )
        )
    ) AS SAMPLE_RECORDS
FROM (
    SELECT
        "Group",
        COUNT(*)                                        AS record_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()       AS pct_of_total
    FROM IDENTIFIER($report_table)
    GROUP BY "Group"
);

-- FT-008: Bucket total count equals overall total count
-- Validates buckets partition the full dataset with no gaps
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
        (SELECT COUNT(*) FROM IDENTIFIER($report_table))                                AS overall_total,
        (SELECT COUNT(*) FROM IDENTIFIER($report_table) WHERE "Group" IS NOT NULL)      AS bucket_total
);

-- =====================================================================
-- CATEGORY 5: DECISION LOGIC VALIDATION
-- =====================================================================

-- FT-009: Decision field only contains PAID or UNPAID
-- The payment_request_paid_unpaid CTE derives Decision from SD_OK_TO_PAY_FLG
-- window min/max. Only two outcomes are valid.
SELECT
    'FT-009: Decision Domain Validation' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Decision" contains unexpected values outside of {PAID, UNPAID}' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_no', "Policy #",
        'decision',  "Decision"
    )) WITHIN GROUP (ORDER BY "Policy #" LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE UPPER(TRIM("Decision")) NOT IN ('PAID', 'UNPAID');

-- FT-010: Decision distribution — informational breakdown
SELECT
    'FT-010: Decision Distribution' AS TEST_ID,
    'INFO' AS STATUS,
    0 AS FAILED_ROWS,
    'Paid vs Unpaid breakdown for the reporting period' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'decision_distribution', ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'decision',     "Decision",
                'record_count', cnt,
                'pct_of_total', ROUND(cnt * 100.0 / SUM(cnt) OVER (), 2)
            )
        )
    ) AS SAMPLE_RECORDS
FROM (
    SELECT "Decision", COUNT(*) AS cnt
    FROM IDENTIFIER($report_table)
    GROUP BY "Decision"
);

-- =====================================================================
-- CATEGORY 6: SLA AND STATISTICAL METRICS
-- =====================================================================

-- FT-011: SLA compliance rate — claims processed within 5 working days
-- Uses the "Group" bucket PROCESSED_0_to_5_DAYS as the SLA threshold
SELECT
    'FT-011: SLA Compliance Rate' AS TEST_ID,
    CASE
        WHEN compliance_rate < 50 THEN 'WARNING'
        ELSE 'INFO'
    END AS STATUS,
    ROUND(100 - compliance_rate, 2) AS NON_COMPLIANT_PCT,
    ROUND(compliance_rate, 2) || '% of claims processed within 5 working days (SLA)' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'total_claims',        total_claims,
        'within_sla',          within_sla,
        'compliance_rate_pct', ROUND(compliance_rate, 2)
    ) AS SAMPLE_RECORDS
FROM (
    SELECT
        COUNT(*)                                                                                     AS total_claims,
        COUNT(*) FILTER (WHERE "Group" = 'PROCESSED_0_to_5_DAYS')                                   AS within_sla,
        COUNT(*) FILTER (WHERE "Group" = 'PROCESSED_0_to_5_DAYS') * 100.0 / NULLIF(COUNT(*), 0)     AS compliance_rate
    FROM IDENTIFIER($report_table)
);

-- FT-012: Statistical outlier detection on "No of Days"
-- Claims with extreme working-day counts warrant investigation
SELECT
    'FT-012: No of Days Statistical Outliers' AS TEST_ID,
    'INFO' AS STATUS,
    outlier_count AS OUTLIER_COUNT,
    outlier_count || ' claims exceed 3 standard deviations from mean No of Days' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'mean_days',           ROUND(mean_days, 2),
        'stddev_days',         ROUND(stddev_days, 2),
        'outlier_threshold',   ROUND(mean_days + (3 * stddev_days), 2),
        'max_days',            max_days,
        'outlier_count',       outlier_count
    ) AS SAMPLE_RECORDS
FROM (
    SELECT
        AVG("No of Days")    AS mean_days,
        STDDEV("No of Days") AS stddev_days,
        MAX("No of Days")    AS max_days,
        COUNT(*) FILTER (
            WHERE "No of Days" > (
                AVG("No of Days") OVER () + 3 * STDDEV("No of Days") OVER ()
            )
        )                     AS outlier_count
    FROM IDENTIFIER($report_table)
    WHERE "No of Days" IS NOT NULL
);

-- =====================================================================
-- CATEGORY 7: ROW COUNT REASONABLENESS
-- =====================================================================

-- FT-013: Report is not empty
SELECT
    'FT-013: Row Count Reasonableness' AS TEST_ID,
    CASE
        WHEN record_count = 0  THEN 'FAIL'
        WHEN record_count < 10 THEN 'WARNING'
        ELSE 'PASS'
    END AS STATUS,
    record_count AS FAILED_ROWS,
    'Report returned ' || record_count || ' rows. Expected > 10 for a typical reporting period.' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'total_records',     record_count,
        'distinct_policies', distinct_policies,
        'distinct_buckets',  distinct_buckets,
        'report_start',      $report_start_dt,
        'report_end',        $report_end_dt
    ) AS SAMPLE_RECORDS
FROM (
    SELECT
        COUNT(*)                      AS record_count,
        COUNT(DISTINCT "Policy #")    AS distinct_policies,
        COUNT(DISTINCT "Group")       AS distinct_buckets
    FROM IDENTIFIER($report_table)
);

-- =====================================================================
-- CATEGORY 8: DATA QUALITY SPOT CHECKS (NULL / FORMAT)
-- =====================================================================

-- FT-014: Critical fields are never NULL
SELECT
    'FT-014: NULL Critical Fields' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'One or more critical fields are NULL — will break downstream reporting' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_no', "Policy #",
        'carrier',   CARRIER_NAME,
        'group',     "Group",
        'decision',  "Decision",
        'doc_id',    "Doc #"
    )) WITHIN GROUP (ORDER BY "Policy #" LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE "Policy #"  IS NULL
   OR CARRIER_NAME IS NULL
   OR "Group"      IS NULL
   OR "Decision"   IS NULL;

-- FT-015: "Group Name" is populated for all records
SELECT
    'FT-015: Missing Group Name' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Group Name" is required for group-level aggregation in downstream reports' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_no', "Policy #",
        'doc_id',    "Doc #"
    )) WITHIN GROUP (ORDER BY "Policy #" LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE "Group Name" IS NULL OR TRIM("Group Name") = '';

-- FT-016: "Payreq Complete Date" format validation (MM/DD/YYYY)
SELECT
    'FT-016: Payreq Complete Date Format Validation' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"Payreq Complete Date" does not parse as MM/DD/YYYY — format error in to_char' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_no',          "Policy #",
        'payreq_complete_dt', "Payreq Complete Date"
    )) WITHIN GROUP (ORDER BY "Policy #" LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE "Payreq Complete Date" IS NOT NULL
  AND TRY_TO_DATE("Payreq Complete Date", 'MM/DD/YYYY') IS NULL;

-- FT-017: "XOB Date" format validation (MM/DD/YYYY)
SELECT
    'FT-017: XOB Date Format Validation' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    '"XOB Date" does not parse as MM/DD/YYYY — format error in to_char' AS BUSINESS_IMPACT,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_no', "Policy #",
        'xob_date',  "XOB Date"
    )) WITHIN GROUP (ORDER BY "Policy #" LIMIT 10) AS SAMPLE_RECORDS
FROM IDENTIFIER($report_table)
WHERE "XOB Date" IS NOT NULL
  AND TRY_TO_DATE("XOB Date", 'MM/DD/YYYY') IS NULL;

-- =====================================================================
-- FINAL SUMMARY: Functional Test Results Rollup
-- =====================================================================
SELECT
    '=== FUNCTIONAL TEST SUMMARY ===' AS TEST_SUITE,
    'Review all FT-* results above' AS INSTRUCTIONS,
    'FAIL status tests must be resolved before production promotion' AS CRITICAL_ACTION,
    'WARNING status tests should be investigated and documented' AS RECOMMENDED_ACTION,
    'INFO status tests are diagnostic and informational' AS INFORMATIONAL_NOTE;
