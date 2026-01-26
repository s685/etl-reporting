-- =====================================================================
-- DATA QUALITY TEST SUITE
-- new_rfb_and_total_claimants_active_detail Report
-- =====================================================================
-- Purpose: Validate data quality before production use
-- Run after: Report execution completes
-- Target: {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
-- =====================================================================

SET report_table = '{{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail';

-- =====================================================================
-- CATEGORY 1: NULL/MISSING DATA CHECKS
-- =====================================================================

-- Test 1.1: Critical fields should never be NULL
SELECT 
    'DQ-001: NULL in Critical Fields' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Critical business keys must be populated' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'status', "Status",
            'decision_date', "EDB Decision Date"
        )
    ) WITHIN GROUP (ORDER BY "Policy Number" LIMIT 5) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE "Policy Number" IS NULL
    OR "Status" IS NULL
    OR "EDB Decision Date" IS NULL
    OR "Statistical Start Date" IS NULL
    OR carrier_name IS NULL;

-- Test 1.2: NULL in calculated fields
SELECT 
    'DQ-002: NULL in Calculated Fields' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'DAYS and NumDaysResolvedWithinTwoWeeks must be calculated' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'days', DAYS,
            'two_week_flag', NumDaysResolvedWithinTwoWeeks
        )
    ) WITHIN GROUP (ORDER BY "Policy Number" LIMIT 5) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE DAYS IS NULL
    OR NumDaysResolvedWithinTwoWeeks IS NULL;

-- Test 1.3: Empty strings in text fields
SELECT 
    'DQ-003: Empty Strings in Fields' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Empty strings should be NULL for consistency' AS BUSINESS_IMPACT,
    ARRAY_AGG("Policy Number") WITHIN GROUP (ORDER BY "Policy Number" LIMIT 5) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE TRIM("Policy Number") = ''
    OR TRIM("Status") = ''
    OR TRIM("Insurance Group") = ''
    OR TRIM(carrier_name) = '';

-- =====================================================================
-- CATEGORY 2: DUPLICATE DETECTION
-- =====================================================================

-- Test 2.1: Duplicate policy numbers with same dates
SELECT 
    'DQ-004: Duplicate Policy Records' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Same policy should not appear twice with same dates - indicates data multiplication' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'count', cnt,
            'status', "Status",
            'decision_date', "EDB Decision Date"
        )
    ) WITHIN GROUP (ORDER BY cnt DESC LIMIT 10) AS SAMPLE_FAILURES
FROM (
    SELECT 
        "Policy Number",
        "Status",
        "EDB Decision Date",
        "Statistical Start Date",
        COUNT(*) AS cnt
    FROM IDENTIFIER($report_table)
    GROUP BY "Policy Number", "Status", "EDB Decision Date", "Statistical Start Date"
    HAVING COUNT(*) > 1
);

-- Test 2.2: Excessive duplicates (same policy many times)
SELECT 
    'DQ-005: Excessive Policy Duplicates' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Policy appears >10 times - severe data multiplication issue' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'count', cnt
        )
    ) WITHIN GROUP (ORDER BY cnt DESC LIMIT 10) AS SAMPLE_FAILURES
FROM (
    SELECT 
        "Policy Number",
        COUNT(*) AS cnt
    FROM IDENTIFIER($report_table)
    GROUP BY "Policy Number"
    HAVING COUNT(*) > 10
);

-- =====================================================================
-- CATEGORY 3: DATA TYPE & FORMAT VALIDATION
-- =====================================================================

-- Test 3.1: Date format validation (MM/DD/YYYY)
SELECT 
    'DQ-006: Invalid Date Formats' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Dates must be in MM/DD/YYYY format for downstream systems' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'start_date', "Statistical Start Date",
            'decision_date', "EDB Decision Date"
        )
    ) WITHIN GROUP (ORDER BY "Policy Number" LIMIT 5) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE TRY_TO_DATE("Statistical Start Date", 'MM/DD/YYYY') IS NULL
    OR TRY_TO_DATE("EDB Decision Date", 'MM/DD/YYYY') IS NULL;

-- Test 3.2: Numeric fields are valid numbers
SELECT 
    'DQ-007: Invalid Numeric Values' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'DAYS and NumDaysResolvedWithinTwoWeeks must be valid integers' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'days', DAYS,
            'flag', NumDaysResolvedWithinTwoWeeks
        )
    ) WITHIN GROUP (ORDER BY "Policy Number" LIMIT 5) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE TRY_CAST(DAYS AS INTEGER) IS NULL
    OR TRY_CAST(NumDaysResolvedWithinTwoWeeks AS INTEGER) IS NULL;

-- =====================================================================
-- CATEGORY 4: VALUE RANGE VALIDATION
-- =====================================================================

-- Test 4.1: Negative days (impossible)
SELECT 
    'DQ-008: Negative Turnaround Days' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Turnaround time cannot be negative - calculation error' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'days', DAYS,
            'start_date', "Statistical Start Date",
            'decision_date', "EDB Decision Date"
        )
    ) WITHIN GROUP (ORDER BY DAYS LIMIT 5) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE DAYS < 0;

-- Test 4.2: Unrealistic turnaround times (>2 years)
SELECT 
    'DQ-009: Unrealistic Turnaround Times' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Processing took >730 days - may be valid but investigate' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'days', DAYS,
            'status', "Status"
        )
    ) WITHIN GROUP (ORDER BY DAYS DESC LIMIT 10) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE DAYS > 730;

-- Test 4.3: Future dates
SELECT 
    'DQ-010: Future Decision Dates' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Decision dates in future indicate system clock issues' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'decision_date', "EDB Decision Date",
            'days_in_future', DATEDIFF(DAY, CURRENT_DATE(), TRY_TO_DATE("EDB Decision Date", 'MM/DD/YYYY'))
        )
    ) WITHIN GROUP (ORDER BY "EDB Decision Date" DESC LIMIT 5) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE TRY_TO_DATE("EDB Decision Date", 'MM/DD/YYYY') > CURRENT_DATE();

-- Test 4.4: Decision before start date (impossible)
SELECT 
    'DQ-011: Decision Before Start Date' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Decision cannot happen before RFB start - data corruption' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'start_date', "Statistical Start Date",
            'decision_date', "EDB Decision Date",
            'days', DAYS
        )
    ) WITHIN GROUP (ORDER BY "Policy Number" LIMIT 5) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE TRY_TO_DATE("Statistical Start Date", 'MM/DD/YYYY') > 
      TRY_TO_DATE("EDB Decision Date", 'MM/DD/YYYY');

-- Test 4.5: TwoWeek flag consistency
SELECT 
    'DQ-012: TwoWeek Flag Inconsistency' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'NumDaysResolvedWithinTwoWeeks flag does not match DAYS value' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'days', DAYS,
            'flag', NumDaysResolvedWithinTwoWeeks,
            'expected_flag', CASE WHEN DAYS < 14 THEN 1 ELSE 0 END
        )
    ) WITHIN GROUP (ORDER BY DAYS LIMIT 10) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE (NumDaysResolvedWithinTwoWeeks = 1 AND DAYS >= 14)
    OR (NumDaysResolvedWithinTwoWeeks = 0 AND DAYS < 14);

-- =====================================================================
-- CATEGORY 5: REFERENTIAL INTEGRITY
-- =====================================================================

-- Test 5.1: Policies exist in source system
SELECT 
    'DQ-013: Orphaned Policies' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Policy numbers in report do not exist in source policy table' AS BUSINESS_IMPACT,
    ARRAY_AGG("Policy Number") WITHIN GROUP (ORDER BY "Policy Number" LIMIT 10) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table) r
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{SOURCE_DATABASE}}.dbo.policy p
    WHERE TRIM(p.policy_no) = TRIM(r."Policy Number")
);

-- Test 5.2: Status codes are valid
SELECT 
    'DQ-014: Invalid Status Codes' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Status values do not match eb_status lookup table' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'status', "Status",
            'status_cd', status_cd
        )
    ) WITHIN GROUP (ORDER BY "Status" LIMIT 10) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table) r
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{SOURCE_DATABASE}}.dbo.eb_status es
    WHERE es.eb_status_cd = r.status_cd
);

-- =====================================================================
-- CATEGORY 6: COMPLETENESS CHECKS
-- =====================================================================

-- Test 6.1: Row count reasonability
SELECT 
    'DQ-015: Row Count Validation' AS TEST_ID,
    CASE 
        WHEN record_count = 0 THEN 'FAIL'
        WHEN record_count < 10 THEN 'WARNING'
        ELSE 'PASS'
    END AS STATUS,
    record_count AS FAILED_ROWS,
    'Report has ' || record_count || ' rows. Expected >10 for typical report period.' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'total_rows', record_count,
        'unique_policies', unique_policies,
        'unique_statuses', unique_statuses
    ) AS SAMPLE_FAILURES
FROM (
    SELECT 
        COUNT(*) AS record_count,
        COUNT(DISTINCT "Policy Number") AS unique_policies,
        COUNT(DISTINCT "Status") AS unique_statuses
    FROM IDENTIFIER($report_table)
);

-- Test 6.2: Carrier name consistency
SELECT 
    'DQ-016: Multiple Carrier Names' AS TEST_ID,
    CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) - 1 AS FAILED_ROWS,
    'Report should have single carrier_name from session variable' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'carrier_name', carrier_name,
            'count', cnt
        )
    ) WITHIN GROUP (ORDER BY cnt DESC) AS SAMPLE_FAILURES
FROM (
    SELECT 
        carrier_name,
        COUNT(*) AS cnt
    FROM IDENTIFIER($report_table)
    GROUP BY carrier_name
);

-- Test 6.3: All required states populated
SELECT 
    'DQ-017: Missing State Data' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Geographic data (states) missing - impacts regional analysis' AS BUSINESS_IMPACT,
    ARRAY_AGG("Policy Number") WITHIN GROUP (ORDER BY "Policy Number" LIMIT 10) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE "Residence State" IS NULL
    OR "Issue State" IS NULL
    OR TRIM("Residence State") = ''
    OR TRIM("Issue State") = '';

-- =====================================================================
-- CATEGORY 7: CONSISTENCY CHECKS
-- =====================================================================

-- Test 7.1: Modified By has consistent format
SELECT 
    'DQ-018: Modified By Format' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Modified By contains domain prefix (DOMAIN\user) - should be stripped' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'modified_by', "Modified By"
        )
    ) WITHIN GROUP (ORDER BY "Policy Number" LIMIT 10) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE "Modified By" LIKE '%\\%';

-- Test 7.2: Insurance Group populated
SELECT 
    'DQ-019: Missing Insurance Group' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Insurance Group is required for grouping and analysis' AS BUSINESS_IMPACT,
    ARRAY_AGG("Policy Number") WITHIN GROUP (ORDER BY "Policy Number" LIMIT 10) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE "Insurance Group" IS NULL
    OR TRIM("Insurance Group") = '';

-- Test 7.3: Date consistency across report
SELECT 
    'DQ-020: Dates Within Report Period' AS TEST_ID,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    COUNT(*) AS FAILED_ROWS,
    'Decision dates should be within report period [$REPORT_START_DT to $REPORT_END_DT]' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'policy_no', "Policy Number",
            'decision_date', "EDB Decision Date"
        )
    ) WITHIN GROUP (ORDER BY "EDB Decision Date" LIMIT 10) AS SAMPLE_FAILURES
FROM IDENTIFIER($report_table)
WHERE TRY_TO_DATE("EDB Decision Date", 'MM/DD/YYYY') NOT BETWEEN $REPORT_START_DT AND $REPORT_END_DT;

-- =====================================================================
-- CATEGORY 8: STATISTICAL ANALYSIS
-- =====================================================================

-- Test 8.1: Outlier detection (extreme values)
SELECT 
    'DQ-021: Statistical Outliers' AS TEST_ID,
    'INFO' AS STATUS,
    outlier_count AS FAILED_ROWS,
    'Records with DAYS > 3 standard deviations from mean - investigate' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'outlier_count', outlier_count,
        'mean_days', ROUND(mean_days, 2),
        'stddev_days', ROUND(stddev_days, 2),
        'max_days', max_days,
        'threshold', ROUND(mean_days + (3 * stddev_days), 2)
    ) AS SAMPLE_FAILURES
FROM (
    SELECT 
        AVG(DAYS) AS mean_days,
        STDDEV(DAYS) AS stddev_days,
        MAX(DAYS) AS max_days,
        COUNT(*) FILTER (WHERE DAYS > (AVG(DAYS) OVER () + (3 * STDDEV(DAYS) OVER ()))) AS outlier_count
    FROM IDENTIFIER($report_table)
);

-- Test 8.2: SLA compliance rate
SELECT 
    'DQ-022: SLA Compliance Rate' AS TEST_ID,
    CASE 
        WHEN compliance_rate < 50 THEN 'WARNING'
        ELSE 'INFO'
    END AS STATUS,
    ROUND(100 - compliance_rate, 2) AS FAILED_ROWS,
    ROUND(compliance_rate, 2) || '% resolved within 14 days' AS BUSINESS_IMPACT,
    OBJECT_CONSTRUCT(
        'total_claims', total_claims,
        'resolved_within_14_days', resolved_count,
        'compliance_rate_pct', ROUND(compliance_rate, 2),
        'avg_days_all', ROUND(avg_days, 2),
        'avg_days_compliant', ROUND(avg_days_compliant, 2),
        'avg_days_non_compliant', ROUND(avg_days_non_compliant, 2)
    ) AS SAMPLE_FAILURES
FROM (
    SELECT 
        COUNT(*) AS total_claims,
        SUM(NumDaysResolvedWithinTwoWeeks) AS resolved_count,
        (SUM(NumDaysResolvedWithinTwoWeeks) * 100.0 / NULLIF(COUNT(*), 0)) AS compliance_rate,
        AVG(DAYS) AS avg_days,
        AVG(DAYS) FILTER (WHERE NumDaysResolvedWithinTwoWeeks = 1) AS avg_days_compliant,
        AVG(DAYS) FILTER (WHERE NumDaysResolvedWithinTwoWeeks = 0) AS avg_days_non_compliant
    FROM IDENTIFIER($report_table)
);

-- Test 8.3: Status distribution analysis
SELECT 
    'DQ-023: Status Distribution' AS TEST_ID,
    'INFO' AS STATUS,
    NULL AS FAILED_ROWS,
    'Distribution of claim statuses - verify expected patterns' AS BUSINESS_IMPACT,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'status', "Status",
            'count', cnt,
            'percentage', ROUND(percentage, 2)
        )
    ) WITHIN GROUP (ORDER BY cnt DESC) AS SAMPLE_FAILURES
FROM (
    SELECT 
        "Status",
        COUNT(*) AS cnt,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
    FROM IDENTIFIER($report_table)
    GROUP BY "Status"
);

-- =====================================================================
-- FINAL SUMMARY
-- =====================================================================
-- Note: This is a conceptual summary. In practice, you'd need to 
-- capture all test results in a temp table and then summarize.
SELECT 
    '=== DATA QUALITY TEST SUITE COMPLETE ===' AS MESSAGE,
    'Review all test results above' AS INSTRUCTIONS,
    'Tests with FAIL status must be addressed before production' AS CRITICAL_ACTION,
    'Tests with WARNING status should be investigated' AS RECOMMENDED_ACTION,
    'Tests with INFO status are informational only' AS INFORMATIONAL;
