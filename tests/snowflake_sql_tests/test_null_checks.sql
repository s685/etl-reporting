-- =====================================================================
-- NULL/MISSING DATA VALIDATION TESTS
-- =====================================================================
-- Tests for: new_rfb_and_total_claimants_active_detail report
-- Purpose: Validate required fields are populated and data is complete
-- =====================================================================

-- Test 1: Check for NULL policy_no (CRITICAL - Required field)
SELECT 
    'Test 1: NULL policy_no Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'Policy Number is a required field and should never be NULL' AS DETAILS,
    ARRAY_AGG(OBJECT_CONSTRUCT('series_id', series_id, 'life_id', life_id, 'rfb_id', rfb_id)) 
        WITHIN GROUP (ORDER BY series_id LIMIT 10) AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE policy_no IS NULL;

-- Test 2: Check for NULL or empty carrier_name
SELECT 
    'Test 2: NULL/Empty carrier_name Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'Carrier name should be populated for all records' AS DETAILS,
    ARRAY_AGG(OBJECT_CONSTRUCT('policy_no', policy_no, 'series_id', series_id)) 
        WITHIN GROUP (ORDER BY policy_no LIMIT 10) AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE carrier_name IS NULL OR TRIM(carrier_name) = '';

-- Test 3: Check for NULL RFB_ID (CRITICAL)
SELECT 
    'Test 3: NULL rfb_id Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'RFB ID is required for all records in this report' AS DETAILS,
    ARRAY_AGG(OBJECT_CONSTRUCT('policy_no', policy_no, 'series_id', series_id, 'life_id', life_id)) 
        WITHIN GROUP (ORDER BY policy_no LIMIT 10) AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE rfb_id IS NULL;

-- Test 4: Check for NULL episode_of_benefit_id
SELECT 
    'Test 4: NULL episode_of_benefit_id Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'Episode of Benefit ID should exist for all RFB records' AS DETAILS,
    ARRAY_AGG(OBJECT_CONSTRUCT('policy_no', policy_no, 'rfb_id', rfb_id, 'series_id', series_id)) 
        WITHIN GROUP (ORDER BY rfb_id LIMIT 10) AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE episode_of_benefit_id IS NULL;

-- Test 5: Check for NULL rfb_statistical_start_dt (CRITICAL)
SELECT 
    'Test 5: NULL rfb_statistical_start_dt Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'RFB Statistical Start Date is required for calculations' AS DETAILS,
    ARRAY_AGG(OBJECT_CONSTRUCT('policy_no', policy_no, 'rfb_id', rfb_id, 'series_id', series_id)) 
        WITHIN GROUP (ORDER BY policy_no LIMIT 10) AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE rfb_statistical_start_dt IS NULL;

-- Test 6: Check for NULL FIRSTEDBDECISIONDT (CRITICAL)
SELECT 
    'Test 6: NULL FIRSTEDBDECISIONDT Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'First EDB Decision Date is required for the report period logic' AS DETAILS,
    ARRAY_AGG(OBJECT_CONSTRUCT('policy_no', policy_no, 'rfb_id', rfb_id, 'eob_id', episode_of_benefit_id)) 
        WITHIN GROUP (ORDER BY policy_no LIMIT 10) AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE FIRSTEDBDECISIONDT IS NULL;

-- Test 7: Check for NULL life_id
SELECT 
    'Test 7: NULL life_id Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'Life ID is required to link to demographics' AS DETAILS,
    ARRAY_AGG(OBJECT_CONSTRUCT('policy_no', policy_no, 'series_id', series_id, 'rfb_id', rfb_id)) 
        WITHIN GROUP (ORDER BY policy_no LIMIT 10) AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE life_id IS NULL;

-- Test 8: Check for NULL status_cd (Status Code)
SELECT 
    'Test 8: NULL status_cd Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'Status code should be populated for all EOB records' AS DETAILS,
    ARRAY_AGG(OBJECT_CONSTRUCT('policy_no', policy_no, 'rfb_id', rfb_id, 'eob_id', episode_of_benefit_id)) 
        WITHIN GROUP (ORDER BY policy_no LIMIT 10) AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE status_cd IS NULL;

-- Test 9: Check for records with all NULLs (corrupt data)
SELECT 
    'Test 9: Completely NULL Record Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'Found records with all NULL values - potential data corruption' AS DETAILS,
    NULL AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE policy_no IS NULL 
    AND series_id IS NULL 
    AND life_id IS NULL 
    AND rfb_id IS NULL 
    AND episode_of_benefit_id IS NULL;

-- Test 10: Check for NULL in calculated fields (WORK_DAYS_BETWEEN)
SELECT 
    'Test 10: NULL WORK_DAYS_BETWEEN Check' AS TEST_NAME,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS,
    COUNT(*) AS ISSUE_COUNT,
    'WORK_DAYS_BETWEEN should be calculated for all records' AS DETAILS,
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_no', policy_no, 
        'rfb_id', rfb_id, 
        'rfb_start', rfb_statistical_start_dt,
        'decision_dt', FIRSTEDBDECISIONDT
    )) WITHIN GROUP (ORDER BY policy_no LIMIT 10) AS SAMPLE_RECORDS
FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail
WHERE WORK_DAYS_BETWEEN IS NULL;

-- =====================================================================
-- SUMMARY: Count total failed tests
-- =====================================================================
SELECT 
    '=== NULL CHECKS SUMMARY ===' AS TEST_SUITE,
    SUM(CASE WHEN TEST_STATUS = 'FAIL' THEN 1 ELSE 0 END) AS TOTAL_FAILED_TESTS,
    SUM(CASE WHEN TEST_STATUS = 'PASS' THEN 1 ELSE 0 END) AS TOTAL_PASSED_TESTS,
    SUM(ISSUE_COUNT) AS TOTAL_ISSUES_FOUND,
    CASE 
        WHEN SUM(CASE WHEN TEST_STATUS = 'FAIL' THEN 1 ELSE 0 END) = 0 
        THEN '✓ ALL NULL CHECKS PASSED' 
        ELSE '✗ CRITICAL: NULL VALUE ISSUES DETECTED' 
    END AS OVERALL_STATUS
FROM (
    -- Re-run all tests and collect results
    SELECT 'NULL policy_no' AS TEST_NAME, CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS TEST_STATUS, COUNT(*) AS ISSUE_COUNT
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail WHERE policy_no IS NULL
    
    UNION ALL
    
    SELECT 'NULL carrier_name', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*)
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail 
    WHERE carrier_name IS NULL OR TRIM(carrier_name) = ''
    
    UNION ALL
    
    SELECT 'NULL rfb_id', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*)
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail WHERE rfb_id IS NULL
    
    UNION ALL
    
    SELECT 'NULL episode_of_benefit_id', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*)
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail WHERE episode_of_benefit_id IS NULL
    
    UNION ALL
    
    SELECT 'NULL rfb_statistical_start_dt', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*)
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail WHERE rfb_statistical_start_dt IS NULL
    
    UNION ALL
    
    SELECT 'NULL FIRSTEDBDECISIONDT', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*)
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail WHERE FIRSTEDBDECISIONDT IS NULL
    
    UNION ALL
    
    SELECT 'NULL life_id', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*)
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail WHERE life_id IS NULL
    
    UNION ALL
    
    SELECT 'NULL status_cd', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*)
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail WHERE status_cd IS NULL
    
    UNION ALL
    
    SELECT 'Completely NULL records', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*)
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail 
    WHERE policy_no IS NULL AND series_id IS NULL AND life_id IS NULL AND rfb_id IS NULL
    
    UNION ALL
    
    SELECT 'NULL WORK_DAYS_BETWEEN', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*)
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.new_rfb_and_total_claimants_active_detail WHERE WORK_DAYS_BETWEEN IS NULL
);
