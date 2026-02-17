-- ============================================================
-- TEST FILE: test_f_next_working_day.sql
-- Purpose:   Functional test suite for ds.F_NEXT_WORKING_DAY
--            Runs 15 scenarios through BOTH:
--              (A) SQL Server logic   - CASE + LEFT JOIN LATERAL + LIMIT 1
--              (B) Snowflake logic    - IFF + MIN()
--            Compares results and asserts PASS / FAIL.
--
-- Dataset Design:
--   full_date   | is_weekday | is_admin_holiday | is_us_civil_holiday | Notes
--   2024-01-11  | Y          | N                | N                   | Thu  regular
--   2024-01-12  | Y          | N                | N                   | Fri  regular
--   2024-01-13  | N          | N                | N                   | Sat  weekend
--   2024-01-14  | N          | N                | N                   | Sun  weekend
--   2024-01-15  | Y          | Y                | Y                   | Mon  MLK Day (BOTH holiday types)
--   2024-01-16  | Y          | N                | N                   | Tue  regular
--   2024-01-17  | Y          | N                | N                   | Wed  regular
--   2024-01-18  | Y          | Y                | N                   | Thu  LTCG/admin holiday ONLY
--   2024-01-19  | Y          | N                | Y                   | Fri  FEDERAL/civil holiday ONLY
--   2024-01-20  | N          | N                | N                   | Sat  weekend
--   2024-01-21  | N          | N                | N                   | Sun  weekend
--   2024-01-22  | Y          | N                | N                   | Mon  regular (LAST entry)
--
-- TC scenarios covered:
--   TC01  Normal Friday + FEDERAL: skip Sat, Sun, MLK Monday
--   TC02  Normal Friday + LTCG:    skip Sat, Sun, MLK Monday
--   TC03  Input IS a holiday (FEDERAL) → return day after
--   TC04  Input IS a holiday (LTCG)    → return day after
--   TC05  LTCG admin-only holiday next day → LTCG skips it, returns day after
--   TC06  LTCG vs FEDERAL diverge: same date, different results
--   TC07  FEDERAL civil-only holiday next day → FEDERAL skips it + weekend
--   TC08  LTCG vs FEDERAL diverge: LTCG returns civil-holiday day, FEDERAL skips it
--   TC09  NULL date                      → NULL
--   TC10  NULL holiday_type              → NULL
--   TC11  Invalid holiday_type 'XYZ'    → NULL
--   TC12  Date not in calendar           → NULL
--   TC13  Last date in calendar          → NULL (no next working day)
--   TC14  Regular Thu → next day Fri
--   TC15  Saturday input                 → skip to next non-holiday weekday
-- ============================================================

WITH

-- -------------------------------------------------------
-- 1. Simulated RPT_CALENDAR_DAY
-- -------------------------------------------------------
calendar AS (
    SELECT '2024-01-11'::DATE AS full_date, 'Y' AS is_weekday, 'N' AS is_admin_holiday, 'N' AS is_us_civil_holiday
    UNION ALL SELECT '2024-01-12'::DATE, 'Y', 'N', 'N'
    UNION ALL SELECT '2024-01-13'::DATE, 'N', 'N', 'N'   -- Sat
    UNION ALL SELECT '2024-01-14'::DATE, 'N', 'N', 'N'   -- Sun
    UNION ALL SELECT '2024-01-15'::DATE, 'Y', 'Y', 'Y'   -- MLK Day (both)
    UNION ALL SELECT '2024-01-16'::DATE, 'Y', 'N', 'N'
    UNION ALL SELECT '2024-01-17'::DATE, 'Y', 'N', 'N'
    UNION ALL SELECT '2024-01-18'::DATE, 'Y', 'Y', 'N'   -- admin holiday only
    UNION ALL SELECT '2024-01-19'::DATE, 'Y', 'N', 'Y'   -- civil holiday only
    UNION ALL SELECT '2024-01-20'::DATE, 'N', 'N', 'N'   -- Sat
    UNION ALL SELECT '2024-01-21'::DATE, 'N', 'N', 'N'   -- Sun
    UNION ALL SELECT '2024-01-22'::DATE, 'Y', 'N', 'N'   -- Mon (LAST ENTRY)
),

-- -------------------------------------------------------
-- 2. Test input cases with expected results
-- -------------------------------------------------------
test_cases AS (
    SELECT  1 AS tc_id, '2024-01-12'::DATE AS p_date, 'FEDERAL'    AS p_holiday_type, '2024-01-16'::DATE AS expected, 'Fri + FEDERAL: skip Sat/Sun/MLK → Tue'                     AS description
    UNION ALL SELECT  2, '2024-01-12'::DATE, 'LTCG',    '2024-01-16'::DATE, 'Fri + LTCG: skip Sat/Sun/MLK → Tue'
    UNION ALL SELECT  3, '2024-01-15'::DATE, 'FEDERAL',  '2024-01-16'::DATE, 'Input IS holiday FEDERAL (MLK) → next Tue'
    UNION ALL SELECT  4, '2024-01-15'::DATE, 'LTCG',    '2024-01-16'::DATE, 'Input IS holiday LTCG (MLK) → next Tue'
    UNION ALL SELECT  5, '2024-01-17'::DATE, 'LTCG',    '2024-01-19'::DATE, 'Wed + LTCG: skip Thu (admin-only) → Fri'
    UNION ALL SELECT  6, '2024-01-17'::DATE, 'FEDERAL',  '2024-01-18'::DATE, 'Wed + FEDERAL: Thu is NOT federal → return Thu [LTCG vs FEDERAL DIFFER]'
    UNION ALL SELECT  7, '2024-01-18'::DATE, 'FEDERAL',  '2024-01-22'::DATE, 'Thu + FEDERAL: skip Fri (civil) + Sat/Sun → Mon'
    UNION ALL SELECT  8, '2024-01-18'::DATE, 'LTCG',    '2024-01-19'::DATE, 'Thu(admin) + LTCG: Fri is NOT admin → Fri [LTCG vs FEDERAL DIFFER]'
    UNION ALL SELECT  9, NULL::DATE,          'FEDERAL',  NULL::DATE,          'NULL date → NULL'
    UNION ALL SELECT 10, '2024-01-12'::DATE, NULL,       NULL::DATE,          'NULL holiday_type → NULL'
    UNION ALL SELECT 11, '2024-01-12'::DATE, 'XYZ',     NULL::DATE,          'Invalid holiday_type → NULL'
    UNION ALL SELECT 12, '1999-01-01'::DATE, 'FEDERAL',  NULL::DATE,          'Date not in calendar → NULL'
    UNION ALL SELECT 13, '2024-01-22'::DATE, 'FEDERAL',  NULL::DATE,          'Last calendar entry → NULL (no next working day)'
    UNION ALL SELECT 14, '2024-01-11'::DATE, 'FEDERAL',  '2024-01-12'::DATE, 'Thu regular → next day Fri'
    UNION ALL SELECT 15, '2024-01-13'::DATE, 'FEDERAL',  '2024-01-16'::DATE, 'Saturday input → skip to Tue (past weekend + MLK)'
),

-- -------------------------------------------------------
-- 3. SQL Server logic simulation
--    Mirrors: IF invalid → NULL
--             OUTER APPLY (SELECT TOP 1 ... ORDER BY ASC)
--             WHERE Cur.full_date = @Date
--    Simulated using: CASE guard + LEFT JOIN LATERAL + LIMIT 1
-- -------------------------------------------------------
sqlserver_results AS (
    SELECT
        t.tc_id,
        CASE
            -- IF @pHolidayType IS NULL OR NOT IN ('LTCG','FEDERAL') RETURN NULL
            WHEN t.p_holiday_type IS NULL
              OR t.p_holiday_type NOT IN ('LTCG', 'FEDERAL')
            THEN NULL::DATE

            -- WHERE Cur.full_date = @Date (date must exist in calendar)
            WHEN NOT EXISTS (
                SELECT 1 FROM calendar c WHERE c.full_date = t.p_date
            )
            THEN NULL::DATE

            -- OUTER APPLY (SELECT TOP 1 ... ORDER BY full_date ASC)
            ELSE (
                SELECT o.full_date
                FROM   calendar o
                WHERE  o.is_weekday = 'Y'
                  AND  o.full_date  > t.p_date
                  AND  CASE t.p_holiday_type
                           WHEN 'LTCG'    THEN o.is_admin_holiday
                           WHEN 'FEDERAL' THEN o.is_us_civil_holiday
                       END = 'N'
                ORDER BY o.full_date ASC
                LIMIT 1
            )
        END AS sqlserver_result
    FROM test_cases t
),

-- -------------------------------------------------------
-- 4. Snowflake logic (mirrors the deployed UDF exactly)
--    IFF(invalid → NULL, MIN() subquery with EXISTS guard)
-- -------------------------------------------------------
snowflake_results AS (
    SELECT
        t.tc_id,
        IFF(
            t.p_holiday_type IS NULL
            OR t.p_holiday_type NOT IN ('LTCG', 'FEDERAL'),
            NULL::DATE,
            (
                SELECT MIN(o.full_date)
                FROM   calendar o
                WHERE  o.is_weekday = 'Y'
                  AND  o.full_date  > t.p_date
                  AND  CASE t.p_holiday_type
                           WHEN 'LTCG'    THEN o.is_admin_holiday
                           WHEN 'FEDERAL' THEN o.is_us_civil_holiday
                       END = 'N'
                  AND  EXISTS (
                           SELECT 1 FROM calendar c
                           WHERE  c.full_date = t.p_date
                       )
            )
        ) AS snowflake_result
    FROM test_cases t
)

-- -------------------------------------------------------
-- 5. Final comparison report
-- -------------------------------------------------------
SELECT
    t.tc_id,
    t.description,
    t.p_date,
    t.p_holiday_type,
    t.expected,
    s.sqlserver_result,
    f.snowflake_result,

    -- SQL Server matches expected?
    CASE
        WHEN t.expected IS NULL AND s.sqlserver_result IS NULL THEN 'PASS'
        WHEN t.expected = s.sqlserver_result                   THEN 'PASS'
        ELSE 'FAIL'
    END AS sqlserver_vs_expected,

    -- Snowflake matches expected?
    CASE
        WHEN t.expected IS NULL AND f.snowflake_result IS NULL THEN 'PASS'
        WHEN t.expected = f.snowflake_result                   THEN 'PASS'
        ELSE 'FAIL'
    END AS snowflake_vs_expected,

    -- Do both engines agree with each other?
    CASE
        WHEN s.sqlserver_result IS NULL AND f.snowflake_result IS NULL THEN 'MATCH'
        WHEN s.sqlserver_result = f.snowflake_result                   THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS sqlserver_vs_snowflake

FROM      test_cases      t
JOIN      sqlserver_results s ON s.tc_id = t.tc_id
JOIN      snowflake_results  f ON f.tc_id = t.tc_id
ORDER BY  t.tc_id
;
