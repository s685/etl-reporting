-- ============================================================
-- TEST FILE: test_f_next_working_day.sql
-- Purpose:   Functional test suite for ds.F_NEXT_WORKING_DAY
--            Runs 26 scenarios through BOTH:
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
--   Group 1 - Core business logic (TC01-TC15)
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
--
--   Group 2 - Timestamp inputs (TC16-TC22)
--   TC16  TIMESTAMP_NTZ morning time     → time stripped, uses date portion
--   TC17  TIMESTAMP_NTZ end of day       → time stripped, same date
--   TC18  TIMESTAMP_NTZ midnight         → time stripped, same date
--   TC19  TIMESTAMP_TZ with +ve offset   → date from embedded offset
--   TC20  TIMESTAMP_TZ with -ve offset   → date from embedded offset
--   TC21  TIMESTAMP_NTZ on a holiday     → time stripped, holiday skipped
--   TC22  TIMESTAMP_NTZ NULL             → NULL
--
--   Group 3 - Date string / format inputs (TC23-TC26)
--   TC23  ISO format string  'YYYY-MM-DD'    → valid, works
--   TC24  YYYYMMDD integer format            → valid after cast
--   TC25  Named month 'DD-MON-YYYY'          → depends on DATE_INPUT_FORMAT
--   TC26  US slash format 'MM/DD/YYYY'       → depends on DATE_INPUT_FORMAT
--
-- IMPORTANT NOTES on TIMESTAMP_LTZ (not tested inline):
--   TIMESTAMP_LTZ → DATE depends on the SESSION TIMEZONE setting.
--   e.g. '2024-01-12 23:30:00' stored as UTC = Jan 13 in UTC session
--        but = Jan 12 in America/New_York session (UTC-5).
--   Recommendation: always pass DATE or TIMESTAMP_NTZ to this function
--   to avoid session-timezone-dependent date shifts.
--
-- IMPORTANT NOTES on string formats (TC25, TC26):
--   Snowflake parses string inputs using the session DATE_INPUT_FORMAT.
--   Default is 'YYYY-MM-DD' (ISO). If your session uses a different
--   format, adjust the input strings accordingly or cast explicitly.
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
--    Group 2 (TC16-TC22): TIMESTAMP inputs are cast to DATE
--    here in the CTE to simulate what Snowflake does when a
--    TIMESTAMP is passed to a DATE-typed UDF parameter.
-- -------------------------------------------------------
test_cases AS (

    -- ---- Group 1: Core business logic ----
    SELECT  1 AS tc_id, '2024-01-12'::DATE AS p_date, 'FEDERAL' AS p_holiday_type, '2024-01-16'::DATE AS expected, 'Fri + FEDERAL: skip Sat/Sun/MLK → Tue'                     AS description
    UNION ALL SELECT  2, '2024-01-12'::DATE, 'LTCG',    '2024-01-16'::DATE, 'Fri + LTCG: skip Sat/Sun/MLK → Tue'
    UNION ALL SELECT  3, '2024-01-15'::DATE, 'FEDERAL', '2024-01-16'::DATE, 'Input IS holiday FEDERAL (MLK) → next Tue'
    UNION ALL SELECT  4, '2024-01-15'::DATE, 'LTCG',    '2024-01-16'::DATE, 'Input IS holiday LTCG (MLK) → next Tue'
    UNION ALL SELECT  5, '2024-01-17'::DATE, 'LTCG',    '2024-01-19'::DATE, 'Wed + LTCG: skip Thu (admin-only) → Fri'
    UNION ALL SELECT  6, '2024-01-17'::DATE, 'FEDERAL', '2024-01-18'::DATE, 'Wed + FEDERAL: Thu is NOT federal → Thu [LTCG≠FEDERAL]'
    UNION ALL SELECT  7, '2024-01-18'::DATE, 'FEDERAL', '2024-01-22'::DATE, 'Thu + FEDERAL: skip Fri(civil)+Sat/Sun → Mon'
    UNION ALL SELECT  8, '2024-01-18'::DATE, 'LTCG',    '2024-01-19'::DATE, 'Thu(admin) + LTCG: Fri NOT admin → Fri [LTCG≠FEDERAL]'
    UNION ALL SELECT  9, NULL::DATE,          'FEDERAL', NULL::DATE,          'NULL date → NULL'
    UNION ALL SELECT 10, '2024-01-12'::DATE, NULL,       NULL::DATE,          'NULL holiday_type → NULL'
    UNION ALL SELECT 11, '2024-01-12'::DATE, 'XYZ',     NULL::DATE,          'Invalid holiday_type XYZ → NULL'
    UNION ALL SELECT 12, '1999-01-01'::DATE, 'FEDERAL', NULL::DATE,          'Date not in calendar → NULL'
    UNION ALL SELECT 13, '2024-01-22'::DATE, 'FEDERAL', NULL::DATE,          'Last calendar entry → NULL (no next working day)'
    UNION ALL SELECT 14, '2024-01-11'::DATE, 'FEDERAL', '2024-01-12'::DATE, 'Thu regular → next day Fri'
    UNION ALL SELECT 15, '2024-01-13'::DATE, 'FEDERAL', '2024-01-16'::DATE, 'Saturday input → skip to Tue (past weekend+MLK)'

    -- ---- Group 2: TIMESTAMP inputs ----
    -- Snowflake auto-casts TIMESTAMP to DATE when the function parameter is DATE.
    -- Casting is done here explicitly (::DATE) to simulate that UDF-entry behaviour.
    -- Rule: TIMESTAMP_NTZ truncates to the stored date (no timezone adjustment).
    --       TIMESTAMP_TZ uses the embedded offset to determine the date.
    --       TIMESTAMP_LTZ uses the SESSION TIMEZONE — NOT tested inline (see note above).

    -- TC16: TIMESTAMP_NTZ morning time (09:30) on a Friday
    --   '2024-01-12 09:30:00'::TIMESTAMP_NTZ → DATE = 2024-01-12 (time stripped)
    UNION ALL SELECT 16,
        '2024-01-12 09:30:00'::TIMESTAMP_NTZ::DATE,
        'FEDERAL',
        '2024-01-16'::DATE,
        'TIMESTAMP_NTZ morning (09:30) Fri → time stripped → same as TC01'

    -- TC17: TIMESTAMP_NTZ end of day (23:59:59) on a Friday
    --   '2024-01-12 23:59:59'::TIMESTAMP_NTZ → DATE = 2024-01-12 (time stripped)
    UNION ALL SELECT 17,
        '2024-01-12 23:59:59'::TIMESTAMP_NTZ::DATE,
        'FEDERAL',
        '2024-01-16'::DATE,
        'TIMESTAMP_NTZ end-of-day (23:59:59) Fri → time stripped → same as TC01'

    -- TC18: TIMESTAMP_NTZ midnight (00:00:00) on a Friday
    --   '2024-01-12 00:00:00'::TIMESTAMP_NTZ → DATE = 2024-01-12
    UNION ALL SELECT 18,
        '2024-01-12 00:00:00'::TIMESTAMP_NTZ::DATE,
        'FEDERAL',
        '2024-01-16'::DATE,
        'TIMESTAMP_NTZ midnight (00:00:00) Fri → date unchanged → same as TC01'

    -- TC19: TIMESTAMP_TZ with positive offset (+05:30, IST)
    --   '2024-01-12 14:00:00+05:30' → date = 2024-01-12 (embedded offset used)
    UNION ALL SELECT 19,
        '2024-01-12 14:00:00+05:30'::TIMESTAMP_TZ::DATE,
        'FEDERAL',
        '2024-01-16'::DATE,
        'TIMESTAMP_TZ +05:30 afternoon → date = Jan 12 → same as TC01'

    -- TC20: TIMESTAMP_TZ with negative offset (-05:00, EST) near midnight
    --   '2024-01-12 23:30:00-05:00' → date = 2024-01-12 (offset kept, still Jan 12)
    UNION ALL SELECT 20,
        '2024-01-12 23:30:00-05:00'::TIMESTAMP_TZ::DATE,
        'FEDERAL',
        '2024-01-16'::DATE,
        'TIMESTAMP_TZ -05:00 near midnight → date still Jan 12 → same as TC01'

    -- TC21: TIMESTAMP_NTZ on a holiday (MLK Mon at 10:00 AM)
    --   '2024-01-15 10:00:00'::TIMESTAMP_NTZ → DATE = 2024-01-15 (holiday)
    --   → function should skip it and return Jan 16
    UNION ALL SELECT 21,
        '2024-01-15 10:00:00'::TIMESTAMP_NTZ::DATE,
        'FEDERAL',
        '2024-01-16'::DATE,
        'TIMESTAMP_NTZ on MLK Day holiday → strips to Jan 15 → skips → Jan 16'

    -- TC22: TIMESTAMP_NTZ NULL → NULL date → NULL result
    UNION ALL SELECT 22,
        NULL::TIMESTAMP_NTZ::DATE,
        'FEDERAL',
        NULL::DATE,
        'TIMESTAMP_NTZ NULL → NULL date → NULL'

    -- ---- Group 3: Date string / format inputs ----
    -- Snowflake parses string → DATE using the session DATE_INPUT_FORMAT.
    -- Default session format is AUTO (accepts ISO 'YYYY-MM-DD' and others).
    -- These casts simulate what happens when a caller passes a string literal.

    -- TC23: ISO format 'YYYY-MM-DD' — always valid in Snowflake (default format)
    UNION ALL SELECT 23,
        TO_DATE('2024-01-12', 'YYYY-MM-DD'),
        'FEDERAL',
        '2024-01-16'::DATE,
        'String ISO format YYYY-MM-DD → valid → same as TC01'

    -- TC24: YYYYMMDD compact integer-style format
    UNION ALL SELECT 24,
        TO_DATE('20240112', 'YYYYMMDD'),
        'FEDERAL',
        '2024-01-16'::DATE,
        'String compact YYYYMMDD → valid → same as TC01'

    -- TC25: Named month format 'DD-MON-YYYY'
    UNION ALL SELECT 25,
        TO_DATE('12-JAN-2024', 'DD-MON-YYYY'),
        'FEDERAL',
        '2024-01-16'::DATE,
        'String DD-MON-YYYY → valid with explicit format → same as TC01'

    -- TC26: US slash format 'MM/DD/YYYY'
    UNION ALL SELECT 26,
        TO_DATE('01/12/2024', 'MM/DD/YYYY'),
        'FEDERAL',
        '2024-01-16'::DATE,
        'String MM/DD/YYYY → valid with explicit format → same as TC01'
),

-- -------------------------------------------------------
-- 3. SQL Server logic simulation
--    Mirrors: IF invalid → NULL
--             OUTER APPLY (SELECT TOP 1 ... ORDER BY ASC)
--             WHERE Cur.full_date = @Date
-- -------------------------------------------------------
sqlserver_results AS (
    SELECT
        t.tc_id,
        CASE
            WHEN t.p_holiday_type IS NULL
              OR t.p_holiday_type NOT IN ('LTCG', 'FEDERAL')
            THEN NULL::DATE

            WHEN NOT EXISTS (
                SELECT 1 FROM calendar c WHERE c.full_date = t.p_date
            )
            THEN NULL::DATE

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
    t.p_date                                   AS input_date_after_cast,
    t.p_holiday_type,
    t.expected,
    s.sqlserver_result,
    f.snowflake_result,

    CASE
        WHEN t.expected IS NULL AND s.sqlserver_result IS NULL THEN 'PASS'
        WHEN t.expected = s.sqlserver_result                   THEN 'PASS'
        ELSE 'FAIL'
    END AS sqlserver_vs_expected,

    CASE
        WHEN t.expected IS NULL AND f.snowflake_result IS NULL THEN 'PASS'
        WHEN t.expected = f.snowflake_result                   THEN 'PASS'
        ELSE 'FAIL'
    END AS snowflake_vs_expected,

    CASE
        WHEN s.sqlserver_result IS NULL AND f.snowflake_result IS NULL THEN 'MATCH'
        WHEN s.sqlserver_result = f.snowflake_result                   THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS sqlserver_vs_snowflake

FROM      test_cases       t
JOIN      sqlserver_results s ON s.tc_id = t.tc_id
JOIN      snowflake_results  f ON f.tc_id = t.tc_id
ORDER BY  t.tc_id
;
