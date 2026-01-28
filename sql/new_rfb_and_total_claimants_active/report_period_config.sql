-- =====================================================================
-- REPORT PERIOD CONFIGURATION TABLE
-- =====================================================================
-- Purpose: Store monthly report periods for DMF execution
-- Automatically generates monthly periods without manual intervention
--
-- Timestamps: report_start_date = 00:00:00.000, report_end_date = 23:59:59.997
-- (start of day / end of day) for precise range comparisons.
-- 
-- Usage: This table stores report period metadata that can be used
-- to set session variables or populate target table report period columns
-- =====================================================================

-- Replace with your actual database and schema
SET config_db = '{{TARGET_DATABASE}}';
SET config_schema = '{{TARGET_SCHEMA}}';
SET config_table = 'report_period_config';

-- =====================================================================
-- STEP 1: CREATE TABLE
-- =====================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER($config_db || '.' || $config_schema || '.' || $config_table) (
    frequency VARCHAR(50) NOT NULL,
    report_start_date TIMESTAMP_NTZ NOT NULL,
    report_end_date TIMESTAMP_NTZ NOT NULL,
    as_of_run_dt DATE NOT NULL,
    carrier_name VARCHAR(255),
    created_dt TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (frequency, report_start_date, report_end_date, carrier_name)
);

-- =====================================================================
-- STEP 2: CREATE STORED PROCEDURE TO GENERATE MONTHLY PERIODS
-- =====================================================================
-- This procedure generates monthly periods for a given date range
-- Can be called manually or scheduled via task

CREATE OR REPLACE PROCEDURE IDENTIFIER($config_db || '.' || $config_schema || '.generate_monthly_periods')(
    start_year NUMBER,
    end_year NUMBER,
    carrier_name_param VARCHAR DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_date DATE;
    month_start DATE;
    month_end DATE;
    rows_inserted NUMBER := 0;
BEGIN
    -- Generate periods for each month from start_year to end_year
    current_date := DATE_FROM_PARTS(start_year, 1, 1);
    
    WHILE YEAR(current_date) <= end_year LOOP
        -- First day of current month
        month_start := DATE_TRUNC('MONTH', current_date);
        
        -- Last day of current month
        month_end := LAST_DAY(month_start);
        
        -- Insert monthly period (ignore if already exists)
        -- report_start_date = 00:00:00.000, report_end_date = 23:59:59.997
        INSERT INTO IDENTIFIER($config_db || '.' || $config_schema || '.' || $config_table)
            (frequency, report_start_date, report_end_date, as_of_run_dt, carrier_name)
        SELECT 
            'MONTHLY',
            month_start::TIMESTAMP_NTZ,
            DATEADD('millisecond', -3, (DATEADD('day', 1, month_end))::TIMESTAMP_NTZ),
            CURRENT_DATE(),
            carrier_name_param
        WHERE NOT EXISTS (
            SELECT 1 
            FROM IDENTIFIER($config_db || '.' || $config_schema || '.' || $config_table)
            WHERE frequency = 'MONTHLY'
              AND report_start_date = month_start::TIMESTAMP_NTZ
              AND report_end_date = DATEADD('millisecond', -3, (DATEADD('day', 1, month_end))::TIMESTAMP_NTZ)
              AND (carrier_name = carrier_name_param OR (carrier_name IS NULL AND carrier_name_param IS NULL))
        );
        
        GET DIAGNOSTICS rows_inserted = ROW_COUNT;
        
        -- Move to first day of next month
        current_date := ADD_MONTHS(month_start, 1);
    END LOOP;
    
    RETURN 'Monthly periods generated successfully for years ' || start_year || ' to ' || end_year;
END;
$$;

-- =====================================================================
-- STEP 3: CREATE TASK TO AUTO-GENERATE NEXT MONTH'S PERIOD
-- =====================================================================
-- This task runs monthly to add the next month's period automatically
-- Runs on the 25th of each month to prepare for next month

CREATE OR REPLACE TASK IDENTIFIER($config_db || '.' || $config_schema || '.auto_generate_monthly_period')
WAREHOUSE = 'YOUR_WAREHOUSE'
SCHEDULE = 'USING CRON 0 0 25 * * UTC'  -- 25th of each month at midnight UTC
AS
CALL IDENTIFIER($config_db || '.' || $config_schema || '.generate_monthly_periods')(
    YEAR(CURRENT_DATE()),
    YEAR(ADD_MONTHS(CURRENT_DATE(), 1)),
    NULL  -- Set carrier_name if needed, or modify to use session variable
);

-- =====================================================================
-- STEP 4: INITIAL POPULATION
-- =====================================================================
-- Generate periods for current year and next year (or adjust range as needed)

CALL IDENTIFIER($config_db || '.' || $config_schema || '.generate_monthly_periods')(
    YEAR(CURRENT_DATE()),
    YEAR(CURRENT_DATE()) + 1,
    NULL  -- Set carrier_name if needed
);

-- =====================================================================
-- STEP 5: CREATE VIEW FOR CURRENT PERIOD (FOR DMF USE)
-- =====================================================================
-- This view returns the current active period (deterministic at query time)
-- DMFs can use this view to get the current period dynamically
-- Views are deterministic - they execute the query when accessed

CREATE OR REPLACE VIEW IDENTIFIER($config_db || '.' || $config_schema || '.report_period_current') AS
SELECT 
    frequency,
    report_start_date,
    report_end_date,
    as_of_run_dt,
    carrier_name
FROM IDENTIFIER($config_db || '.' || $config_schema || '.' || $config_table)
WHERE frequency = 'MONTHLY'
  AND CURRENT_TIMESTAMP() BETWEEN report_start_date AND report_end_date
ORDER BY report_start_date DESC
LIMIT 1;

-- =====================================================================
-- STEP 5b: CREATE VIEW report_period_all_frequencies (6 ROWS, ONE PER FREQUENCY)
-- =====================================================================
-- Single view containing 6 records: DAILY, WEEKLY, MONTHLY, QUARTERLY, SEMI_ANNUAL, YEARLY
-- report_start_date / report_end_date = last period; timestamps:
--   report_start_date = 00:00:00.000, report_end_date = 23:59:59.997
-- as_of_run_dt = CURRENT_DATE() (when the view is queried)
-- Use this view as DMF config TABLE; filter by frequency (e.g. MONTHLY) inside DMF or via wrapper views

CREATE OR REPLACE VIEW IDENTIFIER($config_db || '.' || $config_schema || '.report_period_all_frequencies') AS
WITH cd AS (SELECT CURRENT_DATE() AS d)
SELECT 'DAILY'        AS frequency,
       (DATEADD('day', -1, d))::TIMESTAMP_NTZ AS report_start_date,
       DATEADD('millisecond', -3, (DATEADD('day', 1, DATEADD('day', -1, d)))::TIMESTAMP_NTZ) AS report_end_date,
       d AS as_of_run_dt,
       CAST(NULL AS VARCHAR) AS carrier_name
FROM cd
UNION ALL
SELECT 'WEEKLY',
       (DATEADD('day', -7, DATE_TRUNC('week', d)))::TIMESTAMP_NTZ,
       DATEADD('millisecond', -3, (DATEADD('day', 1, DATEADD('day', -1, DATE_TRUNC('week', d))))::TIMESTAMP_NTZ),
       d, CAST(NULL AS VARCHAR)
FROM cd
UNION ALL
SELECT 'MONTHLY',
       (ADD_MONTHS(DATE_TRUNC('month', d), -1))::TIMESTAMP_NTZ,
       DATEADD('millisecond', -3, (DATEADD('day', 1, LAST_DAY(ADD_MONTHS(DATE_TRUNC('month', d), -1))))::TIMESTAMP_NTZ),
       d, CAST(NULL AS VARCHAR)
FROM cd
UNION ALL
SELECT 'QUARTERLY',
       (ADD_MONTHS(DATE_TRUNC('quarter', d), -3))::TIMESTAMP_NTZ,
       DATEADD('millisecond', -3, (DATEADD('day', 1, LAST_DAY(ADD_MONTHS(DATE_TRUNC('quarter', d), -1))))::TIMESTAMP_NTZ),
       d, CAST(NULL AS VARCHAR)
FROM cd
UNION ALL
SELECT 'SEMI_ANNUAL',
       (CASE WHEN MONTH(d) <= 6 THEN DATE_FROM_PARTS(YEAR(d) - 1, 7, 1)
             ELSE DATE_FROM_PARTS(YEAR(d), 1, 1) END)::TIMESTAMP_NTZ,
       DATEADD('millisecond', -3, (DATEADD('day', 1, CASE WHEN MONTH(d) <= 6 THEN DATE_FROM_PARTS(YEAR(d) - 1, 12, 31)
             ELSE DATE_FROM_PARTS(YEAR(d), 6, 30) END))::TIMESTAMP_NTZ),
       d, CAST(NULL AS VARCHAR)
FROM cd
UNION ALL
SELECT 'YEARLY',
       (DATE_FROM_PARTS(YEAR(d) - 1, 1, 1))::TIMESTAMP_NTZ,
       DATEADD('millisecond', -3, (DATEADD('day', 1, DATE_FROM_PARTS(YEAR(d) - 1, 12, 31)))::TIMESTAMP_NTZ),
       d, CAST(NULL AS VARCHAR)
FROM cd;

-- =====================================================================
-- STEP 6: ENABLE TASK (after verifying initial population)
-- =====================================================================
-- Uncomment to enable automatic monthly generation:
-- ALTER TASK IDENTIFIER($config_db || '.' || $config_schema || '.auto_generate_monthly_period') RESUME;

-- =====================================================================
-- USAGE EXAMPLES
-- =====================================================================

-- View all periods (table):
-- SELECT * FROM IDENTIFIER($config_db || '.' || $config_schema || '.' || $config_table)
-- ORDER BY report_start_date DESC;

-- View all 6 frequencies (report_period_all_frequencies; use as DMF config):
-- SELECT * FROM IDENTIFIER($config_db || '.' || $config_schema || '.report_period_all_frequencies');

-- Get current month's period (timestamps 00:00:00.000 / 23:59:59.997):
-- SELECT report_start_date, report_end_date 
-- FROM IDENTIFIER($config_db || '.' || $config_schema || '.' || $config_table)
-- WHERE frequency = 'MONTHLY'
--   AND CURRENT_TIMESTAMP() BETWEEN report_start_date AND report_end_date
-- LIMIT 1;

-- Generate periods for specific carrier:
-- CALL IDENTIFIER($config_db || '.' || $config_schema || '.generate_monthly_periods')(
--     YEAR(CURRENT_DATE()),
--     YEAR(CURRENT_DATE()) + 1,
--     'CARRIER_NAME'
-- );

-- =====================================================================
-- NOTES
-- =====================================================================
-- 1. Table stores monthly report periods with carrier_name (optional)
-- 2. Stored procedure generates periods for a date range
-- 3. Task automatically adds next month's period on 25th of each month
-- 4. Primary key prevents duplicates
-- 5. report_period_all_frequencies: single view with 6 rows (DAILY, WEEKLY, MONTHLY,
--    QUARTERLY, SEMI_ANNUAL, YEARLY). report_start_date = 00:00:00.000, report_end_date = 23:59:59.997.
--    as_of_run_dt = CURRENT_DATE(). Use as DMF config.
-- 6. Modify task schedule or procedure logic as needed for your requirements
-- =====================================================================
