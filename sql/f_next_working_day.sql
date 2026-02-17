--// Name:        ds.F_NEXT_WORKING_DAY
--// Purpose:     Add a working business day to supplied date.
--// Input Params: p_date          - The date to find the next working day for
--//              p_holiday_type  - Holiday category ['LTCG','FEDERAL']
--//
--// Modification History:
--// Date       #    By           Description
--// ??/??/??   ????              Created.
--// 04/20/09        gmakepeace   Added holiday category.
--// 02/24/10        gmakepeace   Refactor from procedural to set-based recursive CTE;
--//                              only going into recursion a few levels, so
--//                              should not be performance hit.
--// 03/09/10        gmakepeace   Refactor: replace recursive CTE with TOP/APPLY
--// 02/17/26                     Converted from SQL Server to Snowflake:
--//                              - OUTER APPLY + TOP(1) -> simplified single-table query + LIMIT 1
--//                              - DATETIME -> DATE (Snowflake DATE has no time component)
--//                              - SQL Server variable syntax -> Snowflake SQL Scripting
--//
--// Conversion Notes:
--//   The original SQL Server version used OUTER APPLY to join the input date's
--//   calendar row (Cur) with the next working day row (Next). In Snowflake, this
--//   is simplified to a direct single-table query since the self-join through Cur
--//   only served to anchor on the input date — we can use the input parameter directly.
--//
--//   Original SQL Server behavior preserved:
--//     - Returns NULL if p_holiday_type is NULL or not in ('LTCG','FEDERAL')
--//     - Returns NULL if p_date is not found in RPT_CALENDAR_DAY
--//     - Returns NULL if no next working day exists in the calendar
--//     - Skips weekends (is_weekday = 'N')
--//     - Skips holidays based on type:
--//         LTCG    -> skips rows where is_admin_holiday = 'Y'
--//         FEDERAL -> skips rows where is_us_civil_holiday = 'Y'

CREATE OR REPLACE FUNCTION ds.F_NEXT_WORKING_DAY(
    p_date DATE,
    p_holiday_type VARCHAR
)
RETURNS DATE
LANGUAGE SQL
AS
$$
DECLARE
    v_return_date  DATE;
    v_date_exists  BOOLEAN DEFAULT FALSE;
BEGIN
    -------------------------------------------------------------------------
    -- Step 1: Validate parameters
    --         Original: IF @pHolidayType IS NULL OR @pHolidayType NOT IN ('LTCG','FEDERAL') RETURN NULL
    -------------------------------------------------------------------------
    IF (p_holiday_type IS NULL OR p_holiday_type NOT IN ('LTCG', 'FEDERAL')) THEN
        RETURN NULL;
    END IF;

    -------------------------------------------------------------------------
    -- Step 2: Verify input date exists in calendar table
    --         Original SQL Server implicitly returned NULL when the input date
    --         was not found in RPT_CALENDAR_DAY (the WHERE Cur.full_date = @Date
    --         returned zero rows). We preserve this behavior explicitly.
    -------------------------------------------------------------------------
    SELECT TRUE INTO :v_date_exists
    FROM ds.RPT_CALENDAR_DAY
    WHERE full_date = :p_date
    LIMIT 1;

    IF (NOT v_date_exists) THEN
        RETURN NULL;
    END IF;

    -------------------------------------------------------------------------
    -- Step 3: Derive next business day
    --         Original used OUTER APPLY + TOP(1) to self-join RPT_CALENDAR_DAY.
    --         Simplified to a direct query: find the first date after p_date
    --         that is a weekday AND not a holiday for the given holiday type.
    --
    --         The CASE expression maps the holiday_type to the correct column:
    --           LTCG    -> check is_admin_holiday
    --           FEDERAL -> check is_us_civil_holiday
    --         We require the holiday flag = 'N' (not a holiday).
    -------------------------------------------------------------------------
    SELECT full_date INTO :v_return_date
    FROM ds.RPT_CALENDAR_DAY
    WHERE is_weekday = 'Y'
      AND full_date > :p_date
      AND 'N' = CASE :p_holiday_type
                    WHEN 'LTCG'    THEN is_admin_holiday
                    WHEN 'FEDERAL' THEN is_us_civil_holiday
                END
    ORDER BY full_date ASC
    LIMIT 1;

    RETURN v_return_date;
END;
$$;
