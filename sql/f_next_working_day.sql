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
--//                              - OUTER APPLY -> LEFT JOIN LATERAL
--//                              - TOP(1) -> LIMIT 1
--//                              - DATETIME -> DATE
--//                              - SQL Server variable syntax -> Snowflake SQL Scripting

CREATE OR REPLACE FUNCTION ds.F_NEXT_WORKING_DAY(
    p_date DATE,
    p_holiday_type VARCHAR
)
RETURNS DATE
LANGUAGE SQL
AS
$$
DECLARE
    v_return_date DATE;
BEGIN
    -- Validate parameters
    IF (p_holiday_type IS NULL OR p_holiday_type NOT IN ('LTCG', 'FEDERAL')) THEN
        RETURN NULL;
    END IF;

    -- Derive next business day
    -- SQL Server OUTER APPLY + TOP(1) converted to Snowflake LEFT JOIN LATERAL + LIMIT 1.
    -- Joins the input date's calendar row with the first subsequent weekday
    -- that is not a holiday (based on the specified holiday type).
    SELECT next_day.full_date INTO :v_return_date
    FROM ds.RPT_CALENDAR_DAY cur
    LEFT JOIN LATERAL (
        SELECT o.full_date
        FROM ds.RPT_CALENDAR_DAY o
        WHERE o.is_weekday = 'Y'
          AND o.full_date > cur.full_date
          AND 'N' = CASE :p_holiday_type
                        WHEN 'LTCG' THEN o.is_admin_holiday
                        WHEN 'FEDERAL' THEN o.is_us_civil_holiday
                    END
        ORDER BY o.full_date ASC
        LIMIT 1
    ) next_day
    WHERE cur.full_date = :p_date::DATE;

    RETURN v_return_date;
END;
$$;
