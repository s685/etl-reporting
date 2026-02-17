--// Name:        ds.F_NEXT_WORKING_DAY
--// Purpose:     Add a working business day to supplied date.
--// Input Params: p_date          - The date to find the next working day for
--//              p_holiday_type  - Holiday category ['LTCG','FEDERAL']
--//
--// Returns:     The next working day (DATE) after p_date, or NULL if:
--//              - p_holiday_type is NULL or not in ('LTCG','FEDERAL')
--//              - p_date is not found in RPT_CALENDAR_DAY
--//              - No next working day exists in the calendar
--//
--// Modification History:
--// Date       #    By           Description
--// ??/??/??   ????              Created.
--// 04/20/09        gmakepeace   Added holiday category.
--// 02/24/10        gmakepeace   Refactor from procedural to set-based recursive CTE;
--//                              only going into recursion a few levels, so
--//                              should not be performance hit.
--// 03/09/10        gmakepeace   Refactor: replace recursive CTE with TOP/APPLY
--// 02/17/26                     Converted from SQL Server to Snowflake.
--//
--// Conversion Notes (SQL Server -> Snowflake):
--//
--//   1. Function type: SQL Server scalar function -> Snowflake pure SQL UDF
--//      (Snowflake Scripting UDFs do NOT support SQL statements like SELECT;
--//       must use a pure SQL expression UDF instead)
--//
--//   2. OUTER APPLY + TOP(1) -> single-table query with MIN()
--//      The original self-joined RPT_CALENDAR_DAY via OUTER APPLY to find the
--//      next row. In Snowflake, this simplifies to a direct MIN() query since
--//      the self-join only anchored on the input date.
--//
--//   3. IF validation -> CASE expression
--//      SQL Server IF/RETURN becomes a CASE wrapper around the query. Invalid
--//      p_holiday_type naturally returns NULL because the CASE inside WHERE
--//      evaluates to NULL, making no rows match, so MIN() returns NULL.
--//
--//   4. DATETIME -> DATE
--//      Snowflake DATE has no time component, so the original time-stripping
--//      logic (CAST(FLOOR(CAST(@pDate AS FLOAT)) AS DATETIME)) is unnecessary.
--//
--//   5. Parameters referenced directly by name (no @ or : prefix).
--//      No semicolon inside the UDF body per Snowflake documentation.
--//
--//   Original SQL Server behavior preserved:
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
    SELECT MIN(o.full_date)
    FROM ds.RPT_CALENDAR_DAY o
    WHERE o.is_weekday = 'Y'
      AND o.full_date > p_date
      AND CASE p_holiday_type
              WHEN 'LTCG'    THEN o.is_admin_holiday
              WHEN 'FEDERAL' THEN o.is_us_civil_holiday
          END = 'N'
      AND EXISTS (
              SELECT 1
              FROM ds.RPT_CALENDAR_DAY c
              WHERE c.full_date = p_date
          )
$$
;
