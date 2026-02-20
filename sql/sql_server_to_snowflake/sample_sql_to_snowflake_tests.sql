-- Sample SQL-to-Snowflake test cases for test_framework_run.py (SQLSERVER_TO_SNOWFLAKE workflow).
-- Placeholders: {source_database_name}, {target_database_name} are replaced at runtime.

-- START_TEST
-- @TEST_NAME: RowCnt_product_package
-- DESCRIPTION: Total row count of product_package between SQL Server and Snowflake (last 7 days).
-- @START_SQL_SERVER_QUERY:
SELECT
    'ROW_COUNT' AS check_name,
    COUNT(*) AS row_cnt
FROM {source_database_name}.DBO.product_package
WHERE last_mod_dt >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE))
AND last_mod_dt < CAST(GETDATE() AS DATE)
-- @END_SQL_SERVER_QUERY:
-- @START_SNOWFLAKE_QUERY:
SELECT
    'ROW_COUNT' AS check_name,
    COUNT(*) AS row_cnt
FROM {target_database_name}.DBO.product_package
WHERE last_mod_dt >= DATEADD(DAY, -7, CAST(CURRENT_DATE() AS DATE))
AND last_mod_dt < CAST(CURRENT_DATE() AS DATE)
-- @END_SNOWFLAKE_QUERY:
-- END_TEST

-- START_TEST
-- @TEST_NAME: PRODUCT_PACKAGE_PP_DESCRIPTION_Count
-- DESCRIPTION: Grouped count by pp_description from product_package.
-- @START_SQL_SERVER_QUERY:
SELECT
    'PRODUCT_PACKAGE_PP_DESCRIPTION_CNT' AS check_name,
    pp_description,
    COUNT(*) AS row_cnt
FROM {source_database_name}.DBO.product_package
WHERE last_mod_dt >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE))
AND last_mod_dt < CAST(GETDATE() AS DATE)
GROUP BY pp_description
-- @END_SQL_SERVER_QUERY:
-- @START_SNOWFLAKE_QUERY:
SELECT
    'PRODUCT_PACKAGE_PP_DESCRIPTION_CNT' AS check_name,
    pp_description,
    COUNT(*) AS row_cnt
FROM {target_database_name}.DBO.product_package
WHERE last_mod_dt >= DATEADD(DAY, -7, CAST(CURRENT_DATE() AS DATE))
AND last_mod_dt < CAST(CURRENT_DATE() AS DATE)
GROUP BY pp_description
-- @END_SNOWFLAKE_QUERY:
-- END_TEST
