-- Example SQL using Snowflake session variables
-- Available variables: $CARRIER_NAME, $REPORT_START_DT, $REPORT_END_DT, $REPORT_RUN_DT, $AS_OF_RUN_DT

SELECT *
FROM source_table
WHERE carrier_name = $CARRIER_NAME
  AND service_date >= $REPORT_START_DT
  AND service_date <= $REPORT_END_DT
