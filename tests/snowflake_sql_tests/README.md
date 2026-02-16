# Snowflake SQL Data Quality Test Cases

## Overview
SQL-based test cases to validate data quality and prevent production bugs.

## Test Categories

### 1. **Null/Missing Data Tests** (`test_null_checks.sql`)
- Validates required fields are not null
- Checks for empty strings
- Validates data completeness

### 2. **Duplicate Detection** (`test_duplicates.sql`)
- Identifies duplicate records
- Validates primary key uniqueness
- Checks for unintended data multiplication

### 3. **Referential Integrity** (`test_referential_integrity.sql`)
- Validates foreign key relationships
- Checks orphaned records
- Validates join consistency

### 4. **Business Logic Validation** (`test_business_rules.sql`)
- Validates business calculations
- Checks date logic
- Validates status transitions

### 5. **Row Count Reconciliation** (`test_row_counts.sql`)
- Compares source vs target counts
- Validates no data loss
- Checks unexpected growth

### 6. **Date Range Validation** (`test_date_validation.sql`)
- Validates report date boundaries
- Checks for future dates
- Validates historical consistency

## Usage

### Run Individual Test:
```sql
-- In Snowflake worksheet
USE DATABASE {{TARGET_DATABASE}};
USE SCHEMA {{TARGET_SCHEMA}};

-- Set session variables
SET CARRIER_NAME = 'TEST_CARRIER';
SET REPORT_START_DT = '2024-01-01';
SET REPORT_END_DT = '2024-01-31';

-- Run test file
@tests/snowflake_sql_tests/test_null_checks.sql
```

### Run All Tests:
```sql
@tests/snowflake_sql_tests/run_all_tests.sql
```

## Test Output Format

Each test returns:
- **TEST_NAME**: Description of the test
- **TEST_STATUS**: 'PASS' or 'FAIL'
- **ISSUE_COUNT**: Number of issues found
- **DETAILS**: Description of failures
- **SAMPLE_RECORDS**: Example failing records (if any)

## Exit Codes
- **0 issues**: Test PASSED ✓
- **>0 issues**: Test FAILED ✗ (review DETAILS column)

## Integration with CI/CD
These tests can be automated in your pipeline using:
```bash
snowsql -f tests/snowflake_sql_tests/run_all_tests.sql -o exit_on_error=true
```
