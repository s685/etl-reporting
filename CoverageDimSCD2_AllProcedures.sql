-- ============================================================================
-- PROCEDURE 1: COVERAGE_DIM MERGE (Updated - All Columns)
-- Updates your existing sp_coverage_dim_merge with 26 additional columns
-- This keeps COVERAGE_DIM as Type 1 (current record) but now with ALL columns
-- ============================================================================

CREATE OR REPLACE PROCEDURE infomart.sp_coverage_dim_merge(
    C_NAME VARCHAR(16777216),
    SNOW_DB VARCHAR(16777216),
    CARRIER_DB VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$

    // ---------------------------------------------------------------
    // Step 1: Create or Replace COVERAGE_D staging view
    // Now includes ALL 26 additional columns from COVERAGE table
    // ---------------------------------------------------------------
    var create_table_sql = `create or replace table ${SNOW_DB}.INFOMART.COVERAGE_D AS
    WITH
    CTE_COVERAGE_IDENTITY AS
    (
        SELECT
            MD5(CAST(COVERAGE_ID AS VARCHAR) || '${C_NAME}') AS COVERAGE_DIM_ID,
            COVERAGE_ID,
            POLICY_ID
        FROM
            ${CARRIER_DB}.DBO.COVERAGE_IDENTITY
    )
    ,CTE_POLICY AS
    (
        SELECT
            MD5(CAST(POLICY_ID AS VARCHAR) || '${C_NAME}') AS POLICY_DIM_ID,
            POLICY_NO,
            POLICY_ID,
            SERIES_ID,
            CERT_NO
        FROM
            ${CARRIER_DB}.DBO.POLICY
    )
    ,CTE_COVERAGE AS
    (
        SELECT
            COVERAGE_ID,
            CREATING_LTCAS_EVENT_ID,
            OBSOLETING_LTCAS_EVENT_ID,
            COVERAGE_STATUS_CD,
            COVERAGE_STATUS_REASON_CD,
            COVERAGE_STATUS_DT,
            COVERAGE_APPROVAL_DT,
            COVERAGE_EFFECTIVE_DT,
            COVERAGE_EXPIRATION_DT,
            COVERAGE_ISSUE_DT,
            -- 26 additional columns
            CORE_BUY_UP_TYPE_CD,
            REISSUE_OF_COVERAGE_ID,
            AGE_CALC_CD,
            COVERAGE_RATED_AGE_DERIVE_DT,
            COVERAGE_REIN_COLLECT_TO_DT,
            COVERAGE_CHGE_FROM_APP_FLG,
            COVERAGE_PROCESS_CD,
            COVERAGE_CHANGE_CD,
            COVERAGE_NP_CD,
            COVERAGE_NP_EVENT_CD,
            COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT,
            COVERAGE_AMENDMENT_NOTICE_SENT_FLG,
            COVERAGE_CHANGE_OF_MIND_FLG,
            UNDERWRITER_USER_ID,
            SHARED_BENEFIT_COVERAGE_ID,
            COVERAGE_LP_EVENT_CD,
            COVERAGE_OVERRIDE_CED_RULE_DT,
            COVERAGE_DESCRIPTION,
            COVERAGE_UW_CLASS_VALUE,
            REVERT_COVERAGE_ID,
            COVERAGE_EXCHANGE_TYPE_CD,
            COVERAGE_ASSET_PROTECTION_TYPE_CD,
            COVERAGE_RATE_RULE_VALUE,
            COVERAGE_BUYOUT_AMOUNT,
            COMPANY_CODE
        FROM
            ${CARRIER_DB}.DBO.COVERAGE
    )
    ,CTE_ACTIVITY AS
    (
        SELECT
            ACTY_DESCRIPTION AS CREATING_ACTIVITY_DESC,
            ACTY_TRAN_CD
        FROM
            ${CARRIER_DB}.DBO.ACTIVITY
    )
    ,CTE_COVERAGE_EVENT AS
    (
        SELECT
            LTCAS_EVENT_ID  AS CREATING_EVENT_ID,
            ACTY_TRAN_CD    AS CREATING_EVENT_ACTY_TRAN_CD,
            LTCAS_EVENT_DT  AS CREATING_EVENT_DT
        FROM
            ${CARRIER_DB}.DBO.COVERAGE_EVENT
    )
    ,CTE_COVERAGE_STATUS AS
    (
        SELECT
            COVERAGE_STATUS_CD,
            COVERAGE_STATUS_DESCRIPTION
        FROM
            ${CARRIER_DB}.DBO.COVERAGE_STATUS
    )
    ,CTE_COVERAGE_STATUS_REASON AS
    (
        SELECT
            COVERAGE_STATUS_CD,
            COVERAGE_STATUS_REASON_CD,
            COVERAGE_STATUS_REASON_DESCRIPTION
        FROM
            ${CARRIER_DB}.DBO.COVERAGE_STATUS_REASON
    )

        SELECT
            CI.COVERAGE_DIM_ID
            ,P.POLICY_DIM_ID
            ,CI.COVERAGE_ID
            ,CI.POLICY_ID
            ,C.COVERAGE_STATUS_CD
            ,C.COVERAGE_STATUS_REASON_CD
            ,CS.COVERAGE_STATUS_DESCRIPTION
            ,CSR.COVERAGE_STATUS_REASON_DESCRIPTION
            ,C.COVERAGE_STATUS_DT
            ,C.COVERAGE_APPROVAL_DT
            ,C.COVERAGE_EFFECTIVE_DT
            ,C.COVERAGE_EXPIRATION_DT
            ,C.COVERAGE_ISSUE_DT
            ,CCE.CREATING_EVENT_ID
            ,CA.CREATING_ACTIVITY_DESC
            ,CCE.CREATING_EVENT_ACTY_TRAN_CD
            ,CCE.CREATING_EVENT_DT
            -- 26 additional columns
            ,C.OBSOLETING_LTCAS_EVENT_ID
            ,C.CORE_BUY_UP_TYPE_CD
            ,C.REISSUE_OF_COVERAGE_ID
            ,C.AGE_CALC_CD
            ,C.COVERAGE_RATED_AGE_DERIVE_DT
            ,C.COVERAGE_REIN_COLLECT_TO_DT
            ,C.COVERAGE_CHGE_FROM_APP_FLG
            ,C.COVERAGE_PROCESS_CD
            ,C.COVERAGE_CHANGE_CD
            ,C.COVERAGE_NP_CD
            ,C.COVERAGE_NP_EVENT_CD
            ,C.COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT
            ,C.COVERAGE_AMENDMENT_NOTICE_SENT_FLG
            ,C.COVERAGE_CHANGE_OF_MIND_FLG
            ,C.UNDERWRITER_USER_ID
            ,C.SHARED_BENEFIT_COVERAGE_ID
            ,C.COVERAGE_LP_EVENT_CD
            ,C.COVERAGE_OVERRIDE_CED_RULE_DT
            ,C.COVERAGE_DESCRIPTION
            ,C.COVERAGE_UW_CLASS_VALUE
            ,C.REVERT_COVERAGE_ID
            ,C.COVERAGE_EXCHANGE_TYPE_CD
            ,C.COVERAGE_ASSET_PROTECTION_TYPE_CD
            ,C.COVERAGE_RATE_RULE_VALUE
            ,C.COVERAGE_BUYOUT_AMOUNT
            ,C.COMPANY_CODE
            ,'${C_NAME}' AS CARRIER_NAME
            ,CURRENT_TIMESTAMP AS LOAD_DATE
        FROM CTE_COVERAGE_IDENTITY CI
        JOIN CTE_COVERAGE C ON C.COVERAGE_ID = CI.COVERAGE_ID
        JOIN CTE_POLICY P ON P.POLICY_ID = CI.POLICY_ID
        JOIN CTE_COVERAGE_EVENT CCE ON CCE.CREATING_EVENT_ID = C.CREATING_LTCAS_EVENT_ID
        JOIN CTE_ACTIVITY CA ON CA.ACTY_TRAN_CD = CCE.CREATING_EVENT_ACTY_TRAN_CD
        JOIN CTE_COVERAGE_STATUS CS ON CS.COVERAGE_STATUS_CD = C.COVERAGE_STATUS_CD
        JOIN CTE_COVERAGE_STATUS_REASON CSR ON CSR.COVERAGE_STATUS_CD = C.COVERAGE_STATUS_CD AND CSR.COVERAGE_STATUS_REASON_CD = C.COVERAGE_STATUS_REASON_CD
        LEFT JOIN CTE_COVERAGE_EVENT OCE ON OCE.CREATING_EVENT_ID = C.OBSOLETING_LTCAS_EVENT_ID
        LEFT JOIN CTE_ACTIVITY OA ON OA.ACTY_TRAN_CD = OCE.CREATING_EVENT_ACTY_TRAN_CD
        WHERE OCE.CREATING_EVENT_ID IS NULL
    `;

    // ---------------------------------------------------------------
    // Step 2: MERGE - Insert new coverages
    // ---------------------------------------------------------------
    var merge_sql = `
        MERGE INTO ${SNOW_DB}.INFOMART.COVERAGE_DIM AS ICD
        USING (
            SELECT
                COVERAGE_DIM_ID
                ,POLICY_DIM_ID
                ,COVERAGE_ID
                ,POLICY_ID
                ,COVERAGE_STATUS_CD
                ,COVERAGE_STATUS_REASON_CD
                ,COVERAGE_STATUS_DESCRIPTION
                ,COVERAGE_STATUS_REASON_DESCRIPTION
                ,COVERAGE_STATUS_DT
                ,COVERAGE_APPROVAL_DT
                ,COVERAGE_EFFECTIVE_DT
                ,COVERAGE_EXPIRATION_DT
                ,COVERAGE_ISSUE_DT
                ,CREATING_EVENT_ID
                ,CREATING_ACTIVITY_DESC
                ,CREATING_EVENT_ACTY_TRAN_CD
                ,CREATING_EVENT_DT
                ,OBSOLETING_LTCAS_EVENT_ID
                ,CORE_BUY_UP_TYPE_CD
                ,REISSUE_OF_COVERAGE_ID
                ,AGE_CALC_CD
                ,COVERAGE_RATED_AGE_DERIVE_DT
                ,COVERAGE_REIN_COLLECT_TO_DT
                ,COVERAGE_CHGE_FROM_APP_FLG
                ,COVERAGE_PROCESS_CD
                ,COVERAGE_CHANGE_CD
                ,COVERAGE_NP_CD
                ,COVERAGE_NP_EVENT_CD
                ,COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT
                ,COVERAGE_AMENDMENT_NOTICE_SENT_FLG
                ,COVERAGE_CHANGE_OF_MIND_FLG
                ,UNDERWRITER_USER_ID
                ,SHARED_BENEFIT_COVERAGE_ID
                ,COVERAGE_LP_EVENT_CD
                ,COVERAGE_OVERRIDE_CED_RULE_DT
                ,COVERAGE_DESCRIPTION
                ,COVERAGE_UW_CLASS_VALUE
                ,REVERT_COVERAGE_ID
                ,COVERAGE_EXCHANGE_TYPE_CD
                ,COVERAGE_ASSET_PROTECTION_TYPE_CD
                ,COVERAGE_RATE_RULE_VALUE
                ,COVERAGE_BUYOUT_AMOUNT
                ,COMPANY_CODE
                ,CARRIER_NAME
                ,LOAD_DATE
            FROM ${SNOW_DB}.INFOMART.COVERAGE_D
        ) AS SCD
        ON SCD.COVERAGE_DIM_ID = ICD.COVERAGE_DIM_ID
        WHEN NOT MATCHED THEN
        INSERT (
                COVERAGE_DIM_ID
                ,POLICY_DIM_ID
                ,COVERAGE_ID
                ,POLICY_ID
                ,COVERAGE_STATUS_CD
                ,COVERAGE_STATUS_REASON_CD
                ,COVERAGE_STATUS_DESCRIPTION
                ,COVERAGE_STATUS_REASON_DESCRIPTION
                ,COVERAGE_STATUS_DT
                ,COVERAGE_APPROVAL_DT
                ,COVERAGE_EFFECTIVE_DT
                ,COVERAGE_EXPIRATION_DT
                ,COVERAGE_ISSUE_DT
                ,CREATING_EVENT_ID
                ,CREATING_ACTIVITY_DESC
                ,CREATING_EVENT_ACTY_TRAN_CD
                ,CREATING_EVENT_DT
                ,OBSOLETING_LTCAS_EVENT_ID
                ,CORE_BUY_UP_TYPE_CD
                ,REISSUE_OF_COVERAGE_ID
                ,AGE_CALC_CD
                ,COVERAGE_RATED_AGE_DERIVE_DT
                ,COVERAGE_REIN_COLLECT_TO_DT
                ,COVERAGE_CHGE_FROM_APP_FLG
                ,COVERAGE_PROCESS_CD
                ,COVERAGE_CHANGE_CD
                ,COVERAGE_NP_CD
                ,COVERAGE_NP_EVENT_CD
                ,COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT
                ,COVERAGE_AMENDMENT_NOTICE_SENT_FLG
                ,COVERAGE_CHANGE_OF_MIND_FLG
                ,UNDERWRITER_USER_ID
                ,SHARED_BENEFIT_COVERAGE_ID
                ,COVERAGE_LP_EVENT_CD
                ,COVERAGE_OVERRIDE_CED_RULE_DT
                ,COVERAGE_DESCRIPTION
                ,COVERAGE_UW_CLASS_VALUE
                ,REVERT_COVERAGE_ID
                ,COVERAGE_EXCHANGE_TYPE_CD
                ,COVERAGE_ASSET_PROTECTION_TYPE_CD
                ,COVERAGE_RATE_RULE_VALUE
                ,COVERAGE_BUYOUT_AMOUNT
                ,COMPANY_CODE
                ,CARRIER_NAME
                ,LOAD_DATE
        )
        VALUES
        (
                SCD.COVERAGE_DIM_ID
                ,SCD.POLICY_DIM_ID
                ,SCD.COVERAGE_ID
                ,SCD.POLICY_ID
                ,SCD.COVERAGE_STATUS_CD
                ,SCD.COVERAGE_STATUS_REASON_CD
                ,SCD.COVERAGE_STATUS_DESCRIPTION
                ,SCD.COVERAGE_STATUS_REASON_DESCRIPTION
                ,SCD.COVERAGE_STATUS_DT
                ,SCD.COVERAGE_APPROVAL_DT
                ,SCD.COVERAGE_EFFECTIVE_DT
                ,SCD.COVERAGE_EXPIRATION_DT
                ,SCD.COVERAGE_ISSUE_DT
                ,SCD.CREATING_EVENT_ID
                ,SCD.CREATING_ACTIVITY_DESC
                ,SCD.CREATING_EVENT_ACTY_TRAN_CD
                ,SCD.CREATING_EVENT_DT
                ,SCD.OBSOLETING_LTCAS_EVENT_ID
                ,SCD.CORE_BUY_UP_TYPE_CD
                ,SCD.REISSUE_OF_COVERAGE_ID
                ,SCD.AGE_CALC_CD
                ,SCD.COVERAGE_RATED_AGE_DERIVE_DT
                ,SCD.COVERAGE_REIN_COLLECT_TO_DT
                ,SCD.COVERAGE_CHGE_FROM_APP_FLG
                ,SCD.COVERAGE_PROCESS_CD
                ,SCD.COVERAGE_CHANGE_CD
                ,SCD.COVERAGE_NP_CD
                ,SCD.COVERAGE_NP_EVENT_CD
                ,SCD.COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT
                ,SCD.COVERAGE_AMENDMENT_NOTICE_SENT_FLG
                ,SCD.COVERAGE_CHANGE_OF_MIND_FLG
                ,SCD.UNDERWRITER_USER_ID
                ,SCD.SHARED_BENEFIT_COVERAGE_ID
                ,SCD.COVERAGE_LP_EVENT_CD
                ,SCD.COVERAGE_OVERRIDE_CED_RULE_DT
                ,SCD.COVERAGE_DESCRIPTION
                ,SCD.COVERAGE_UW_CLASS_VALUE
                ,SCD.REVERT_COVERAGE_ID
                ,SCD.COVERAGE_EXCHANGE_TYPE_CD
                ,SCD.COVERAGE_ASSET_PROTECTION_TYPE_CD
                ,SCD.COVERAGE_RATE_RULE_VALUE
                ,SCD.COVERAGE_BUYOUT_AMOUNT
                ,SCD.COMPANY_CODE
                ,SCD.CARRIER_NAME
                ,SCD.LOAD_DATE
        );
    `;

    // ---------------------------------------------------------------
    // Step 3: UPDATE - Update existing coverages (Type 1 overwrite)
    // ---------------------------------------------------------------
    var update_sql = `
        MERGE INTO ${SNOW_DB}.INFOMART.COVERAGE_DIM AS ICD
        USING (
            SELECT
                COVERAGE_DIM_ID
                ,POLICY_DIM_ID
                ,COVERAGE_ID
                ,POLICY_ID
                ,COVERAGE_STATUS_CD
                ,COVERAGE_STATUS_REASON_CD
                ,COVERAGE_STATUS_DESCRIPTION
                ,COVERAGE_STATUS_REASON_DESCRIPTION
                ,COVERAGE_STATUS_DT
                ,COVERAGE_APPROVAL_DT
                ,COVERAGE_EFFECTIVE_DT
                ,COVERAGE_EXPIRATION_DT
                ,COVERAGE_ISSUE_DT
                ,CREATING_EVENT_ID
                ,CREATING_ACTIVITY_DESC
                ,CREATING_EVENT_ACTY_TRAN_CD
                ,CREATING_EVENT_DT
                ,OBSOLETING_LTCAS_EVENT_ID
                ,CORE_BUY_UP_TYPE_CD
                ,REISSUE_OF_COVERAGE_ID
                ,AGE_CALC_CD
                ,COVERAGE_RATED_AGE_DERIVE_DT
                ,COVERAGE_REIN_COLLECT_TO_DT
                ,COVERAGE_CHGE_FROM_APP_FLG
                ,COVERAGE_PROCESS_CD
                ,COVERAGE_CHANGE_CD
                ,COVERAGE_NP_CD
                ,COVERAGE_NP_EVENT_CD
                ,COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT
                ,COVERAGE_AMENDMENT_NOTICE_SENT_FLG
                ,COVERAGE_CHANGE_OF_MIND_FLG
                ,UNDERWRITER_USER_ID
                ,SHARED_BENEFIT_COVERAGE_ID
                ,COVERAGE_LP_EVENT_CD
                ,COVERAGE_OVERRIDE_CED_RULE_DT
                ,COVERAGE_DESCRIPTION
                ,COVERAGE_UW_CLASS_VALUE
                ,REVERT_COVERAGE_ID
                ,COVERAGE_EXCHANGE_TYPE_CD
                ,COVERAGE_ASSET_PROTECTION_TYPE_CD
                ,COVERAGE_RATE_RULE_VALUE
                ,COVERAGE_BUYOUT_AMOUNT
                ,COMPANY_CODE
                ,CARRIER_NAME
                ,LOAD_DATE
            FROM ${SNOW_DB}.INFOMART.COVERAGE_D
        ) AS SCD
        ON SCD.COVERAGE_DIM_ID = ICD.COVERAGE_DIM_ID
        WHEN MATCHED THEN
        UPDATE SET
                ICD.COVERAGE_DIM_ID = SCD.COVERAGE_DIM_ID
                ,ICD.POLICY_DIM_ID = SCD.POLICY_DIM_ID
                ,ICD.COVERAGE_ID = SCD.COVERAGE_ID
                ,ICD.POLICY_ID = SCD.POLICY_ID
                ,ICD.COVERAGE_STATUS_CD = SCD.COVERAGE_STATUS_CD
                ,ICD.COVERAGE_STATUS_REASON_CD = SCD.COVERAGE_STATUS_REASON_CD
                ,ICD.COVERAGE_STATUS_DESCRIPTION = SCD.COVERAGE_STATUS_DESCRIPTION
                ,ICD.COVERAGE_STATUS_REASON_DESCRIPTION = SCD.COVERAGE_STATUS_REASON_DESCRIPTION
                ,ICD.COVERAGE_STATUS_DT = SCD.COVERAGE_STATUS_DT
                ,ICD.COVERAGE_APPROVAL_DT = SCD.COVERAGE_APPROVAL_DT
                ,ICD.COVERAGE_EFFECTIVE_DT = SCD.COVERAGE_EFFECTIVE_DT
                ,ICD.COVERAGE_EXPIRATION_DT = SCD.COVERAGE_EXPIRATION_DT
                ,ICD.COVERAGE_ISSUE_DT = SCD.COVERAGE_ISSUE_DT
                ,ICD.CREATING_EVENT_ID = SCD.CREATING_EVENT_ID
                ,ICD.CREATING_ACTIVITY_DESC = SCD.CREATING_ACTIVITY_DESC
                ,ICD.CREATING_EVENT_ACTY_TRAN_CD = SCD.CREATING_EVENT_ACTY_TRAN_CD
                ,ICD.CREATING_EVENT_DT = SCD.CREATING_EVENT_DT
                ,ICD.OBSOLETING_LTCAS_EVENT_ID = SCD.OBSOLETING_LTCAS_EVENT_ID
                ,ICD.CORE_BUY_UP_TYPE_CD = SCD.CORE_BUY_UP_TYPE_CD
                ,ICD.REISSUE_OF_COVERAGE_ID = SCD.REISSUE_OF_COVERAGE_ID
                ,ICD.AGE_CALC_CD = SCD.AGE_CALC_CD
                ,ICD.COVERAGE_RATED_AGE_DERIVE_DT = SCD.COVERAGE_RATED_AGE_DERIVE_DT
                ,ICD.COVERAGE_REIN_COLLECT_TO_DT = SCD.COVERAGE_REIN_COLLECT_TO_DT
                ,ICD.COVERAGE_CHGE_FROM_APP_FLG = SCD.COVERAGE_CHGE_FROM_APP_FLG
                ,ICD.COVERAGE_PROCESS_CD = SCD.COVERAGE_PROCESS_CD
                ,ICD.COVERAGE_CHANGE_CD = SCD.COVERAGE_CHANGE_CD
                ,ICD.COVERAGE_NP_CD = SCD.COVERAGE_NP_CD
                ,ICD.COVERAGE_NP_EVENT_CD = SCD.COVERAGE_NP_EVENT_CD
                ,ICD.COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT = SCD.COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT
                ,ICD.COVERAGE_AMENDMENT_NOTICE_SENT_FLG = SCD.COVERAGE_AMENDMENT_NOTICE_SENT_FLG
                ,ICD.COVERAGE_CHANGE_OF_MIND_FLG = SCD.COVERAGE_CHANGE_OF_MIND_FLG
                ,ICD.UNDERWRITER_USER_ID = SCD.UNDERWRITER_USER_ID
                ,ICD.SHARED_BENEFIT_COVERAGE_ID = SCD.SHARED_BENEFIT_COVERAGE_ID
                ,ICD.COVERAGE_LP_EVENT_CD = SCD.COVERAGE_LP_EVENT_CD
                ,ICD.COVERAGE_OVERRIDE_CED_RULE_DT = SCD.COVERAGE_OVERRIDE_CED_RULE_DT
                ,ICD.COVERAGE_DESCRIPTION = SCD.COVERAGE_DESCRIPTION
                ,ICD.COVERAGE_UW_CLASS_VALUE = SCD.COVERAGE_UW_CLASS_VALUE
                ,ICD.REVERT_COVERAGE_ID = SCD.REVERT_COVERAGE_ID
                ,ICD.COVERAGE_EXCHANGE_TYPE_CD = SCD.COVERAGE_EXCHANGE_TYPE_CD
                ,ICD.COVERAGE_ASSET_PROTECTION_TYPE_CD = SCD.COVERAGE_ASSET_PROTECTION_TYPE_CD
                ,ICD.COVERAGE_RATE_RULE_VALUE = SCD.COVERAGE_RATE_RULE_VALUE
                ,ICD.COVERAGE_BUYOUT_AMOUNT = SCD.COVERAGE_BUYOUT_AMOUNT
                ,ICD.COMPANY_CODE = SCD.COMPANY_CODE
                ,ICD.CARRIER_NAME = SCD.CARRIER_NAME
                ,ICD.LOAD_DATE = SCD.LOAD_DATE
        ;
    `;

    // ---------------------------------------------------------------
    // Execute
    // ---------------------------------------------------------------
    var stmt1 = snowflake.createStatement({sqlText: create_table_sql});
    stmt1.execute();

    var stmt2 = snowflake.createStatement({sqlText: merge_sql});
    stmt2.execute();

    var stmt3 = snowflake.createStatement({sqlText: update_sql});
    stmt3.execute();

    return "Coverage Merge Procedure Executed Successfully";

$$;


-- ============================================================================
-- PROCEDURE 2: COVERAGE_DIM_SCD2 FULL LOAD (One-Time)
-- Loads ALL historical versions with dedup + LEAD()
-- ============================================================================

CREATE OR REPLACE PROCEDURE infomart.sp_coverage_dim_scd2_full_load(
    C_NAME VARCHAR(16777216),
    SNOW_DB VARCHAR(16777216),
    CARRIER_DB VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$

    // ---------------------------------------------------------------
    // Step 1: Create SCD2 table if not exists
    // ---------------------------------------------------------------
    var create_table_sql = `
        CREATE TABLE IF NOT EXISTS ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2 (
            COVERAGE_SK                         NUMBER AUTOINCREMENT PRIMARY KEY,
            -- Original COVERAGE_DIM columns
            COVERAGE_DIM_ID                     VARCHAR,
            POLICY_DIM_ID                       VARCHAR,
            COVERAGE_ID                         NUMBER,
            POLICY_ID                           NUMBER,
            COVERAGE_STATUS_CD                  NUMBER,
            COVERAGE_STATUS_REASON_CD           NUMBER,
            COVERAGE_STATUS_DESCRIPTION         VARCHAR,
            COVERAGE_STATUS_REASON_DESCRIPTION  VARCHAR,
            COVERAGE_STATUS_DT                  TIMESTAMP_NTZ(9),
            COVERAGE_APPROVAL_DT                TIMESTAMP_NTZ(9),
            COVERAGE_EFFECTIVE_DT               TIMESTAMP_NTZ(9),
            COVERAGE_EXPIRATION_DT              TIMESTAMP_NTZ(9),
            COVERAGE_ISSUE_DT                   TIMESTAMP_NTZ(9),
            CREATING_EVENT_ID                   NUMBER,
            CREATING_ACTIVITY_DESC              VARCHAR,
            CREATING_EVENT_ACTY_TRAN_CD         VARCHAR,
            CREATING_EVENT_DT                   TIMESTAMP_NTZ(9),
            -- 26 additional columns
            OBSOLETING_LTCAS_EVENT_ID           NUMBER(10,0),
            CORE_BUY_UP_TYPE_CD                 NUMBER(5,0),
            REISSUE_OF_COVERAGE_ID              NUMBER(10,0),
            AGE_CALC_CD                         NUMBER(5,0),
            COVERAGE_RATED_AGE_DERIVE_DT        TIMESTAMP_NTZ(9),
            COVERAGE_REIN_COLLECT_TO_DT         TIMESTAMP_NTZ(9),
            COVERAGE_CHGE_FROM_APP_FLG          NUMBER(5,0),
            COVERAGE_PROCESS_CD                 NUMBER(5,0),
            COVERAGE_CHANGE_CD                  NUMBER(5,0),
            COVERAGE_NP_CD                      NUMBER(5,0),
            COVERAGE_NP_EVENT_CD                NUMBER(10,0),
            COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT TIMESTAMP_NTZ(9),
            COVERAGE_AMENDMENT_NOTICE_SENT_FLG  NUMBER(5,0),
            COVERAGE_CHANGE_OF_MIND_FLG         NUMBER(5,0),
            UNDERWRITER_USER_ID                 VARCHAR(30),
            SHARED_BENEFIT_COVERAGE_ID          NUMBER(10,0),
            COVERAGE_LP_EVENT_CD                NUMBER(5,0),
            COVERAGE_OVERRIDE_CED_RULE_DT       TIMESTAMP_NTZ(9),
            COVERAGE_DESCRIPTION                VARCHAR(50),
            COVERAGE_UW_CLASS_VALUE             VARCHAR(20),
            REVERT_COVERAGE_ID                  NUMBER(10,0),
            COVERAGE_EXCHANGE_TYPE_CD           NUMBER(5,0),
            COVERAGE_ASSET_PROTECTION_TYPE_CD   NUMBER(5,0),
            COVERAGE_RATE_RULE_VALUE            VARCHAR(20),
            COVERAGE_BUYOUT_AMOUNT              NUMBER(12,2),
            COMPANY_CODE                        VARCHAR(40),
            CARRIER_NAME                        VARCHAR,
            LOAD_DATE                           TIMESTAMP_NTZ(9),
            -- SCD2 Columns
            DIM_EFF_DT                          DATE NOT NULL,
            DIM_EXP_DT                          DATE NOT NULL DEFAULT '9999-12-31',
            CURRENT_FLG                         CHAR(1) NOT NULL DEFAULT 'Y'
        )
    `;

    // ---------------------------------------------------------------
    // Step 2: Truncate (full reload)
    // ---------------------------------------------------------------
    var truncate_sql = `TRUNCATE TABLE ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2`;

    // ---------------------------------------------------------------
    // Step 3: Full load - ALL versions with dedup + LEAD()
    // ---------------------------------------------------------------
    var full_load_sql = `
        INSERT INTO ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2 (
            COVERAGE_DIM_ID, POLICY_DIM_ID, COVERAGE_ID, POLICY_ID,
            COVERAGE_STATUS_CD, COVERAGE_STATUS_REASON_CD,
            COVERAGE_STATUS_DESCRIPTION, COVERAGE_STATUS_REASON_DESCRIPTION,
            COVERAGE_STATUS_DT, COVERAGE_APPROVAL_DT,
            COVERAGE_EFFECTIVE_DT, COVERAGE_EXPIRATION_DT, COVERAGE_ISSUE_DT,
            CREATING_EVENT_ID, CREATING_ACTIVITY_DESC,
            CREATING_EVENT_ACTY_TRAN_CD, CREATING_EVENT_DT,
            OBSOLETING_LTCAS_EVENT_ID, CORE_BUY_UP_TYPE_CD,
            REISSUE_OF_COVERAGE_ID, AGE_CALC_CD,
            COVERAGE_RATED_AGE_DERIVE_DT, COVERAGE_REIN_COLLECT_TO_DT,
            COVERAGE_CHGE_FROM_APP_FLG, COVERAGE_PROCESS_CD,
            COVERAGE_CHANGE_CD, COVERAGE_NP_CD, COVERAGE_NP_EVENT_CD,
            COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT, COVERAGE_AMENDMENT_NOTICE_SENT_FLG,
            COVERAGE_CHANGE_OF_MIND_FLG, UNDERWRITER_USER_ID,
            SHARED_BENEFIT_COVERAGE_ID, COVERAGE_LP_EVENT_CD,
            COVERAGE_OVERRIDE_CED_RULE_DT, COVERAGE_DESCRIPTION,
            COVERAGE_UW_CLASS_VALUE, REVERT_COVERAGE_ID,
            COVERAGE_EXCHANGE_TYPE_CD, COVERAGE_ASSET_PROTECTION_TYPE_CD,
            COVERAGE_RATE_RULE_VALUE, COVERAGE_BUYOUT_AMOUNT, COMPANY_CODE,
            CARRIER_NAME, LOAD_DATE,
            DIM_EFF_DT, DIM_EXP_DT, CURRENT_FLG
        )

        WITH
        CTE_COVERAGE_IDENTITY AS
        (
            SELECT
                MD5(CAST(COVERAGE_ID AS VARCHAR) || '${C_NAME}') AS COVERAGE_DIM_ID,
                COVERAGE_ID,
                POLICY_ID
            FROM
                ${CARRIER_DB}.DBO.COVERAGE_IDENTITY
        )
        ,CTE_POLICY AS
        (
            SELECT
                MD5(CAST(POLICY_ID AS VARCHAR) || '${C_NAME}') AS POLICY_DIM_ID,
                POLICY_NO,
                POLICY_ID,
                SERIES_ID,
                CERT_NO
            FROM
                ${CARRIER_DB}.DBO.POLICY
        )
        ,CTE_COVERAGE AS
        (
            SELECT
                COVERAGE_ID,
                CREATING_LTCAS_EVENT_ID,
                OBSOLETING_LTCAS_EVENT_ID,
                COVERAGE_STATUS_CD,
                COVERAGE_STATUS_REASON_CD,
                COVERAGE_STATUS_DT,
                COVERAGE_APPROVAL_DT,
                COVERAGE_EFFECTIVE_DT,
                COVERAGE_EXPIRATION_DT,
                COVERAGE_ISSUE_DT,
                CORE_BUY_UP_TYPE_CD,
                REISSUE_OF_COVERAGE_ID,
                AGE_CALC_CD,
                COVERAGE_RATED_AGE_DERIVE_DT,
                COVERAGE_REIN_COLLECT_TO_DT,
                COVERAGE_CHGE_FROM_APP_FLG,
                COVERAGE_PROCESS_CD,
                COVERAGE_CHANGE_CD,
                COVERAGE_NP_CD,
                COVERAGE_NP_EVENT_CD,
                COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT,
                COVERAGE_AMENDMENT_NOTICE_SENT_FLG,
                COVERAGE_CHANGE_OF_MIND_FLG,
                UNDERWRITER_USER_ID,
                SHARED_BENEFIT_COVERAGE_ID,
                COVERAGE_LP_EVENT_CD,
                COVERAGE_OVERRIDE_CED_RULE_DT,
                COVERAGE_DESCRIPTION,
                COVERAGE_UW_CLASS_VALUE,
                REVERT_COVERAGE_ID,
                COVERAGE_EXCHANGE_TYPE_CD,
                COVERAGE_ASSET_PROTECTION_TYPE_CD,
                COVERAGE_RATE_RULE_VALUE,
                COVERAGE_BUYOUT_AMOUNT,
                COMPANY_CODE,
                DML_OPERATION,
                SEQUENCE_NO,
                SEQUENCED_AT
            FROM
                ${CARRIER_DB}.DBO.COVERAGE
        )
        ,CTE_ACTIVITY AS
        (
            SELECT
                ACTY_DESCRIPTION AS CREATING_ACTIVITY_DESC,
                ACTY_TRAN_CD
            FROM
                ${CARRIER_DB}.DBO.ACTIVITY
        )
        ,CTE_COVERAGE_EVENT AS
        (
            SELECT
                LTCAS_EVENT_ID  AS CREATING_EVENT_ID,
                ACTY_TRAN_CD    AS CREATING_EVENT_ACTY_TRAN_CD,
                LTCAS_EVENT_DT  AS CREATING_EVENT_DT
            FROM
                ${CARRIER_DB}.DBO.COVERAGE_EVENT
        )
        ,CTE_COVERAGE_STATUS AS
        (
            SELECT
                COVERAGE_STATUS_CD,
                COVERAGE_STATUS_DESCRIPTION
            FROM
                ${CARRIER_DB}.DBO.COVERAGE_STATUS
        )
        ,CTE_COVERAGE_STATUS_REASON AS
        (
            SELECT
                COVERAGE_STATUS_CD,
                COVERAGE_STATUS_REASON_CD,
                COVERAGE_STATUS_REASON_DESCRIPTION
            FROM
                ${CARRIER_DB}.DBO.COVERAGE_STATUS_REASON
        )

        -- Dedup: pick latest CDC record per (COVERAGE_ID, CREATING_LTCAS_EVENT_ID)
        ,CTE_DEDUPED AS
        (
            SELECT
                C.*,
                ROW_NUMBER() OVER (
                    PARTITION BY C.COVERAGE_ID, C.CREATING_LTCAS_EVENT_ID
                    ORDER BY C.SEQUENCED_AT DESC, C.DML_OPERATION DESC
                ) AS RN
            FROM CTE_COVERAGE C
        )

        -- Join all reference tables (ALL versions, no WHERE OCE IS NULL filter)
        ,CTE_JOINED AS
        (
            SELECT
                CI.COVERAGE_DIM_ID
                ,P.POLICY_DIM_ID
                ,CI.COVERAGE_ID
                ,CI.POLICY_ID
                ,C.COVERAGE_STATUS_CD
                ,C.COVERAGE_STATUS_REASON_CD
                ,CS.COVERAGE_STATUS_DESCRIPTION
                ,CSR.COVERAGE_STATUS_REASON_DESCRIPTION
                ,C.COVERAGE_STATUS_DT
                ,C.COVERAGE_APPROVAL_DT
                ,C.COVERAGE_EFFECTIVE_DT
                ,C.COVERAGE_EXPIRATION_DT
                ,C.COVERAGE_ISSUE_DT
                ,CCE.CREATING_EVENT_ID
                ,CA.CREATING_ACTIVITY_DESC
                ,CCE.CREATING_EVENT_ACTY_TRAN_CD
                ,CCE.CREATING_EVENT_DT
                ,C.OBSOLETING_LTCAS_EVENT_ID
                ,C.CORE_BUY_UP_TYPE_CD
                ,C.REISSUE_OF_COVERAGE_ID
                ,C.AGE_CALC_CD
                ,C.COVERAGE_RATED_AGE_DERIVE_DT
                ,C.COVERAGE_REIN_COLLECT_TO_DT
                ,C.COVERAGE_CHGE_FROM_APP_FLG
                ,C.COVERAGE_PROCESS_CD
                ,C.COVERAGE_CHANGE_CD
                ,C.COVERAGE_NP_CD
                ,C.COVERAGE_NP_EVENT_CD
                ,C.COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT
                ,C.COVERAGE_AMENDMENT_NOTICE_SENT_FLG
                ,C.COVERAGE_CHANGE_OF_MIND_FLG
                ,C.UNDERWRITER_USER_ID
                ,C.SHARED_BENEFIT_COVERAGE_ID
                ,C.COVERAGE_LP_EVENT_CD
                ,C.COVERAGE_OVERRIDE_CED_RULE_DT
                ,C.COVERAGE_DESCRIPTION
                ,C.COVERAGE_UW_CLASS_VALUE
                ,C.REVERT_COVERAGE_ID
                ,C.COVERAGE_EXCHANGE_TYPE_CD
                ,C.COVERAGE_ASSET_PROTECTION_TYPE_CD
                ,C.COVERAGE_RATE_RULE_VALUE
                ,C.COVERAGE_BUYOUT_AMOUNT
                ,C.COMPANY_CODE
                ,'${C_NAME}' AS CARRIER_NAME
                ,CURRENT_TIMESTAMP AS LOAD_DATE
            FROM CTE_DEDUPED C
            JOIN CTE_COVERAGE_IDENTITY CI ON C.COVERAGE_ID = CI.COVERAGE_ID
            JOIN CTE_POLICY P ON P.POLICY_ID = CI.POLICY_ID
            JOIN CTE_COVERAGE_EVENT CCE ON CCE.CREATING_EVENT_ID = C.CREATING_LTCAS_EVENT_ID
            JOIN CTE_ACTIVITY CA ON CA.ACTY_TRAN_CD = CCE.CREATING_EVENT_ACTY_TRAN_CD
            JOIN CTE_COVERAGE_STATUS CS ON CS.COVERAGE_STATUS_CD = C.COVERAGE_STATUS_CD
            JOIN CTE_COVERAGE_STATUS_REASON CSR
                ON CSR.COVERAGE_STATUS_CD = C.COVERAGE_STATUS_CD
                AND CSR.COVERAGE_STATUS_REASON_CD = C.COVERAGE_STATUS_REASON_CD
            WHERE C.RN = 1
        )

        -- Compute SCD2 date ranges using LEAD()
        ,CTE_VERSIONED AS
        (
            SELECT
                J.*,
                LEAD(J.CREATING_EVENT_DT) OVER (
                    PARTITION BY J.COVERAGE_ID
                    ORDER BY J.CREATING_EVENT_DT, J.CREATING_EVENT_ID
                ) AS NEXT_EVENT_DT
            FROM CTE_JOINED J
        )

        SELECT
            COVERAGE_DIM_ID, POLICY_DIM_ID, COVERAGE_ID, POLICY_ID,
            COVERAGE_STATUS_CD, COVERAGE_STATUS_REASON_CD,
            COVERAGE_STATUS_DESCRIPTION, COVERAGE_STATUS_REASON_DESCRIPTION,
            COVERAGE_STATUS_DT, COVERAGE_APPROVAL_DT,
            COVERAGE_EFFECTIVE_DT, COVERAGE_EXPIRATION_DT, COVERAGE_ISSUE_DT,
            CREATING_EVENT_ID, CREATING_ACTIVITY_DESC,
            CREATING_EVENT_ACTY_TRAN_CD, CREATING_EVENT_DT,
            OBSOLETING_LTCAS_EVENT_ID, CORE_BUY_UP_TYPE_CD,
            REISSUE_OF_COVERAGE_ID, AGE_CALC_CD,
            COVERAGE_RATED_AGE_DERIVE_DT, COVERAGE_REIN_COLLECT_TO_DT,
            COVERAGE_CHGE_FROM_APP_FLG, COVERAGE_PROCESS_CD,
            COVERAGE_CHANGE_CD, COVERAGE_NP_CD, COVERAGE_NP_EVENT_CD,
            COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT, COVERAGE_AMENDMENT_NOTICE_SENT_FLG,
            COVERAGE_CHANGE_OF_MIND_FLG, UNDERWRITER_USER_ID,
            SHARED_BENEFIT_COVERAGE_ID, COVERAGE_LP_EVENT_CD,
            COVERAGE_OVERRIDE_CED_RULE_DT, COVERAGE_DESCRIPTION,
            COVERAGE_UW_CLASS_VALUE, REVERT_COVERAGE_ID,
            COVERAGE_EXCHANGE_TYPE_CD, COVERAGE_ASSET_PROTECTION_TYPE_CD,
            COVERAGE_RATE_RULE_VALUE, COVERAGE_BUYOUT_AMOUNT, COMPANY_CODE,
            CARRIER_NAME, LOAD_DATE,

            -- SCD2: DIM_EFF_DT
            CAST(CREATING_EVENT_DT AS DATE) AS DIM_EFF_DT,

            -- SCD2: DIM_EXP_DT
            CASE
                WHEN NEXT_EVENT_DT IS NOT NULL
                THEN DATEADD(DAY, -1, CAST(NEXT_EVENT_DT AS DATE))
                ELSE CAST('9999-12-31' AS DATE)
            END AS DIM_EXP_DT,

            -- SCD2: CURRENT_FLG
            CASE
                WHEN NEXT_EVENT_DT IS NULL THEN 'Y'
                ELSE 'N'
            END AS CURRENT_FLG

        FROM CTE_VERSIONED
    `;

    // ---------------------------------------------------------------
    // Execute
    // ---------------------------------------------------------------
    var stmt1 = snowflake.createStatement({sqlText: create_table_sql});
    stmt1.execute();

    var stmt2 = snowflake.createStatement({sqlText: truncate_sql});
    stmt2.execute();

    var stmt3 = snowflake.createStatement({sqlText: full_load_sql});
    stmt3.execute();

    return "Coverage DIM SCD2 Full Load Executed Successfully";

$$;


-- ============================================================================
-- PROCEDURE 3: COVERAGE_DIM_SCD2 INCREMENTAL
-- Ongoing: Uses stream on COVERAGE_DIM to maintain SCD2 history
-- ============================================================================

CREATE OR REPLACE PROCEDURE infomart.sp_coverage_dim_scd2_incremental(
    SNOW_DB VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$

    // ---------------------------------------------------------------
    // Step 1: Update same version in place (Type 1)
    // Same COVERAGE_DIM_ID + same CREATING_EVENT_ID already in SCD2
    // ---------------------------------------------------------------
    var step1_update_same_version = `
        UPDATE ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2 tgt
        SET
            tgt.POLICY_DIM_ID = src.POLICY_DIM_ID
            ,tgt.COVERAGE_ID = src.COVERAGE_ID
            ,tgt.POLICY_ID = src.POLICY_ID
            ,tgt.COVERAGE_STATUS_CD = src.COVERAGE_STATUS_CD
            ,tgt.COVERAGE_STATUS_REASON_CD = src.COVERAGE_STATUS_REASON_CD
            ,tgt.COVERAGE_STATUS_DESCRIPTION = src.COVERAGE_STATUS_DESCRIPTION
            ,tgt.COVERAGE_STATUS_REASON_DESCRIPTION = src.COVERAGE_STATUS_REASON_DESCRIPTION
            ,tgt.COVERAGE_STATUS_DT = src.COVERAGE_STATUS_DT
            ,tgt.COVERAGE_APPROVAL_DT = src.COVERAGE_APPROVAL_DT
            ,tgt.COVERAGE_EFFECTIVE_DT = src.COVERAGE_EFFECTIVE_DT
            ,tgt.COVERAGE_EXPIRATION_DT = src.COVERAGE_EXPIRATION_DT
            ,tgt.COVERAGE_ISSUE_DT = src.COVERAGE_ISSUE_DT
            ,tgt.CREATING_ACTIVITY_DESC = src.CREATING_ACTIVITY_DESC
            ,tgt.CREATING_EVENT_ACTY_TRAN_CD = src.CREATING_EVENT_ACTY_TRAN_CD
            ,tgt.CREATING_EVENT_DT = src.CREATING_EVENT_DT
            ,tgt.OBSOLETING_LTCAS_EVENT_ID = src.OBSOLETING_LTCAS_EVENT_ID
            ,tgt.CORE_BUY_UP_TYPE_CD = src.CORE_BUY_UP_TYPE_CD
            ,tgt.REISSUE_OF_COVERAGE_ID = src.REISSUE_OF_COVERAGE_ID
            ,tgt.AGE_CALC_CD = src.AGE_CALC_CD
            ,tgt.COVERAGE_RATED_AGE_DERIVE_DT = src.COVERAGE_RATED_AGE_DERIVE_DT
            ,tgt.COVERAGE_REIN_COLLECT_TO_DT = src.COVERAGE_REIN_COLLECT_TO_DT
            ,tgt.COVERAGE_CHGE_FROM_APP_FLG = src.COVERAGE_CHGE_FROM_APP_FLG
            ,tgt.COVERAGE_PROCESS_CD = src.COVERAGE_PROCESS_CD
            ,tgt.COVERAGE_CHANGE_CD = src.COVERAGE_CHANGE_CD
            ,tgt.COVERAGE_NP_CD = src.COVERAGE_NP_CD
            ,tgt.COVERAGE_NP_EVENT_CD = src.COVERAGE_NP_EVENT_CD
            ,tgt.COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT = src.COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT
            ,tgt.COVERAGE_AMENDMENT_NOTICE_SENT_FLG = src.COVERAGE_AMENDMENT_NOTICE_SENT_FLG
            ,tgt.COVERAGE_CHANGE_OF_MIND_FLG = src.COVERAGE_CHANGE_OF_MIND_FLG
            ,tgt.UNDERWRITER_USER_ID = src.UNDERWRITER_USER_ID
            ,tgt.SHARED_BENEFIT_COVERAGE_ID = src.SHARED_BENEFIT_COVERAGE_ID
            ,tgt.COVERAGE_LP_EVENT_CD = src.COVERAGE_LP_EVENT_CD
            ,tgt.COVERAGE_OVERRIDE_CED_RULE_DT = src.COVERAGE_OVERRIDE_CED_RULE_DT
            ,tgt.COVERAGE_DESCRIPTION = src.COVERAGE_DESCRIPTION
            ,tgt.COVERAGE_UW_CLASS_VALUE = src.COVERAGE_UW_CLASS_VALUE
            ,tgt.REVERT_COVERAGE_ID = src.REVERT_COVERAGE_ID
            ,tgt.COVERAGE_EXCHANGE_TYPE_CD = src.COVERAGE_EXCHANGE_TYPE_CD
            ,tgt.COVERAGE_ASSET_PROTECTION_TYPE_CD = src.COVERAGE_ASSET_PROTECTION_TYPE_CD
            ,tgt.COVERAGE_RATE_RULE_VALUE = src.COVERAGE_RATE_RULE_VALUE
            ,tgt.COVERAGE_BUYOUT_AMOUNT = src.COVERAGE_BUYOUT_AMOUNT
            ,tgt.COMPANY_CODE = src.COMPANY_CODE
            ,tgt.CARRIER_NAME = src.CARRIER_NAME
            ,tgt.LOAD_DATE = src.LOAD_DATE
        FROM (
            SELECT * FROM ${SNOW_DB}.INFOMART.STM_COVERAGE_DIM
            WHERE METADATA$ACTION = 'INSERT'
              AND METADATA$ISUPDATE = TRUE
        ) src
        WHERE tgt.COVERAGE_DIM_ID = src.COVERAGE_DIM_ID
          AND tgt.CREATING_EVENT_ID = src.CREATING_EVENT_ID
    `;

    // ---------------------------------------------------------------
    // Step 2a: Expire old current row (Type 2 - new version)
    // ---------------------------------------------------------------
    var step2a_expire_old = `
        UPDATE ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2 tgt
        SET
            tgt.DIM_EXP_DT  = DATEADD(DAY, -1, CAST(src.CREATING_EVENT_DT AS DATE))
            ,tgt.CURRENT_FLG = 'N'
        FROM (
            SELECT * FROM ${SNOW_DB}.INFOMART.STM_COVERAGE_DIM
            WHERE METADATA$ACTION = 'INSERT'
              AND METADATA$ISUPDATE = TRUE
        ) src
        WHERE tgt.COVERAGE_DIM_ID = src.COVERAGE_DIM_ID
          AND tgt.CURRENT_FLG = 'Y'
          AND tgt.CREATING_EVENT_ID != src.CREATING_EVENT_ID
    `;

    // ---------------------------------------------------------------
    // Step 2b: Insert new version as current
    // ---------------------------------------------------------------
    var step2b_insert_new_version = `
        INSERT INTO ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2 (
            COVERAGE_DIM_ID, POLICY_DIM_ID, COVERAGE_ID, POLICY_ID,
            COVERAGE_STATUS_CD, COVERAGE_STATUS_REASON_CD,
            COVERAGE_STATUS_DESCRIPTION, COVERAGE_STATUS_REASON_DESCRIPTION,
            COVERAGE_STATUS_DT, COVERAGE_APPROVAL_DT,
            COVERAGE_EFFECTIVE_DT, COVERAGE_EXPIRATION_DT, COVERAGE_ISSUE_DT,
            CREATING_EVENT_ID, CREATING_ACTIVITY_DESC,
            CREATING_EVENT_ACTY_TRAN_CD, CREATING_EVENT_DT,
            OBSOLETING_LTCAS_EVENT_ID, CORE_BUY_UP_TYPE_CD,
            REISSUE_OF_COVERAGE_ID, AGE_CALC_CD,
            COVERAGE_RATED_AGE_DERIVE_DT, COVERAGE_REIN_COLLECT_TO_DT,
            COVERAGE_CHGE_FROM_APP_FLG, COVERAGE_PROCESS_CD,
            COVERAGE_CHANGE_CD, COVERAGE_NP_CD, COVERAGE_NP_EVENT_CD,
            COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT, COVERAGE_AMENDMENT_NOTICE_SENT_FLG,
            COVERAGE_CHANGE_OF_MIND_FLG, UNDERWRITER_USER_ID,
            SHARED_BENEFIT_COVERAGE_ID, COVERAGE_LP_EVENT_CD,
            COVERAGE_OVERRIDE_CED_RULE_DT, COVERAGE_DESCRIPTION,
            COVERAGE_UW_CLASS_VALUE, REVERT_COVERAGE_ID,
            COVERAGE_EXCHANGE_TYPE_CD, COVERAGE_ASSET_PROTECTION_TYPE_CD,
            COVERAGE_RATE_RULE_VALUE, COVERAGE_BUYOUT_AMOUNT, COMPANY_CODE,
            CARRIER_NAME, LOAD_DATE,
            DIM_EFF_DT, DIM_EXP_DT, CURRENT_FLG
        )
        SELECT
            COVERAGE_DIM_ID, POLICY_DIM_ID, COVERAGE_ID, POLICY_ID,
            COVERAGE_STATUS_CD, COVERAGE_STATUS_REASON_CD,
            COVERAGE_STATUS_DESCRIPTION, COVERAGE_STATUS_REASON_DESCRIPTION,
            COVERAGE_STATUS_DT, COVERAGE_APPROVAL_DT,
            COVERAGE_EFFECTIVE_DT, COVERAGE_EXPIRATION_DT, COVERAGE_ISSUE_DT,
            CREATING_EVENT_ID, CREATING_ACTIVITY_DESC,
            CREATING_EVENT_ACTY_TRAN_CD, CREATING_EVENT_DT,
            OBSOLETING_LTCAS_EVENT_ID, CORE_BUY_UP_TYPE_CD,
            REISSUE_OF_COVERAGE_ID, AGE_CALC_CD,
            COVERAGE_RATED_AGE_DERIVE_DT, COVERAGE_REIN_COLLECT_TO_DT,
            COVERAGE_CHGE_FROM_APP_FLG, COVERAGE_PROCESS_CD,
            COVERAGE_CHANGE_CD, COVERAGE_NP_CD, COVERAGE_NP_EVENT_CD,
            COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT, COVERAGE_AMENDMENT_NOTICE_SENT_FLG,
            COVERAGE_CHANGE_OF_MIND_FLG, UNDERWRITER_USER_ID,
            SHARED_BENEFIT_COVERAGE_ID, COVERAGE_LP_EVENT_CD,
            COVERAGE_OVERRIDE_CED_RULE_DT, COVERAGE_DESCRIPTION,
            COVERAGE_UW_CLASS_VALUE, REVERT_COVERAGE_ID,
            COVERAGE_EXCHANGE_TYPE_CD, COVERAGE_ASSET_PROTECTION_TYPE_CD,
            COVERAGE_RATE_RULE_VALUE, COVERAGE_BUYOUT_AMOUNT, COMPANY_CODE,
            CARRIER_NAME, LOAD_DATE,
            CAST(CREATING_EVENT_DT AS DATE),
            CAST('9999-12-31' AS DATE),
            'Y'
        FROM ${SNOW_DB}.INFOMART.STM_COVERAGE_DIM
        WHERE METADATA$ACTION = 'INSERT'
          AND METADATA$ISUPDATE = TRUE
          AND CREATING_EVENT_ID NOT IN (
              SELECT CREATING_EVENT_ID
              FROM ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2
              WHERE COVERAGE_DIM_ID IN (
                  SELECT COVERAGE_DIM_ID
                  FROM ${SNOW_DB}.INFOMART.STM_COVERAGE_DIM
                  WHERE METADATA$ACTION = 'INSERT'
                    AND METADATA$ISUPDATE = TRUE
              )
          )
    `;

    // ---------------------------------------------------------------
    // Step 3: Insert brand new coverages
    // ---------------------------------------------------------------
    var step3_insert_new_coverage = `
        INSERT INTO ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2 (
            COVERAGE_DIM_ID, POLICY_DIM_ID, COVERAGE_ID, POLICY_ID,
            COVERAGE_STATUS_CD, COVERAGE_STATUS_REASON_CD,
            COVERAGE_STATUS_DESCRIPTION, COVERAGE_STATUS_REASON_DESCRIPTION,
            COVERAGE_STATUS_DT, COVERAGE_APPROVAL_DT,
            COVERAGE_EFFECTIVE_DT, COVERAGE_EXPIRATION_DT, COVERAGE_ISSUE_DT,
            CREATING_EVENT_ID, CREATING_ACTIVITY_DESC,
            CREATING_EVENT_ACTY_TRAN_CD, CREATING_EVENT_DT,
            OBSOLETING_LTCAS_EVENT_ID, CORE_BUY_UP_TYPE_CD,
            REISSUE_OF_COVERAGE_ID, AGE_CALC_CD,
            COVERAGE_RATED_AGE_DERIVE_DT, COVERAGE_REIN_COLLECT_TO_DT,
            COVERAGE_CHGE_FROM_APP_FLG, COVERAGE_PROCESS_CD,
            COVERAGE_CHANGE_CD, COVERAGE_NP_CD, COVERAGE_NP_EVENT_CD,
            COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT, COVERAGE_AMENDMENT_NOTICE_SENT_FLG,
            COVERAGE_CHANGE_OF_MIND_FLG, UNDERWRITER_USER_ID,
            SHARED_BENEFIT_COVERAGE_ID, COVERAGE_LP_EVENT_CD,
            COVERAGE_OVERRIDE_CED_RULE_DT, COVERAGE_DESCRIPTION,
            COVERAGE_UW_CLASS_VALUE, REVERT_COVERAGE_ID,
            COVERAGE_EXCHANGE_TYPE_CD, COVERAGE_ASSET_PROTECTION_TYPE_CD,
            COVERAGE_RATE_RULE_VALUE, COVERAGE_BUYOUT_AMOUNT, COMPANY_CODE,
            CARRIER_NAME, LOAD_DATE,
            DIM_EFF_DT, DIM_EXP_DT, CURRENT_FLG
        )
        SELECT
            COVERAGE_DIM_ID, POLICY_DIM_ID, COVERAGE_ID, POLICY_ID,
            COVERAGE_STATUS_CD, COVERAGE_STATUS_REASON_CD,
            COVERAGE_STATUS_DESCRIPTION, COVERAGE_STATUS_REASON_DESCRIPTION,
            COVERAGE_STATUS_DT, COVERAGE_APPROVAL_DT,
            COVERAGE_EFFECTIVE_DT, COVERAGE_EXPIRATION_DT, COVERAGE_ISSUE_DT,
            CREATING_EVENT_ID, CREATING_ACTIVITY_DESC,
            CREATING_EVENT_ACTY_TRAN_CD, CREATING_EVENT_DT,
            OBSOLETING_LTCAS_EVENT_ID, CORE_BUY_UP_TYPE_CD,
            REISSUE_OF_COVERAGE_ID, AGE_CALC_CD,
            COVERAGE_RATED_AGE_DERIVE_DT, COVERAGE_REIN_COLLECT_TO_DT,
            COVERAGE_CHGE_FROM_APP_FLG, COVERAGE_PROCESS_CD,
            COVERAGE_CHANGE_CD, COVERAGE_NP_CD, COVERAGE_NP_EVENT_CD,
            COVERAGE_OVERRIDE_ENROLLMENT_RULE_DT, COVERAGE_AMENDMENT_NOTICE_SENT_FLG,
            COVERAGE_CHANGE_OF_MIND_FLG, UNDERWRITER_USER_ID,
            SHARED_BENEFIT_COVERAGE_ID, COVERAGE_LP_EVENT_CD,
            COVERAGE_OVERRIDE_CED_RULE_DT, COVERAGE_DESCRIPTION,
            COVERAGE_UW_CLASS_VALUE, REVERT_COVERAGE_ID,
            COVERAGE_EXCHANGE_TYPE_CD, COVERAGE_ASSET_PROTECTION_TYPE_CD,
            COVERAGE_RATE_RULE_VALUE, COVERAGE_BUYOUT_AMOUNT, COMPANY_CODE,
            CARRIER_NAME, LOAD_DATE,
            CAST(CREATING_EVENT_DT AS DATE),
            CAST('9999-12-31' AS DATE),
            'Y'
        FROM ${SNOW_DB}.INFOMART.STM_COVERAGE_DIM
        WHERE METADATA$ACTION = 'INSERT'
          AND METADATA$ISUPDATE = FALSE
          AND COVERAGE_DIM_ID NOT IN (
              SELECT COVERAGE_DIM_ID
              FROM ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2
          )
    `;

    // ---------------------------------------------------------------
    // Step 4: Handle deletes (soft-expire)
    // ---------------------------------------------------------------
    var step4_handle_deletes = `
        UPDATE ${SNOW_DB}.INFOMART.COVERAGE_DIM_SCD2
        SET
            DIM_EXP_DT  = CURRENT_DATE()
            ,CURRENT_FLG = 'N'
        WHERE COVERAGE_DIM_ID IN (
            SELECT COVERAGE_DIM_ID
            FROM ${SNOW_DB}.INFOMART.STM_COVERAGE_DIM
            WHERE METADATA$ACTION = 'DELETE'
              AND METADATA$ISUPDATE = FALSE
        )
        AND CURRENT_FLG = 'Y'
    `;

    // ---------------------------------------------------------------
    // Execute all steps in order
    // ---------------------------------------------------------------
    var stmt1 = snowflake.createStatement({sqlText: step1_update_same_version});
    stmt1.execute();

    var stmt2a = snowflake.createStatement({sqlText: step2a_expire_old});
    stmt2a.execute();

    var stmt2b = snowflake.createStatement({sqlText: step2b_insert_new_version});
    stmt2b.execute();

    var stmt3 = snowflake.createStatement({sqlText: step3_insert_new_coverage});
    stmt3.execute();

    var stmt4 = snowflake.createStatement({sqlText: step4_handle_deletes});
    stmt4.execute();

    return "Coverage DIM SCD2 Incremental Executed Successfully";

$$;


-- ============================================================================
-- STREAM SETUP (Run once after COVERAGE_DIM is updated with new columns)
-- ============================================================================
-- CREATE OR REPLACE STREAM ${SNOW_DB}.INFOMART.STM_COVERAGE_DIM
--     ON TABLE ${SNOW_DB}.INFOMART.COVERAGE_DIM
--     APPEND_ONLY = FALSE;


-- ============================================================================
-- EXECUTION ORDER
-- ============================================================================
--
-- ONE-TIME SETUP:
--   1. Deploy updated sp_coverage_dim_merge (adds 26 columns to COVERAGE_DIM)
--   2. Run sp_coverage_dim_merge to rebuild COVERAGE_DIM with all columns
--   3. Create stream STM_COVERAGE_DIM on COVERAGE_DIM
--   4. Run sp_coverage_dim_scd2_full_load (loads all history into SCD2)
--
-- ONGOING (each refresh cycle):
--   1. Run sp_coverage_dim_merge         (refreshes COVERAGE_DIM - Type 1)
--   2. Run sp_coverage_dim_scd2_incremental (processes stream into SCD2)
--      Step 1: Type 1 update same version in place
--      Step 2a: Expire old current row (new version)
--      Step 2b: Insert new version row
--      Step 3: Insert brand new coverages
--      Step 4: Soft-expire deletes
