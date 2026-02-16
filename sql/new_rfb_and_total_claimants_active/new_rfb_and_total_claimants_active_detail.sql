-- =====================================================================
-- NEW RFB AND TOTAL CLAIMANTS ACTIVE - DETAIL REPORT
-- =====================================================================
-- Session Variables: $CARRIER_NAME, $REPORT_START_DT, $REPORT_END_DT
-- Uses: service_type_by_vendor view
-- =====================================================================

WITH 
-- Policy base data
policy AS (
    SELECT
        policy_id,
        policy_no,
        series_id,
        cert_no,
        life_id
    FROM {{SOURCE_DATABASE}}.dbo.policy
    WHERE last_mod_dt <= $REPORT_END_DT
    QUALIFY ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY last_mod_dt DESC, sequence_no DESC) = 1
),

-- Life demographics with effective date ranges using LEAD() for optimization
life AS (
    SELECT
        life_id,
        life_state,
        life_na_eff_dt,
        life_na_exp_dt,
        life_fname,
        life_mname,
        life_lname,
        CASE
            WHEN $REPORT_START_DT BETWEEN life_na_eff_dt AND life_na_exp_dt THEN 1
            WHEN life_rank = 1 AND life_na_eff_dt > $REPORT_START_DT THEN 1
            ELSE 0
        END AS max_life_effective_flag
    FROM (
        SELECT
            life_id,
            life_state,
            life_na_eff_dt,
            life_fname,
            life_mname,
            life_lname,
            RANK() OVER (PARTITION BY life_id ORDER BY life_na_eff_dt) AS life_rank,
            LEAD(DATEADD(minute, -1, life_na_eff_dt), 1, '9999-12-31') 
                OVER (PARTITION BY life_id ORDER BY life_na_eff_dt) AS life_na_exp_dt
        FROM {{SOURCE_DATABASE}}.dbo.life_demographics
    ) a
    WHERE life_na_exp_dt >= $REPORT_START_DT
),

-- Latest policy series records
policy_series AS (
    SELECT
        series_id,
        filing_state
    FROM {{SOURCE_DATABASE}}.dbo.policy_series
    WHERE sequenced_at <= $REPORT_END_DT
    QUALIFY ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY sequenced_at DESC, sequence_no DESC) = 1
),

-- Latest policy affiliation
policy_affiliation AS (
    SELECT
        series_id,
        cert_no,
        grp_id,
        paf_created_dt,
        paf_eff_dt,
        paf_obsolete_dt,
        paf_exp_dt
    FROM {{SOURCE_DATABASE}}.dbo.policy_affiliation
    WHERE sequenced_at <= $REPORT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY series_id, cert_no, grp_id, life_id, paf_eff_dt, paf_created_dt 
        ORDER BY sequenced_at DESC, sequence_no DESC
    ) = 1
),

-- Latest insurance group
ins_grp AS (
    SELECT
        grp_id,
        ig_1
    FROM {{SOURCE_DATABASE}}.dbo.ins_group
    WHERE last_mod_dt <= $REPORT_END_DT
    QUALIFY ROW_NUMBER() OVER (PARTITION BY grp_id ORDER BY last_mod_dt DESC, sequence_no DESC) = 1
),

-- Policy group information with date filters
policy_grp AS (
    SELECT
        p.series_id,
        TRIM(p.cert_no) AS cert_no,
        p.policy_id,
        p.policy_no,
        ig.grp_id,
        ig.ig_grp_nm,
        ps.filing_state AS issue_state,
        p.life_id
    FROM policy p
    JOIN policy_series ps ON ps.series_id = p.series_id
    JOIN policy_affiliation paf ON TRIM(paf.cert_no) = TRIM(p.cert_no) AND paf.series_id = p.series_id
    JOIN ins_grp ig ON ig.grp_id = paf.grp_id
    WHERE $REPORT_END_DT >= paf.paf_created_dt
        AND $REPORT_END_DT < COALESCE(paf.paf_obsolete_dt, '9999-12-31')
        AND $REPORT_END_DT >= paf.paf_eff_dt
        AND $REPORT_END_DT < COALESCE(paf.paf_exp_dt, '9999-12-31')
),

-- RFB base data
rfb AS (
    SELECT
        series_id,
        cert_no,
        rfb_id,
        rfb_statistical_start_dt,
        rfb_statistical_end_dt
    FROM {{SOURCE_DATABASE}}.dbo.request_for_benefit
    WHERE last_mod_dt <= $REPORT_END_DT
    QUALIFY ROW_NUMBER() OVER (PARTITION BY series_id, cert_no, rfb_id ORDER BY last_mod_dt DESC, sequence_no DESC) = 1
),

-- Episode of benefit
eob AS (
    SELECT
        episode_of_benefit_id,
        rfb_id,
        eb_creation_dt,
        eb_decision_dt,
        eb_start_dt,
        eb_end_dt,
        CASE
            WHEN CONTAINS(e.last_mod_by_user_id, '\\') THEN SUBSTRING(e.last_mod_by_user_id, CHARINDEX('\\', e.last_mod_by_user_id) + 1, LEN(e.last_mod_by_user_id))
            ELSE e.last_mod_by_user_id
        END AS last_mod_by_user_id,
        e.eb_status_cd
    FROM {{SOURCE_DATABASE}}.dbo.episode_of_benefit e
    WHERE last_mod_dt <= $REPORT_END_DT
    QUALIFY ROW_NUMBER() OVER (PARTITION BY episode_of_benefit_id ORDER BY last_mod_dt DESC, e.sequence_no DESC) = 1
),

-- Episode of benefit status
eb_status AS (
    SELECT
        eb_status_cd,
        eb_status_desc
    FROM {{SOURCE_DATABASE}}.dbo.eb_status
    QUALIFY ROW_NUMBER() OVER (PARTITION BY eb_status_cd ORDER BY sequenced_at DESC, sequence_no DESC) = 1
),

-- EOB ranking for latest decisions
eob_ranking AS (
    SELECT
        episode_of_benefit_id,
        rfb_id,
        eb_creation_dt,
        eb_decision_dt,
        eb_start_dt,
        eb_end_dt,
        last_mod_by_user_id,
        ROW_NUMBER() OVER (PARTITION BY rfb_id ORDER BY eb_creation_dt DESC, episode_of_benefit_id DESC) AS min_episode_of_benefit_rank,
        ROW_NUMBER() OVER (PARTITION BY rfb_id ORDER BY eb_decision_dt) AS firstedbdecisiondt_rank,
        MIN(eb_decision_dt) OVER (PARTITION BY rfb_id) AS firstedbdecisiondt,
        es.eb_status_desc,
        es.eb_status_cd
    FROM eob e
    LEFT JOIN eb_status es ON e.eb_status_cd = es.eb_status_cd
),

-- RFB and EOB details
rfb_eob AS (
    SELECT
        r.series_id,
        r.cert_no,
        r.rfb_id,
        e.episode_of_benefit_id,
        r.rfb_statistical_end_dt,
        r.rfb_statistical_start_dt,
        e.eb_status_cd,
        e.eb_status_desc,
        EOB_RANKER,
        FIRSTEDBDECISIONDT_RANK,
        FIRSTEDBDECISIONDT,
        last_mod_by_user_id
    FROM rfb r
    JOIN eob_ranking e ON r.rfb_id = e.rfb_id AND FIRSTEDBDECISIONDT_RANK = 1
    WHERE FIRSTEDBDECISIONDT BETWEEN $REPORT_START_DT AND $REPORT_END_DT
),

-- Initial decisions within the report period
rfb_eob_initial_decisions AS (
    SELECT
        p.series_id,
        p.cert_no,
        p.policy_no,
        p.issue_state,
        l.life_state,
        re.rfb_id,
        re.episode_of_benefit_id,
        re.eb_status_cd,
        re.eb_status_desc,
        re.FIRSTEDBDECISIONDT,
        re.rfb_statistical_start_dt,
        re.last_mod_by_user_id,
        p.grp_nm,
        CASE
            WHEN FIRSTEDBDECISIONDT >= RFB_STATISTICAL_START_DT
            THEN ({{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.reference.FM_WORKINGDAYSBETWEEN(RFB_STATISTICAL_START_DT, FIRSTEDBDECISIONDT))
            ELSE 0 
        END AS WORK_DAYS_BETWEEN,
        DATEDIFF(DAY, RFB_STATISTICAL_START_DT, FIRSTEDBDECISIONDT) AS CalendarDaysBetween,
        CASE WHEN WORK_DAYS_BETWEEN < 14 THEN 1 ELSE 0 END AS NumDaysResolvedWithinTwoWeeks
    FROM policy_grp p
    INNER JOIN rfb_eob re ON p.rfb_id = re.rfb_id
    INNER JOIN life l ON l.life_id = p.life_id AND l.max_life_effective_flag = 1
),

-- Policies missing from initial decisions
missing_policy AS (
    SELECT
        TRIM(a.policy_no) AS policy_no,
        grp_nm,
        LIFE_STATE,
        issue_state,
        a.rfb_id,
        TO_DATE(a.cms_end_dt) AS eob_date
    FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.service_type_by_vendor_stage a
    WHERE i=1
        AND a.contracted_service_id IN (28, 31, 48, 47, 77)
        AND a.cms_end_dt BETWEEN TO_DATE($REPORT_START_DT) AND TO_DATE($REPORT_END_DT)
        AND trim(a.policy_no) NOT IN (SELECT trim(policy_no) FROM rfb_eob_initial_decisions)
),

-- Reopen after closed cases
closed_after_reopen AS (
    SELECT
        e.eb_status_cd,
        e.eb_status_desc,
        mp.policy_no,
        mp.grp_nm,
        CASE 
            WHEN FIRSTEDBDECISIONDT >= RFB_STATISTICAL_START_DT
            THEN dev_snowflake_warehouse.reference.FM_WORKINGDAYSBETWEEN(RFB_STATISTICAL_START_DT, FIRSTEDBDECISIONDT)
            ELSE 0 
        END AS WORK_DAYS_BETWEEN,
        CASE WHEN WORK_DAYS_BETWEEN < 14 THEN 1 ELSE 0 END AS NumDaysResolvedWithinTwoWeeks,
        r.rfb_statistical_start_dt,
        r.last_mod_by_user_id,
        r.life_state,
        r.issue_state,
        r.FIRSTEDBDECISIONDT
    FROM missing_policy mp
    JOIN rfb r ON r.rfb_id = mp.rfb_id
    JOIN eob_ranking e ON r.rfb_id = e.rfb_id AND EOB_RANKER = 1
),

-- Final SELECT combining all data
final AS (
    SELECT
        eb_status_cd AS status_cd,
        eb_status_desc AS "Status",
        policy_no AS "Policy Number",
        grp_nm AS "Insurance Group",
        NULL AS "Ext Ref",
        CASE 
            WHEN WORK_DAYS_BETWEEN > 0 THEN WORK_DAYS_BETWEEN - 1 
            ELSE WORK_DAYS_BETWEEN 
        END AS DAYS,
        TO_CHAR(rfb_statistical_start_dt, 'MM/DD/YYYY') AS "Statistical Start Date",
        TO_CHAR(FIRSTEDBDECISIONDT, 'MM/DD/YYYY') AS "EDB Decision Date",
        last_mod_by_user_id AS "Modified By",
        life_state AS "Residence State",
        issue_state AS "Issue State",
        NumDaysResolvedWithinTwoWeeks,
        $CARRIER_NAME AS carrier_name,
        TO_DATE($REPORT_START_DT) AS report_start_dt,
        TO_DATE($REPORT_END_DT) AS report_end_dt
    FROM rfb_eob_initial_decisions
    
    UNION
    
    SELECT
        eb_status_cd AS status_cd,
        eb_status_desc AS "Status",
        policy_no AS "Policy Number",
        grp_nm AS "Insurance Group",
        NULL AS "Ext Ref",
        CASE 
            WHEN WORK_DAYS_BETWEEN > 0 THEN WORK_DAYS_BETWEEN - 1 
            ELSE WORK_DAYS_BETWEEN 
        END AS DAYS,
        TO_CHAR(rfb_statistical_start_dt, 'MM/DD/YYYY') AS "Statistical Start Date",
        TO_CHAR(FIRSTEDBDECISIONDT, 'MM/DD/YYYY') AS "EDB Decision Date",
        last_mod_by_user_id AS "Modified By",
        life_state AS "Residence State",
        issue_state AS "Issue State",
        NumDaysResolvedWithinTwoWeeks,
        $CARRIER_NAME AS carrier_name,
        TO_DATE($REPORT_START_DT) AS report_start_dt,
        TO_DATE($REPORT_END_DT) AS report_end_dt
    FROM closed_after_reopen
)

SELECT * FROM final;
