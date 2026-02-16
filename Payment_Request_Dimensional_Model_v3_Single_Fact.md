# Dimensional Model v3: Single Fact — Timespan Accumulating Snapshot
## Payment Request Claims Processing (CDC Source)

---

## Design Pattern: Timespan Accumulating Snapshot

**Kimball Ch. 16, pp. 394-395:**

> *"An accumulating snapshot does a great job presenting a workflow's current state, but it obliterates the intermediate states... Alternatively, you could add effective and expiration dates to the accumulating snapshot. In this scenario, instead of destructively updating each row as changes occur, you add a new row that preserves the state of a claim for a span of time."*

> *"Similar to a type 2 slowly changing dimension, the fact row includes: Snapshot start date, Snapshot end date (updated when a new row is added), Snapshot current flag (updated when a new row is added)."*

> *"Most users are only interested in the current view... by defining a view that filters based on the current flag. The minority of users who need to look at the pipeline as of any arbitrary date in the past can do so by filtering on the snapshot start and end dates."*

**This is your single fact table.** It behaves like SCD Type 2 applied to fact rows. Instead of destructively updating the row, you expire the old row and insert a new one. Every historical state is preserved. One table serves daily, weekly, monthly, and ad-hoc historical reporting — all by passing date parameters.

---

## How It Works

```
Traditional Accumulating Snapshot (DESTROYS history):
═══════════════════════════════════════════════════════
  Jan 15: INSERT row for request #100  (status=New)
  Jan 20: UPDATE same row              (status=Detail Added)   ← old state LOST
  Feb 10: UPDATE same row              (status=Complete)       ← old state LOST

  Result: 1 row. Only shows "Complete". Cannot recreate Jan state.


Timespan Accumulating Snapshot (PRESERVES history):
═══════════════════════════════════════════════════════
  Jan 15: INSERT row for request #100
            ROW_EFFECTIVE_DT = Jan 15
            ROW_EXPIRY_DT    = 9999-12-31 (open-ended)
            CURRENT_ROW_FLG  = 'Y'
            STATUS           = 'Request Only'

  Jan 20: Detail arrives →
            EXPIRE old row:  ROW_EXPIRY_DT = Jan 19, CURRENT_ROW_FLG = 'N'
            INSERT new row:  ROW_EFFECTIVE_DT = Jan 20
                             ROW_EXPIRY_DT    = 9999-12-31
                             CURRENT_ROW_FLG  = 'Y'
                             STATUS           = 'Request + Detail'

  Feb 10: Service detail arrives →
            EXPIRE old row:  ROW_EXPIRY_DT = Feb 9, CURRENT_ROW_FLG = 'N'
            INSERT new row:  ROW_EFFECTIVE_DT = Feb 10
                             ROW_EXPIRY_DT    = 9999-12-31
                             CURRENT_ROW_FLG  = 'Y'
                             STATUS           = 'Complete'

  Result: 3 rows for request #100. Full history preserved.

  ┌──────────┬──────────┬──────────┬─────┬──────────────────────┐
  │ EFF_DT   │ EXP_DT   │ CURRENT  │ AMT │ STATUS               │
  ├──────────┼──────────┼──────────┼─────┼──────────────────────┤
  │ Jan 15   │ Jan 19   │ N        │   0 │ Request Only         │
  │ Jan 20   │ Feb 09   │ N        │5000 │ Request + Detail     │
  │ Feb 10   │ 9999-12-31│ Y       │4200 │ Complete             │
  └──────────┴──────────┴──────────┴─────┴──────────────────────┘
```

---

## Reporting With Date Parameters — Single Fact, All Cadences

### Daily Report (e.g., "Jan 25 report")
```sql
WHERE ROW_EFFECTIVE_DT <= '2025-01-25'
  AND ROW_EXPIRY_DT   >= '2025-01-25'
```
Returns the row that was active on Jan 25 → "Request + Detail, $5,000"

### Weekly Report (e.g., "Week ending Jan 19")
```sql
WHERE ROW_EFFECTIVE_DT <= '2025-01-19'
  AND ROW_EXPIRY_DT   >= '2025-01-19'
```
Returns the row that was active on Jan 19 → "Request Only, $0"

### Monthly Report (e.g., "December 2025" run in Feb 2026)
```sql
WHERE ROW_EFFECTIVE_DT <= '2025-12-31'
  AND ROW_EXPIRY_DT   >= '2025-12-31'
```
Returns exactly one row per entity — the state as of Dec 31. Same numbers every time, forever.

### Current State (operational dashboard)
```sql
WHERE CURRENT_ROW_FLG = 'Y'
```
Equivalent to a traditional accumulating snapshot. Fast.

**One table. One pattern. All reporting needs.**

---

## Grain Declaration

> **One row per PAYMENT_REQUEST_DETAIL per effective time period.**

When service details arrive, they enrich the existing row (expire + insert new version). If multiple service details map to one payment_request_detail, each gets its own tracked row at the service_detail grain.

In other words:
- Before service details exist: grain = 1 row per PAYMENT_REQUEST_DETAIL per time period
- After service details arrive: grain = 1 row per SERVICE_DETAIL per time period

This is clean because SERVICE_DETAIL is always a child of PAYMENT_REQUEST_DETAIL.

---

## Fact Table Schema

### FACT_PAYMENT_SERVICE (Timespan Accumulating Snapshot)

```sql
CREATE OR REPLACE TABLE DW.FACT_PAYMENT_SERVICE (

    -- ═══════════════════════════════════════════════════════════
    -- SURROGATE KEY
    -- ═══════════════════════════════════════════════════════════
    FACT_SK                     NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,

    -- ═══════════════════════════════════════════════════════════
    -- TIMESPAN COLUMNS (the SCD Type 2 for facts)
    -- These three columns make history queryable
    -- ═══════════════════════════════════════════════════════════
    ROW_EFFECTIVE_DT            DATE NOT NULL,        -- when this version became active
    ROW_EXPIRY_DT               DATE NOT NULL DEFAULT '9999-12-31',  -- when superseded
    CURRENT_ROW_FLG             CHAR(1) NOT NULL DEFAULT 'Y',        -- Y = latest version

    -- ═══════════════════════════════════════════════════════════
    -- ROLE-PLAYING DATE DIMENSION FKs
    -- (single physical DIM_DATE, multiple views)
    -- DEFAULT -1 = "Not Yet Known / TBD"
    -- ═══════════════════════════════════════════════════════════
    PAYREQ_RECEIVED_DATE_KEY    INT NOT NULL DEFAULT -1,
    PAYREQ_COMPLETE_DATE_KEY    INT NOT NULL DEFAULT -1,
    PAYREQ_DUE_DATE_KEY         INT NOT NULL DEFAULT -1,
    PAYREQ_STAT_START_DATE_KEY  INT NOT NULL DEFAULT -1,
    PAYREQ_STAT_END_DATE_KEY    INT NOT NULL DEFAULT -1,
    FAC_STMT_SIGN_DATE_KEY      INT NOT NULL DEFAULT -1,
    PRD_SERVICE_START_DATE_KEY  INT NOT NULL DEFAULT -1,
    PRD_SERVICE_END_DATE_KEY    INT NOT NULL DEFAULT -1,
    SD_SERVICE_START_DATE_KEY   INT NOT NULL DEFAULT -1,
    SD_SERVICE_END_DATE_KEY     INT NOT NULL DEFAULT -1,
    SD_OK_DECISION_DATE_KEY     INT NOT NULL DEFAULT -1,
    SD_ENTERED_DATE_KEY         INT NOT NULL DEFAULT -1,

    -- ═══════════════════════════════════════════════════════════
    -- ENTITY DIMENSION FKs
    -- DEFAULT -1 = "Not Applicable / Unknown"
    -- ═══════════════════════════════════════════════════════════
    PAYEE_KEY                   INT NOT NULL DEFAULT -1,
    USER_KEY                    INT NOT NULL DEFAULT -1,
    INTAKE_USER_KEY             INT NOT NULL DEFAULT -1,
    LAST_MOD_USER_KEY           INT NOT NULL DEFAULT -1,
    SERVICE_PROVIDER_KEY        INT NOT NULL DEFAULT -1,
    SVC_PROVIDER_CAREGIVER_KEY  INT NOT NULL DEFAULT -1,
    SERVICE_TYPE_KEY            INT NOT NULL DEFAULT -1,
    BENEFIT_TYPE_KEY            INT NOT NULL DEFAULT -1,
    POOL_OPTION_KEY             INT NOT NULL DEFAULT -1,
    LOCATION_OF_CARE_KEY        INT NOT NULL DEFAULT -1,
    COMPLETE_REASON_KEY         INT NOT NULL DEFAULT -1,
    OK_REASON_KEY               INT NOT NULL DEFAULT -1,
    COMPANY_KEY                 INT NOT NULL DEFAULT -1,

    -- ═══════════════════════════════════════════════════════════
    -- JUNK DIMENSION FKs
    -- ═══════════════════════════════════════════════════════════
    PAYREQ_STATUS_JUNK_KEY      INT NOT NULL DEFAULT -1,
    SD_STATUS_JUNK_KEY          INT NOT NULL DEFAULT -1,

    -- ═══════════════════════════════════════════════════════════
    -- STATUS DIMENSION FK
    -- ═══════════════════════════════════════════════════════════
    REQUEST_STATUS_KEY          INT NOT NULL,

    -- ═══════════════════════════════════════════════════════════
    -- AUDIT DIMENSION FK
    -- ═══════════════════════════════════════════════════════════
    AUDIT_KEY                   INT NOT NULL,

    -- ═══════════════════════════════════════════════════════════
    -- DEGENERATE DIMENSIONS
    -- ═══════════════════════════════════════════════════════════
    PAYMENT_REQUEST_ID          NUMBER(38,0) NOT NULL,
    PAYMENT_REQUEST_DETAIL_ID   NUMBER(38,0) NOT NULL DEFAULT -1,
    SERVICE_DETAIL_ID           NUMBER(38,0) NOT NULL DEFAULT -1,
    DOC_ID                      NUMBER(38,0),
    PAYREQ_REFERENCE            VARCHAR(20),
    PRD_REFERENCE               VARCHAR(10),
    CERT_NO                     VARCHAR(15),
    SERIES_ID                   NUMBER(38,0),
    BENEFIT_ADJUSTMENT_ID       NUMBER(38,0),
    BENEFIT_PAYMENT_ID          NUMBER(38,0),
    REVERSED_SERVICE_DETAIL_ID  NUMBER(38,0),
    SD_PARENT_SERVICE_DETAIL_ID NUMBER(38,0),

    -- ═══════════════════════════════════════════════════════════
    -- ADDITIVE FACTS
    -- ═══════════════════════════════════════════════════════════
    PRD_REQUESTED_AMOUNT        NUMBER(12,2),
    PRD_SERVICE_UNITS           FLOAT,
    PRD_PER_UNIT_AMOUNT         NUMBER(20,8),     -- non-additive (unit price)
    SD_AMOUNT                   NUMBER(12,2),
    SD_SERVICE_UNITS            FLOAT,

    -- ═══════════════════════════════════════════════════════════
    -- LAG FACTS (in days, calculated at row creation)
    -- ═══════════════════════════════════════════════════════════
    RECEIVED_TO_COMPLETE_LAG    INT,
    RECEIVED_TO_DUE_LAG         INT,
    SERVICE_TO_DECISION_LAG     INT,
    ENTERED_TO_DECISION_LAG     INT,
    RECEIVED_TO_FIRST_SD_LAG    INT,

    -- ═══════════════════════════════════════════════════════════
    -- MILESTONE COMPLETION COUNTERS (0 or 1)
    -- Useful for aggregation: SUM(IS_COMPLETE) = count of complete
    -- ═══════════════════════════════════════════════════════════
    IS_DETAIL_ATTACHED          TINYINT NOT NULL DEFAULT 0,
    IS_SERVICE_DETAIL_ATTACHED  TINYINT NOT NULL DEFAULT 0,
    IS_DECISION_MADE            TINYINT NOT NULL DEFAULT 0,
    IS_COMPLETE                 TINYINT NOT NULL DEFAULT 0,
    IS_VOIDED                   TINYINT NOT NULL DEFAULT 0,

    -- ═══════════════════════════════════════════════════════════
    -- DATA COMPLETENESS LEVEL
    -- ═══════════════════════════════════════════════════════════
    DATA_COMPLETENESS_LEVEL     VARCHAR(30) NOT NULL DEFAULT 'REQUEST_ONLY'
        -- 'REQUEST_ONLY' | 'REQUEST_AND_DETAIL' | 'FULL'
);
```

---

## Convenience Views

Per Kimball: *"Most users are only interested in the current view... by defining a view that filters based on the current flag."*

### View 1: Current State (replaces traditional accumulating snapshot)
```sql
CREATE OR REPLACE VIEW DW.VW_FACT_PAYMENT_SERVICE_CURRENT AS
SELECT *
FROM DW.FACT_PAYMENT_SERVICE
WHERE CURRENT_ROW_FLG = 'Y';
```
Use for: Daily operational dashboards, current pipeline status.

### View 2: As-Of Any Date (historical point-in-time)
```sql
-- Parameterized in your BI tool / stored proc:
CREATE OR REPLACE VIEW DW.VW_FACT_PAYMENT_SERVICE_AS_OF AS
SELECT *
FROM DW.FACT_PAYMENT_SERVICE
WHERE ROW_EFFECTIVE_DT <= CURRENT_DATE()
  AND ROW_EXPIRY_DT   >= CURRENT_DATE();

-- For ad-hoc: pass @as_of_date parameter
-- WHERE ROW_EFFECTIVE_DT <= @as_of_date
--   AND ROW_EXPIRY_DT   >= @as_of_date
```

---

## Late-Arriving Service Details — Full Walkthrough

```
═══════════════════════════════════════════════════════════════════
EVENT 1: Jan 15 — PAYMENT_REQUEST #100 arrives (no details yet)
═══════════════════════════════════════════════════════════════════

INSERT INTO FACT_PAYMENT_SERVICE:
  FACT_SK = 1
  ROW_EFFECTIVE_DT       = 2025-01-15
  ROW_EXPIRY_DT          = 9999-12-31
  CURRENT_ROW_FLG        = 'Y'
  PAYMENT_REQUEST_ID     = 100
  PAYMENT_REQUEST_DETAIL_ID = -1          ← not yet known
  SERVICE_DETAIL_ID      = -1             ← not yet known
  PAYREQ_RECEIVED_DATE_KEY = 20250115
  PRD_SERVICE_START_DATE_KEY = -1         ← TBD
  SD_SERVICE_START_DATE_KEY  = -1         ← TBD
  PRD_REQUESTED_AMOUNT   = NULL
  SD_AMOUNT              = NULL
  REQUEST_STATUS_KEY     → 'Request Only - Awaiting Details'
  DATA_COMPLETENESS      = 'REQUEST_ONLY'
  IS_DETAIL_ATTACHED     = 0
  IS_SERVICE_DETAIL_ATTACHED = 0

═══════════════════════════════════════════════════════════════════
EVENT 2: Jan 20 — PAYMENT_REQUEST_DETAIL #200 arrives
═══════════════════════════════════════════════════════════════════

Step A: Expire current row
  UPDATE FACT_PAYMENT_SERVICE
  SET ROW_EXPIRY_DT = '2025-01-19',
      CURRENT_ROW_FLG = 'N'
  WHERE PAYMENT_REQUEST_ID = 100
    AND CURRENT_ROW_FLG = 'Y';

Step B: Insert new version with detail data merged in
  INSERT INTO FACT_PAYMENT_SERVICE:
    FACT_SK = 2
    ROW_EFFECTIVE_DT       = 2025-01-20
    ROW_EXPIRY_DT          = 9999-12-31
    CURRENT_ROW_FLG        = 'Y'
    PAYMENT_REQUEST_ID     = 100
    PAYMENT_REQUEST_DETAIL_ID = 200        ← now known
    SERVICE_DETAIL_ID      = -1            ← still unknown
    PAYREQ_RECEIVED_DATE_KEY = 20250115    ← carried forward
    PRD_SERVICE_START_DATE_KEY = 20250118  ← from detail
    PRD_REQUESTED_AMOUNT   = 5000.00       ← from detail
    SD_AMOUNT              = NULL           ← still unknown
    REQUEST_STATUS_KEY     → 'Detail Attached - Awaiting Service'
    DATA_COMPLETENESS      = 'REQUEST_AND_DETAIL'
    IS_DETAIL_ATTACHED     = 1
    IS_SERVICE_DETAIL_ATTACHED = 0

═══════════════════════════════════════════════════════════════════
EVENT 3: Feb 10 — SERVICE_DETAIL #300 arrives
═══════════════════════════════════════════════════════════════════

Step A: Expire current row
  UPDATE FACT_PAYMENT_SERVICE
  SET ROW_EXPIRY_DT = '2025-02-09',
      CURRENT_ROW_FLG = 'N'
  WHERE PAYMENT_REQUEST_DETAIL_ID = 200
    AND CURRENT_ROW_FLG = 'Y';

Step B: Insert new version with all data merged
  INSERT INTO FACT_PAYMENT_SERVICE:
    FACT_SK = 3
    ROW_EFFECTIVE_DT       = 2025-02-10
    ROW_EXPIRY_DT          = 9999-12-31
    CURRENT_ROW_FLG        = 'Y'
    PAYMENT_REQUEST_ID     = 100
    PAYMENT_REQUEST_DETAIL_ID = 200
    SERVICE_DETAIL_ID      = 300           ← now known
    PAYREQ_RECEIVED_DATE_KEY = 20250115    ← carried forward
    PRD_SERVICE_START_DATE_KEY = 20250118  ← carried forward
    SD_SERVICE_START_DATE_KEY = 20250205   ← from SD
    PRD_REQUESTED_AMOUNT   = 5000.00       ← carried forward
    SD_AMOUNT              = 4200.00       ← from SD
    REQUEST_STATUS_KEY     → 'Complete'
    DATA_COMPLETENESS      = 'FULL'
    IS_DETAIL_ATTACHED     = 1
    IS_SERVICE_DETAIL_ATTACHED = 1
    IS_COMPLETE            = 1
    RECEIVED_TO_FIRST_SD_LAG = 26          ← Feb 10 - Jan 15
    RECEIVED_TO_COMPLETE_LAG = 26

═══════════════════════════════════════════════════════════════════
FINAL STATE — 3 rows in fact table for request #100:
═══════════════════════════════════════════════════════════════════

┌────┬────────────┬────────────┬─────┬───────┬──────┬──────────────────────────┐
│ SK │ EFF_DT     │ EXP_DT     │ CUR │ REQ$  │ SD$  │ STATUS                   │
├────┼────────────┼────────────┼─────┼───────┼──────┼──────────────────────────┤
│  1 │ 2025-01-15 │ 2025-01-19 │ N   │  NULL │ NULL │ Request Only             │
│  2 │ 2025-01-20 │ 2025-02-09 │ N   │ 5,000 │ NULL │ Detail - Awaiting Svc    │
│  3 │ 2025-02-10 │ 9999-12-31 │ Y   │ 5,000 │ 4,200│ Complete                 │
└────┴────────────┴────────────┴─────┴───────┴──────┴──────────────────────────┘
```

---

## All Reporting Cadences — Same Table, Same Pattern

### Daily Report — "Show me state as of Jan 18, 2025"
```sql
SELECT
    rs.STATUS_DESCRIPTION,
    COUNT(*)                         AS request_count,
    SUM(f.PRD_REQUESTED_AMOUNT)      AS total_requested,
    SUM(f.SD_AMOUNT)                 AS total_sd_amount,
    SUM(f.IS_DETAIL_ATTACHED)        AS with_details,
    SUM(f.IS_SERVICE_DETAIL_ATTACHED) AS with_service,
    SUM(f.IS_COMPLETE)               AS completed
FROM DW.FACT_PAYMENT_SERVICE f
JOIN DW.DIM_REQUEST_STATUS rs ON f.REQUEST_STATUS_KEY = rs.REQUEST_STATUS_KEY
WHERE f.ROW_EFFECTIVE_DT <= '2025-01-18'
  AND f.ROW_EXPIRY_DT   >= '2025-01-18'
GROUP BY rs.STATUS_DESCRIPTION;

-- Returns request #100 as "Request Only", $0 — correct for Jan 18
```

### Weekly Report — "Week ending Jan 26, 2025"
```sql
SELECT
    rs.STATUS_DESCRIPTION,
    COUNT(*)                         AS request_count,
    SUM(f.PRD_REQUESTED_AMOUNT)      AS total_requested,
    SUM(f.SD_AMOUNT)                 AS total_sd_amount
FROM DW.FACT_PAYMENT_SERVICE f
JOIN DW.DIM_REQUEST_STATUS rs ON f.REQUEST_STATUS_KEY = rs.REQUEST_STATUS_KEY
WHERE f.ROW_EFFECTIVE_DT <= '2025-01-26'
  AND f.ROW_EXPIRY_DT   >= '2025-01-26'
GROUP BY rs.STATUS_DESCRIPTION;

-- Returns request #100 as "Detail Attached", $5,000 — correct for Jan 26
```

### Monthly Report — "December 2025" (run in Feb 2026)
```sql
SELECT
    d.MONTH_NAME,
    d.YEAR,
    rs.STATUS_DESCRIPTION,
    COUNT(*)                         AS request_count,
    SUM(f.PRD_REQUESTED_AMOUNT)      AS total_requested,
    SUM(f.SD_AMOUNT)                 AS total_sd_amount,
    SUM(f.IS_COMPLETE)               AS completed_count,
    AVG(f.RECEIVED_TO_COMPLETE_LAG)  AS avg_completion_days
FROM DW.FACT_PAYMENT_SERVICE f
JOIN DW.DIM_REQUEST_STATUS rs ON f.REQUEST_STATUS_KEY = rs.REQUEST_STATUS_KEY
JOIN DW.DIM_DATE d ON d.DATE_KEY = 20251231
WHERE f.ROW_EFFECTIVE_DT <= '2025-12-31'
  AND f.ROW_EXPIRY_DT   >= '2025-12-31'
GROUP BY d.MONTH_NAME, d.YEAR, rs.STATUS_DESCRIPTION;

-- Returns state as of Dec 31, 2025. Same result today, tomorrow, next year.
```

### Year-over-Year — "Dec 2025 vs Dec 2024"
```sql
WITH dec_2024 AS (
    SELECT * FROM DW.FACT_PAYMENT_SERVICE
    WHERE ROW_EFFECTIVE_DT <= '2024-12-31'
      AND ROW_EXPIRY_DT   >= '2024-12-31'
),
dec_2025 AS (
    SELECT * FROM DW.FACT_PAYMENT_SERVICE
    WHERE ROW_EFFECTIVE_DT <= '2025-12-31'
      AND ROW_EXPIRY_DT   >= '2025-12-31'
)
SELECT '2024' AS year, COUNT(*) AS cnt, SUM(SD_AMOUNT) AS total FROM dec_2024
UNION ALL
SELECT '2025', COUNT(*), SUM(SD_AMOUNT) FROM dec_2025;
```

### Current Dashboard
```sql
SELECT *
FROM DW.VW_FACT_PAYMENT_SERVICE_CURRENT;  -- just WHERE CURRENT_ROW_FLG = 'Y'
```

---

## ETL Logic — CDC Processing

```
FOR EACH CDC EVENT (ordered by SEQUENCED_AT):
│
├── 1. Process dimensions (SCD Type 2 lookups)
│
├── 2. Determine affected fact row
│      FIND current row WHERE:
│        - PAYMENT_REQUEST_ID matches (for PR events)
│        - PAYMENT_REQUEST_DETAIL_ID matches (for PRD events)
│        - SERVICE_DETAIL_ID matches (for SD events)
│        AND CURRENT_ROW_FLG = 'Y'
│
├── 3. If FOUND:
│      ├── EXPIRE the current row:
│      │     UPDATE SET ROW_EXPIRY_DT = CDC_EVENT_DATE - 1,
│      │                CURRENT_ROW_FLG = 'N'
│      │
│      └── INSERT new row:
│            - Copy ALL columns from expired row (carry forward)
│            - Overlay changed columns from CDC event
│            - SET ROW_EFFECTIVE_DT = CDC_EVENT_DATE
│            - SET ROW_EXPIRY_DT = '9999-12-31'
│            - SET CURRENT_ROW_FLG = 'Y'
│            - Recalculate lags
│            - Update milestone counters
│            - Update REQUEST_STATUS_KEY
│            - Update DATA_COMPLETENESS_LEVEL
│
├── 4. If NOT FOUND (new entity):
│      └── INSERT brand new row:
│            - SET ROW_EFFECTIVE_DT = CDC_EVENT_DATE
│            - SET ROW_EXPIRY_DT = '9999-12-31'
│            - SET CURRENT_ROW_FLG = 'Y'
│            - Unknown FKs → -1 (TBD)
│            - Unknown facts → NULL
│
└── 5. Handle DML_OPERATION = 'DELETE':
       ├── EXPIRE the current row
       └── INSERT new row with REQUEST_STATUS_KEY → 'Deleted/Voided'
           IS_VOIDED = 1
```

### Multiple Service Details per Payment Request Detail

```
When PAYMENT_REQUEST_DETAIL #200 has multiple SERVICE_DETAILs:

  SD #300 arrives (first):
    → Expire PRD-level row (FACT_SK=2)
    → Insert row at SD grain: SD_ID=300, carries PRD+PR data

  SD #301 arrives (second):
    → Does NOT touch the SD #300 row
    → INSERT a brand new row: SD_ID=301, carries same PRD+PR data
    → Each SD gets its own independent lifecycle tracking

  SD #300 gets updated later:
    → Expire the current SD #300 row
    → Insert new version of SD #300 row
    → SD #301 row is untouched
```

---

## Indexing and Performance (Snowflake)

```sql
-- Snowflake clustering for the dominant query pattern
ALTER TABLE DW.FACT_PAYMENT_SERVICE
  CLUSTER BY (CURRENT_ROW_FLG, ROW_EFFECTIVE_DT);

-- Why this clustering?
-- Pattern 1: Current state → CURRENT_ROW_FLG = 'Y' (most common)
-- Pattern 2: As-of date   → ROW_EFFECTIVE_DT <= X AND ROW_EXPIRY_DT >= X
-- Snowflake micro-partitions will naturally prune on these columns
```

For non-Snowflake platforms, create:
```sql
-- Covering index for current-state queries
CREATE INDEX idx_fact_current ON FACT_PAYMENT_SERVICE (CURRENT_ROW_FLG)
  WHERE CURRENT_ROW_FLG = 'Y';  -- partial/filtered index

-- Index for as-of-date queries
CREATE INDEX idx_fact_timespan ON FACT_PAYMENT_SERVICE
  (ROW_EFFECTIVE_DT, ROW_EXPIRY_DT);

-- Index for finding the row to expire during ETL
CREATE INDEX idx_fact_etl_lookup ON FACT_PAYMENT_SERVICE
  (PAYMENT_REQUEST_DETAIL_ID, CURRENT_ROW_FLG)
  WHERE CURRENT_ROW_FLG = 'Y';
```

---

## Row Growth Estimation

```
Row growth per entity = number of CDC events for that entity

Example:
  - Average payment request goes through 4 state changes
  - 1 million payment requests per year
  - Rows per year: ~4 million

  - Each row ≈ 500 bytes
  - Annual storage: ~2 GB (trivial for Snowflake)

  - After 5 years: ~20 million rows, ~10 GB
  - Still very manageable

Compared to periodic snapshot approach:
  - Monthly snapshot: 1M requests × 12 months = 12M rows/year
  - Daily snapshot: 1M requests × 365 days = 365M rows/year
  - Timespan approach is FAR more storage-efficient
    because rows are only created when state ACTUALLY changes
```

---

## Dimension Tables

### DIM_DATE (Role-Playing)

```sql
CREATE OR REPLACE TABLE DW.DIM_DATE (
    DATE_KEY        INT PRIMARY KEY,        -- YYYYMMDD
    FULL_DATE       DATE NOT NULL,
    DAY_OF_WEEK     VARCHAR(10),
    DAY_OF_MONTH    INT,
    DAY_OF_YEAR     INT,
    WEEK_OF_YEAR    INT,
    MONTH_NUMBER    INT,
    MONTH_NAME      VARCHAR(15),
    QUARTER_NUMBER  INT,
    QUARTER_NAME    VARCHAR(5),
    YEAR            INT,
    FISCAL_YEAR     INT,
    FISCAL_QUARTER  INT,
    IS_WEEKEND      CHAR(1),
    IS_HOLIDAY      CHAR(1),
    HOLIDAY_NAME    VARCHAR(50),
    IS_MONTH_END    CHAR(1),
    IS_QUARTER_END  CHAR(1),
    IS_YEAR_END     CHAR(1)
);

-- Special rows
INSERT INTO DIM_DATE VALUES (-1, '0001-01-01', 'TBD', ...);       -- Not Yet Known
INSERT INTO DIM_DATE VALUES (-2, '0001-01-02', 'N/A', ...);       -- Not Applicable
INSERT INTO DIM_DATE VALUES (-3, '0001-01-03', 'MISSING', ...);   -- Data Missing
```

12+ role-playing views: `DIM_PAYREQ_RECEIVED_DATE`, `DIM_PAYREQ_COMPLETE_DATE`, `DIM_PRD_SERVICE_START_DATE`, `DIM_SD_SERVICE_START_DATE`, `DIM_SD_OK_DECISION_DATE`, etc.

### DIM_REQUEST_STATUS

```sql
CREATE OR REPLACE TABLE DW.DIM_REQUEST_STATUS (
    REQUEST_STATUS_KEY  INT PRIMARY KEY AUTOINCREMENT,
    STATUS_CODE         VARCHAR(10) NOT NULL,
    STATUS_DESCRIPTION  VARCHAR(100) NOT NULL,
    STATUS_CATEGORY     VARCHAR(30) NOT NULL,    -- OPEN / IN_PROGRESS / COMPLETE / VOIDED
    IS_TERMINAL         CHAR(1) NOT NULL,
    DISPLAY_ORDER       INT NOT NULL
);

INSERT INTO DIM_REQUEST_STATUS VALUES
  (1, 'REQ_ONLY',  'Request Only - Awaiting Details',          'OPEN',        'N', 10),
  (2, 'REQ_DTL',   'Request + Detail - Awaiting Service',      'OPEN',        'N', 20),
  (3, 'REQ_SVC',   'Request + Detail + Service - In Review',   'IN_PROGRESS', 'N', 30),
  (4, 'DECISION',  'Decision Made - Awaiting Completion',      'IN_PROGRESS', 'N', 40),
  (5, 'COMPLETE',  'Complete',                                  'COMPLETE',    'Y', 50),
  (6, 'VOIDED',    'Voided',                                    'VOIDED',      'Y', 60),
  (7, 'REVERSED',  'Reversed',                                  'VOIDED',      'Y', 70);
```

### DIM_PAYEE, DIM_USER, DIM_SERVICE_PROVIDER

Standard SCD Type 2 dimensions with surrogate keys. Same as v1 design.

### DIM_SERVICE_TYPE

Conformed across PAYMENT_REQUEST_DETAIL and SERVICE_DETAIL. Flattened hierarchy.

### DIM_JUNK_PAYREQ_STATUS / DIM_JUNK_SD_STATUS

Same junk dimensions as v1 — all low-cardinality flags combined.

### DIM_COMPLETE_REASON / DIM_OK_REASON

Reason code + description dimensions.

### DIM_LOCATION_OF_CARE, DIM_BENEFIT_TYPE, DIM_POOL_OPTION, DIM_COMPANY

Standard Type 1 dimensions.

### DIM_AUDIT

```sql
CREATE OR REPLACE TABLE DW.DIM_AUDIT (
    AUDIT_KEY           INT PRIMARY KEY AUTOINCREMENT,
    ETL_BATCH_ID        VARCHAR(50),
    ETL_LOAD_TS         TIMESTAMP_NTZ,
    SOURCE_SYSTEM       VARCHAR(50),
    SOURCE_TABLE        VARCHAR(50),
    CDC_DML_OPERATION   VARCHAR(10),
    CDC_SEQUENCE_NO     VARCHAR(36),
    ROW_COUNT_PROCESSED INT
);
```

---

## Star Schema Diagram

```
    DIM_DATE                                                DIM_SERVICE_PROVIDER
    (12+ role-playing views)                                (role: provider + caregiver)
         │                                                       │
         │ 12 FKs                                                │
         │                                                       │
    DIM_PAYEE ──┐    ┌──────────────────────────────┐    ┌── DIM_SERVICE_TYPE
                │    │   FACT_PAYMENT_SERVICE        │    │
    DIM_USER ───┤    │   (Timespan Accum Snapshot)   │    ├── DIM_BENEFIT_TYPE
    (3 roles)   │    │──────────────────────────────│    │
                ├────│  FACT_SK (PK)                 │────┤
    DIM_REQUEST │    │  ROW_EFFECTIVE_DT  ★          │    ├── DIM_POOL_OPTION
     _STATUS ───┤    │  ROW_EXPIRY_DT     ★          │    │
                │    │  CURRENT_ROW_FLG   ★          │    ├── DIM_LOCATION_OF_CARE
    DIM_JUNK    │    │──────────────────────────────│    │
    _PAYREQ ────┤    │  12x DATE_*_KEY (FK)          │    ├── DIM_COMPLETE_REASON
                │    │  PAYEE_KEY (FK)               │    │
    DIM_JUNK    │    │  USER_KEY (FK)                │    ├── DIM_OK_REASON
    _SD ────────┤    │  SERVICE_PROVIDER_KEY (FK)    │    │
                │    │  SERVICE_TYPE_KEY (FK)        │    ├── DIM_COMPANY
    DIM_AUDIT ──┘    │  ... more FKs ...             │    │
                     │──────────────────────────────│    │
                     │  PAYMENT_REQUEST_ID (DD)      │────┘
                     │  PAYMENT_REQUEST_DETAIL_ID(DD)│
                     │  SERVICE_DETAIL_ID (DD)       │
                     │──────────────────────────────│
                     │  PRD_REQUESTED_AMOUNT         │
                     │  SD_AMOUNT                    │
                     │  SD_SERVICE_UNITS             │
                     │  ... more facts ...           │
                     │──────────────────────────────│
                     │  RECEIVED_TO_COMPLETE_LAG     │
                     │  ... more lags ...            │
                     │──────────────────────────────│
                     │  IS_DETAIL_ATTACHED (0/1)     │
                     │  IS_COMPLETE (0/1)            │
                     │  DATA_COMPLETENESS_LEVEL      │
                     └──────────────────────────────┘

    ★ = The three timespan columns that make this
        a Timespan Accumulating Snapshot
        (Kimball Ch. 16, pp. 394-395)
```

---

## Summary: Why This Design Is Unquestionable

| Concern | Answer |
|---------|--------|
| Single fact table? | ✓ Yes. One table serves all reporting. |
| Daily report? | `WHERE ROW_EFFECTIVE_DT <= @date AND ROW_EXPIRY_DT >= @date` |
| Weekly report? | Same filter with week-end date. |
| Monthly report? | Same filter with month-end date. |
| Historical stability? | History is never destroyed. Old rows are expired, not deleted. Dec 2025 report in Feb 2026 returns identical numbers. |
| Late-arriving service details? | Row starts as 'REQUEST_ONLY'. When details arrive, old row expires, new row inserted with merged data. Both states preserved. |
| Current operational view? | `WHERE CURRENT_ROW_FLG = 'Y'` — fast, simple. |
| Kimball reference? | Timespan Accumulating Snapshot — Ch. 16, pp. 394-395. SCD Type 2 applied to fact rows. |
| Storage efficient? | Rows created only when state changes — far fewer rows than daily periodic snapshots. |
| Scalable? | Snowflake cluster on `(CURRENT_ROW_FLG, ROW_EFFECTIVE_DT)`. Micro-partition pruning handles billions of rows. |
