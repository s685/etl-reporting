"""
Run DMF setup (setup_dmf_data_quality.sql) and log debug info for troubleshooting.
Uses same datamart args as new_rfb_and_total_claimants_active report.
"""
from __future__ import annotations

import json
import sys
import time
import uuid
from pathlib import Path

from datamart_analytics.connector.snowpark_connector import SnowparkConnector
from datamart_analytics.logger import logger
from datamart_analytics.tools.datamart_utils import (
    create_and_parse_datamart_table_args,
    create_target_credentials,
)

DEBUG_LOG = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
RUN_ID = f"run_{uuid.uuid4().hex[:8]}"
SESSION_ID = "debug-session"


def _log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # #region agent log
    try:
        DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "sessionId": SESSION_ID,
            "runId": RUN_ID,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with open(DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # #endregion


def main() -> None:
    datamart_table = create_and_parse_datamart_table_args("new_rfb_and_total_claimants_active")

    _log("H1", "run_dmf_setup:entry", "DMF setup run started", {"target_db": datamart_table.target_database, "target_schema": datamart_table.target_schema})

    creds = create_target_credentials(datamart_table)
    sql_base = Path(__file__).resolve().parent.parent.parent / "sql"
    setup_path = sql_base / "new_rfb_and_total_claimants_active" / "setup_dmf_data_quality.sql"
    query = setup_path.read_text(encoding="utf-8")
    for k, v in [
        ("{{TARGET_DATABASE}}", datamart_table.target_database),
        ("{{TARGET_SCHEMA}}", datamart_table.target_schema),
        ("{{SOURCE_DATABASE}}", datamart_table.source_database),
    ]:
        if k in query:
            query = query.replace(k, v)
    _log("H3", "run_dmf_setup:placeholders", "Placeholders substituted", {"keys_replaced": ["TARGET_DATABASE", "TARGET_SCHEMA", "SOURCE_DATABASE"]})

    with SnowparkConnector(creds) as conn:
        if conn.session is None:
            _log("H5", "run_dmf_setup:session", "Session is None", {})
            logger.error("Snowpark session not initialized")
            sys.exit(1)
        conn.set_session_variables_from_datamart_table(datamart_table)

        obj_type_sql = f"""
        SELECT TABLE_TYPE FROM {datamart_table.target_database}.INFORMATION_SCHEMA.TABLES
        WHERE UPPER(TABLE_CATALOG) = UPPER('{datamart_table.target_database}')
          AND UPPER(TABLE_SCHEMA) = UPPER('{datamart_table.target_schema}')
          AND UPPER(TABLE_NAME) = 'NEW_RFB_AND_TOTAL_CLAIMANTS_ACTIVE_DETAIL'
        """
        try:
            obj_df = conn.session.sql(obj_type_sql).collect()
            obj_type = str(obj_df[0]["TABLE_TYPE"]) if obj_df else "NOT_FOUND"
        except Exception as e:
            obj_type = f"ERROR:{e!s}"
        _log("H1", "run_dmf_setup:object_type", "Target object type (TABLE vs VIEW)", {"object_type": obj_type, "detail_name": "new_rfb_and_total_claimants_active_detail"})

        try:
            _log("H4", "run_dmf_setup:before_exec", "About to run setup SQL", {"query_len": len(query)})
            conn.session.sql(query).collect()
            _log("H4", "run_dmf_setup:after_exec", "Setup SQL completed", {"status": "ok"})
        except Exception as e:
            err_msg = str(e).upper()
            ids = []
            if "NOT A VIEW" in err_msg or "IS NOT A VIEW" in err_msg or "BASE TABLE" in err_msg:
                ids.append("H1")
            if "POLICY NUMBER" in err_msg or "POLICY_NUMBER" in err_msg or "invalid column" in err_msg:
                ids.append("H2")
            if "{{TARGET_DATABASE}}" in query or "{{TARGET_SCHEMA}}" in query or "{{SOURCE_DATABASE}}" in query:
                ids.append("H3")
            if "UNEXPECTED DATABASE" in err_msg or "DATABASE NAME" in err_msg:
                ids.append("H4")
            if "INVALID IDENTIFIER" in err_msg or "VARIABLE" in err_msg or "NOT FOUND" in err_msg:
                ids.append("H5")
            if not ids:
                ids = ["H1", "H2", "H3", "H4", "H5"]
            for hi in ids:
                _log(hi, "run_dmf_setup:error", "Setup failed", {"error": str(e), "error_type": type(e).__name__})
            logger.error("DMF setup failed: %s", e, exc_info=True)
            raise


if __name__ == "__main__":
    main()
