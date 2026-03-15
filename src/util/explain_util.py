"""
EXPLAIN runner: execute a query with EXPLAIN variants and write output to /out/.
"""

import json
from datetime import datetime
from enum import Enum
from pathlib import Path

from src.util.db import DBConnection
from src.util.file_util import FileUtil


class ExplainMode(Enum):
    EXPLAIN = "explain"
    EXPLAIN_ANALYZE = "explain_analyze"
    EXPLAIN_ANALYZE_JSON = "explain_analyze_json"


_MODE_PREFIX = {
    ExplainMode.EXPLAIN:              "EXPLAIN",
    ExplainMode.EXPLAIN_ANALYZE:      "EXPLAIN ANALYZE",
    ExplainMode.EXPLAIN_ANALYZE_JSON: "EXPLAIN (ANALYZE, FORMAT JSON)",
}

_file_util = FileUtil()


def run_explain(query: str, mode: ExplainMode) -> list[dict]:
    prefix = _MODE_PREFIX[mode]
    explain_sql = f"{prefix}\n{query.strip()}"
    db = DBConnection()
    return db.query(explain_sql)


def write_output(rows: list[dict], mode: ExplainMode) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if mode == ExplainMode.EXPLAIN_ANALYZE_JSON:
        # Single row, single column: JSON value (string or already-parsed object)
        raw = rows[0]["QUERY PLAN"]
        data = json.loads(raw) if isinstance(raw, str) else raw
        filename = f"query_explain_{mode.value}_{timestamp}.json"
        return _file_util.write_file(filename, json.dumps(data, indent=2))
    else:
        filename = f"query_explain_{mode.value}_{timestamp}.txt"
        lines = [row["QUERY PLAN"] for row in rows]
        return _file_util.write_file(filename, "\n".join(lines) + "\n")


def explain_and_save(query: str, mode: ExplainMode) -> Path:
    rows = run_explain(query, mode)
    return write_output(rows, mode)
