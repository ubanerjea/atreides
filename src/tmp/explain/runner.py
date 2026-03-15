"""
EXPLAIN runner: execute a query with EXPLAIN variants and write output to /out/.
"""

import json
import sys
from datetime import datetime
from enum import Enum
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from db import DBConnection


class ExplainMode(Enum):
    EXPLAIN = "explain"
    EXPLAIN_ANALYZE = "explain_analyze"
    EXPLAIN_ANALYZE_JSON = "explain_analyze_json"


_MODE_PREFIX = {
    ExplainMode.EXPLAIN:             "EXPLAIN",
    ExplainMode.EXPLAIN_ANALYZE:     "EXPLAIN ANALYZE",
    ExplainMode.EXPLAIN_ANALYZE_JSON: "EXPLAIN (ANALYZE, FORMAT JSON)",
}

_OUT_DIR = Path(__file__).parent.parent.parent.parent / "out"


def run_explain(query: str, mode: ExplainMode) -> list[tuple]:
    prefix = _MODE_PREFIX[mode]
    explain_sql = f"{prefix}\n{query.strip()}"
    db = DBConnection()
    return db.query(explain_sql)


def write_output(rows: list[tuple], mode: ExplainMode) -> Path:
    _OUT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if mode == ExplainMode.EXPLAIN_ANALYZE_JSON:
        # Single row, single column: JSON value (string or already-parsed object)
        raw = rows[0][0]
        data = json.loads(raw) if isinstance(raw, str) else raw
        filename = f"query_explain_{mode.value}_{timestamp}.json"
        out_path = _OUT_DIR / filename
        out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    else:
        filename = f"query_explain_{mode.value}_{timestamp}.txt"
        out_path = _OUT_DIR / filename
        lines = [row[0] for row in rows]
        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return out_path


def explain_and_save(query: str, mode: ExplainMode) -> Path:
    rows = run_explain(query, mode)
    return write_output(rows, mode)
