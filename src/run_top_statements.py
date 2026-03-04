# Run with: python src/run_top_statements.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import tomllib
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.db import DBConnection

QUERY_NAME = "top_statements"
SQL_PATH   = Path(__file__).parent / "sql" / "top_statements.sql"
OUT_DIR    = Path(__file__).parent.parent / "out"
CFG_PATH   = Path(__file__).parent.parent / "db.toml"

METRIC_ORDER = [
    "elapsed_time",
    "avg_elapsed_time",
    "disk_reads",
    "avg_disk_reads",
    "buffer_gets",
    "avg_buffer_gets",
    "disk_writes",
    "avg_disk_writes",
    "concurrency_wait_time",
]

METRIC_LABELS = {
    "elapsed_time"         : "ELAPSED TIME (ms)",
    "avg_elapsed_time"     : "AVG ELAPSED TIME (ms)",
    "disk_reads"           : "DISK READS (blocks)",
    "avg_disk_reads"       : "AVG DISK READS (blocks)",
    "buffer_gets"          : "BUFFER GETS (blocks)",
    "avg_buffer_gets"      : "AVG BUFFER GETS (blocks)",
    "disk_writes"          : "DISK WRITES (blocks)",
    "avg_disk_writes"      : "AVG DISK WRITES (blocks)",
    "concurrency_wait_time": "CONCURRENCY WAIT TIME (ms)",
}

COLUMNS = ["metric", "queryid", "query_text", "calls", "metric_value"]
WIDTH   = 120


def main():
    with open(CFG_PATH, "rb") as f:
        cfg = tomllib.load(f)["database"]

    db_name   = cfg["name"]
    schema    = cfg["schema"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path  = OUT_DIR / f"{QUERY_NAME}_{db_name}_{schema}_{timestamp}.txt"

    OUT_DIR.mkdir(exist_ok=True)

    db      = DBConnection(CFG_PATH)
    sql     = SQL_PATH.read_text()
    rows    = db.query(sql)
    io_rows = db.query("SELECT setting FROM pg_settings WHERE name = 'track_io_timing'")
    io_timing_on = io_rows[0][0].lower() == "on" if io_rows else False

    df = pd.DataFrame(rows, columns=COLUMNS)

    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(f"Top SQL Statements Report\n")
        fh.write(f"Database : {db_name}\n")
        fh.write(f"Schema   : {schema}\n")
        fh.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        if not io_timing_on:
            fh.write(f"NOTE     : track_io_timing=off — I/O times are 0; concurrency wait = total elapsed time\n")
        fh.write("=" * WIDTH + "\n")

        groups = {m: g for m, g in df.groupby("metric", sort=False)}

        for metric in METRIC_ORDER:
            label = METRIC_LABELS[metric]
            group = groups.get(metric)

            fh.write(f"\nTOP 10 BY {label}\n")
            fh.write("-" * WIDTH + "\n")

            if group is None or group.empty:
                fh.write("  (no data)\n")
                continue

            display = (
                group[["queryid", "query_text", "calls", "metric_value"]]
                .copy()
                .reset_index(drop=True)
            )
            display.index         += 1
            display.index.name     = "rank"
            display["metric_value"] = display["metric_value"].apply(
                lambda v: f"{float(v):,.2f}"
            )
            display["query_text"] = display["query_text"].str.slice(0, 80).str.replace("\n", " ", regex=False)

            fh.write(display.to_string())
            fh.write("\n")

    print(f"Output written to: {out_path}")


if __name__ == "__main__":
    main()
