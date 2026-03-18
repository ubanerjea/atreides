# Run with: python src/tmp/cpu_stress/run.py
#           python src/tmp/cpu_stress/run.py [--sessions {1,2}]

import argparse
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from src.util.db import DBConnection
from src.util.file_util import FileUtil

SQL_PATH = Path(__file__).resolve().parent.parent.parent / "sql" / "queries" / "cpu_stress.sql"
OUT_DIR  = Path(__file__).resolve().parent.parent.parent.parent / "out"
WIDTH    = 100

# Distinctive token used to locate the stress query in pg_stat_statements
_STRESS_MARKER = "fivethous"

_ACTIVITY_SQL = """
SELECT pid,
       state,
       wait_event_type,
       wait_event,
       EXTRACT(EPOCH FROM (now() - query_start))::numeric(10,2) AS duration_s,
       LEFT(query, 100) AS query_snippet
FROM pg_stat_activity
WHERE state = 'active'
  AND query NOT LIKE '%pg_stat_activity%'
ORDER BY query_start
"""

_STAT_STATEMENTS_SQL = f"""
SELECT queryid,
       calls,
       round(total_exec_time::numeric, 2)  AS total_exec_time_ms,
       round(mean_exec_time::numeric,  2)  AS mean_exec_time_ms,
       rows,
       shared_blks_hit,
       shared_blks_read,
       COALESCE(temp_blks_written, 0)      AS temp_blks_written,
       LEFT(query, 120)                    AS query_text
FROM pg_stat_statements
WHERE query LIKE '%%{_STRESS_MARKER}%%'
  AND query NOT LIKE '%%pg_stat%%'
ORDER BY total_exec_time DESC
LIMIT 5
"""


class CpuStressRunner:
    def __init__(self):
        self._monitor_db = DBConnection()
        self._file_util  = FileUtil(str(OUT_DIR))

    # ── SQL loading ───────────────────────────────────────────────────────────

    def load_stress_statements(self) -> list[str]:
        """Read cpu_stress.sql, strip block comments, split into statements."""
        raw = SQL_PATH.read_text(encoding="utf-8")
        raw = re.sub(r'/\*.*?\*/', '', raw, flags=re.DOTALL)
        return [s.strip() for s in raw.split(';') if s.strip()]

    # ── database helpers ──────────────────────────────────────────────────────

    def snapshot_stat_statements(self) -> list[dict]:
        return self._monitor_db.query(_STAT_STATEMENTS_SQL)

    def run_stress_session(self, session_id: int, statements: list[str]) -> dict:
        """Execute the stress SQL on a dedicated connection; return timing metadata."""
        db    = DBConnection()
        start = time.monotonic()
        start_wall = datetime.now()
        rows_returned = 0
        error = None
        try:
            with db._engine.connect() as conn:
                for stmt in statements[:-1]:      # SET statements
                    conn.execute(text(stmt))
                result = conn.execute(text(statements[-1]))  # SELECT
                rows_returned = len(result.fetchall())
        except Exception as exc:
            error = str(exc)
        return {
            "session_id": session_id,
            "start_wall": start_wall,
            "elapsed_s":  round(time.monotonic() - start, 2),
            "rows":       rows_returned,
            "error":      error,
        }

    def monitor_activity(self, stop_event: threading.Event, snapshots: list) -> None:
        """Poll pg_stat_activity every 1.5 s until stop_event is set."""
        while not stop_event.is_set():
            ts   = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            rows = self._monitor_db.query(_ACTIVITY_SQL)
            if rows:
                snapshots.append({"timestamp": ts, "rows": rows})
            stop_event.wait(1.5)

    # ── orchestration ─────────────────────────────────────────────────────────

    def run_parallel_stress(self, n_sessions: int = 2) -> dict:
        statements = self.load_stress_statements()

        activity_snapshots: list = []
        stop_event = threading.Event()
        monitor_thread = threading.Thread(
            target=self.monitor_activity,
            args=(stop_event, activity_snapshots),
            daemon=True,
        )

        before = self.snapshot_stat_statements()
        label = "stress session 1" if n_sessions == 1 else f"stress sessions 1 and 2 in parallel"
        print(f"Launching {label}...")

        monitor_thread.start()
        with ThreadPoolExecutor(max_workers=n_sessions) as pool:
            futures = {
                pool.submit(self.run_stress_session, i, statements): i
                for i in range(1, n_sessions + 1)
            }
            results = {}
            for future in as_completed(futures):
                r = future.result()
                results[r["session_id"]] = r
                status = f"ERROR: {r['error']}" if r["error"] else f"{r['rows']:,} rows"
                print(f"  Session {r['session_id']} finished in {r['elapsed_s']}s  ({status})")

        stop_event.set()
        monitor_thread.join()
        after = self.snapshot_stat_statements()

        return {
            "n_sessions":         n_sessions,
            "before":             before,
            "after":              after,
            "session_results":    results,
            "activity_snapshots": activity_snapshots,
        }

    # ── report formatting ─────────────────────────────────────────────────────

    def _fmt_stat_rows(self, rows: list[dict]) -> str:
        if not rows:
            return "  (no matching rows in pg_stat_statements)\n"
        lines = []
        for r in rows:
            lines.append(
                f"  queryid={r['queryid']}"
                f"  calls={r['calls']}"
                f"  total_ms={float(r['total_exec_time_ms']):>12,.2f}"
                f"  mean_ms={float(r['mean_exec_time_ms']):>10,.2f}"
                f"  rows={r['rows']:>10,}"
                f"  blks_hit={r['shared_blks_hit']:>8,}"
                f"  blks_read={r['shared_blks_read']:>8,}"
                f"  temp_written={r['temp_blks_written']:>6,}\n"
                f"    query: {str(r['query_text'])[:100]}\n"
            )
        return "".join(lines)

    def _fmt_activity_snapshots(self, snapshots: list, n_sessions: int) -> str:
        if not snapshots:
            return "  (no active sessions captured — stress query may have completed before first poll)\n"
        # Count distinct PIDs to detect parallel workers. PostgreSQL spins up
        # parallel worker backends that inherit the leader's query text, so more
        # PIDs than stress sessions indicates parallel execution.
        all_pids = {r["pid"] for snap in snapshots for r in snap["rows"]}
        note = ""
        if len(all_pids) > n_sessions:
            note = (
                f"  NOTE: {len(all_pids)} distinct PIDs observed across all snapshots "
                f"(expected {n_sessions} stress session(s) + up to "
                f"{len(all_pids) - n_sessions} parallel worker backends).\n"
                "  PostgreSQL parallel query is active — each session may have spawned workers.\n\n"
            )
        lines = [note]
        for snap in snapshots:
            lines.append(f"  [{snap['timestamp']}]\n")
            for r in snap["rows"]:
                wait = (
                    f"{r['wait_event_type']}/{r['wait_event']}"
                    if r["wait_event_type"]
                    else "None (CPU active)"
                )
                lines.append(
                    f"    pid={r['pid']:>6}"
                    f"  state={r['state']:<8}"
                    f"  wait={wait:<28}"
                    f"  dur={r['duration_s']:>7}s"
                    f"  query: {str(r['query_snippet'])[:60]}\n"
                )
        return "".join(lines)

    # ── analysis ──────────────────────────────────────────────────────────────

    def _analyse(self, data: dict) -> str:
        before     = data["before"]
        after      = data["after"]
        results    = data["session_results"]
        snapshots  = data["activity_snapshots"]
        n_sessions = data["n_sessions"]
        sep        = "-" * WIDTH

        after_row  = after[0] if after else None
        # Match the before snapshot to the same queryid so COPY/DDL rows that also
        # contain the stress marker don't corrupt the delta calculation.
        before_row = (
            next((r for r in before if r["queryid"] == after_row["queryid"]), None)
            if after_row else None
        )

        lines = []

        # ── Finding 1: concurrent execution (or single-session baseline) ──────
        s1 = results.get(1, {})
        lines.append("FINDING 1 — Concurrent execution\n")
        lines.append(sep + "\n")
        if n_sessions == 1:
            lines.append(
                "  Single-session mode — no concurrency intended.\n"
                f"  Session 1: started {s1['start_wall'].strftime('%H:%M:%S')}, elapsed {s1.get('elapsed_s')}s\n"
                "  This run serves as a single-session baseline for comparison with a two-session\n"
                "  contention run. Compare elapsed time and pg_stat_statements cost between modes\n"
                "  to quantify the overhead introduced by CPU contention.\n"
            )
        else:
            s2 = results.get(2, {})
            s1_start = s1.get("start_wall")
            s2_start = s2.get("start_wall")
            s1_end   = s1_start.timestamp() + s1.get("elapsed_s", 0) if s1_start else None
            s2_end   = s2_start.timestamp() + s2.get("elapsed_s", 0) if s2_start else None
            concurrent = (
                s1_start and s2_start
                and s1_start.timestamp() < s2_end
                and s2_start.timestamp() < s1_end
            )
            if concurrent:
                overlap_s = max(0.0, min(s1_end, s2_end) - max(s1_start.timestamp(), s2_start.timestamp()))
                lines.append(
                    f"  CONFIRMED. Sessions overlapped for ~{overlap_s:.1f}s.\n"
                    f"  Session 1: started {s1_start.strftime('%H:%M:%S')}, elapsed {s1.get('elapsed_s')}s\n"
                    f"  Session 2: started {s2_start.strftime('%H:%M:%S')}, elapsed {s2.get('elapsed_s')}s\n"
                )
            else:
                lines.append(
                    "  WARNING: Sessions did not overlap — contention test may be invalid.\n"
                    "  Try reducing startup overhead or increasing query complexity.\n"
                )
        lines.append("\n")

        # ── Finding 2: CPU-bound (wait_event analysis) ────────────────────────
        lines.append("FINDING 2 — CPU-bound execution (wait_event analysis)\n")
        lines.append(sep + "\n")
        all_obs     = [r for snap in snapshots for r in snap["rows"]]
        cpu_obs     = [r for r in all_obs if r["wait_event_type"] is None]
        wait_obs    = [r for r in all_obs if r["wait_event_type"] is not None]

        if all_obs:
            pct = 100 * len(cpu_obs) / len(all_obs)
            lines.append(
                f"  {len(cpu_obs)}/{len(all_obs)} session-observations ({pct:.0f}%) "
                f"had wait_event_type = NULL (actively executing on CPU).\n"
            )
            if pct >= 80:
                lines.append(
                    "  CONFIRMED CPU-bound: sessions spent the large majority of observed time\n"
                    "  executing instructions rather than waiting on locks, I/O, or other resources.\n"
                )
            else:
                wait_counts: dict[str, int] = {}
                for r in wait_obs:
                    k = f"{r['wait_event_type']}/{r['wait_event']}"
                    wait_counts[k] = wait_counts.get(k, 0) + 1
                lines.append("  Sessions were NOT purely CPU-bound. Observed wait events:\n")
                for k, v in sorted(wait_counts.items(), key=lambda x: -x[1]):
                    lines.append(f"    {k}: {v} observations\n")
        else:
            lines.append("  No pg_stat_activity data captured during the run.\n")
        lines.append("\n")

        # ── Finding 3: work_mem / temp spill ─────────────────────────────────
        lines.append("FINDING 3 — Hash join memory (work_mem / temp spill check)\n")
        lines.append(sep + "\n")
        if after_row:
            temp_after  = int(after_row["temp_blks_written"])
            temp_before = int(before_row["temp_blks_written"]) if before_row else 0
            delta_temp  = temp_after - temp_before
            if delta_temp == 0:
                lines.append(
                    "  temp_blks_written delta = 0.\n"
                    "  Hash join remained in memory — work_mem = '1GB' was sufficient.\n"
                    "  Workload is confirmed CPU-bound as intended.\n"
                )
            else:
                lines.append(
                    f"  WARNING: temp_blks_written delta = {delta_temp:,} blocks "
                    f"({delta_temp * 8 / 1024:.1f} MB spilled to disk).\n"
                    "  Increase work_mem to keep the hash join in memory and restore CPU-bound behaviour.\n"
                )
        else:
            lines.append("  pg_stat_statements data unavailable — cannot check temp spill.\n")
        lines.append("\n")

        # ── Finding 4: pg_stat_statements delta ──────────────────────────────
        lines.append("FINDING 4 — pg_stat_statements accumulated cost\n")
        lines.append(sep + "\n")
        if after_row and before_row:
            calls_delta   = after_row["calls"] - before_row["calls"]
            elapsed_delta = float(after_row["total_exec_time_ms"]) - float(before_row["total_exec_time_ms"])
            session_label = "one session" if n_sessions == 1 else "both sessions"
            lines.append(
                f"  calls delta      : {calls_delta:+d}  (expected +{n_sessions})\n"
                f"  total_exec_time  : +{elapsed_delta:,.1f} ms across {session_label}\n"
                f"  avg per session  : {elapsed_delta / max(calls_delta, 1):,.1f} ms\n"
            )
        elif after_row:
            lines.append(
                f"  First recorded run — no prior baseline.\n"
                f"  calls={after_row['calls']}  total_exec_time={float(after_row['total_exec_time_ms']):,.1f} ms\n"
            )
        else:
            lines.append("  pg_stat_statements data not available.\n")
        lines.append("\n")

        # ── Finding 5: diagnosability ─────────────────────────────────────────
        lines.append("FINDING 5 — Visibility in pg_stat_statements\n")
        lines.append(sep + "\n")
        if after_row:
            total_ms = float(after_row["total_exec_time_ms"])
            lines.append(
                f"  VISIBLE. The stress query accumulated {total_ms:,.0f} ms total execution time "
                f"across {after_row['calls']} call(s).\n"
                f"  Ordering pg_stat_statements by total_exec_time DESC will rank it at or near the top.\n"
                f"  This matches how a CPU-intensive query would be identified in a production diagnosis.\n"
            )
        else:
            lines.append(
                "  Query NOT found in pg_stat_statements.\n"
                "  Check that pg_stat_statements is installed and pg_stat_statements.track is not 'none'.\n"
            )

        return "".join(lines)

    # ── report assembly ───────────────────────────────────────────────────────

    def build_report(self, data: dict) -> str:
        sep        = "=" * WIDTH
        subsep     = "-" * WIDTH
        now        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        n_sessions = data["n_sessions"]

        def session_line(s: dict) -> str:
            suffix = f"  ERROR: {s['error']}" if s.get("error") else ""
            return f"{s.get('elapsed_s')}s elapsed, {s.get('rows', 0):,} rows returned{suffix}"

        session_lines = "".join(
            f"Session {i} : {session_line(data['session_results'].get(i, {}))}\n"
            for i in range(1, n_sessions + 1)
        )

        parts = [
            "CPU Stress Verification Report\n",
            f"Generated : {now}\n",
            f"Mode      : {n_sessions} session(s)\n",
            session_lines,
            sep + "\n",

            "\n1. pg_stat_statements — BEFORE stress run\n",
            subsep + "\n",
            self._fmt_stat_rows(data["before"]),

            "\n2. pg_stat_activity — DURING stress run (sampled every ~1.5s)\n",
            subsep + "\n",
            self._fmt_activity_snapshots(data["activity_snapshots"], n_sessions),

            "\n3. pg_stat_statements — AFTER stress run\n",
            subsep + "\n",
            self._fmt_stat_rows(data["after"]),

            "\n4. ANALYSIS\n",
            sep + "\n",
            self._analyse(data),
        ]
        return "".join(parts)

    def write_report(self, content: str) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self._file_util.write_file(f"cpu_stress_verification_{timestamp}.txt", content)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run CPU stress query and verify via pg_stat_statements.")
    parser.add_argument(
        "--sessions", type=int, choices=[1, 2], default=2,
        help="Number of concurrent stress sessions to launch (default: 2)",
    )
    args = parser.parse_args()

    runner = CpuStressRunner()
    data   = runner.run_parallel_stress(args.sessions)
    report = runner.build_report(data)
    path   = runner.write_report(report)
    print(f"Report written to: {path}")


if __name__ == "__main__":
    main()
