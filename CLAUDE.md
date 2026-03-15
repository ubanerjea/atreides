# Project Atreides — Claude Code Instructions

## Project Overview

Atreides is a PostgreSQL performance analysis and monitoring tool. It connects to a PostgreSQL database, runs diagnostic queries (primarily via `pg_stat_statements`), and writes formatted text reports to the `/out/` directory. It also includes DDL for a financial trading/position schema and utilities for running EXPLAIN plans.

## Project Layout

- `src/db.py` — `DBConnection` class; core database access layer
- `src/run_top_statements.py` — main script; generates top SQL statements report
- `src/sql/` — SQL files (queries and DDL)
- `src/tmp/explain/` — scratch area for EXPLAIN plan analysis
- `out/` — report output directory (git-ignored)
- `tests/` — pytest test suite
- `db.toml` — database connection config (credentials live here)

## Running the Project

```bash
# Main report
python src/run_top_statements.py

# EXPLAIN analysis
python src/tmp/explain/run.py

# Tests
pytest tests/test_db.py -v
```

Install in editable mode: `pip install -e .`

## Code Conventions

- Python 3.11+; use built-in `tomllib`, `pathlib.Path`, type hints
- Prefer raw SQL over ORM abstractions — queries live in `src/sql/` as `.sql` files
- All database access goes through `DBConnection` in `src/db.py`
- Reports are written to `/out/` with timestamped filenames
- Keep scripts runnable from the project root (scripts use `sys.path.insert` to resolve `src/`)
- Every script must have a comment at the top with the command to run it, e.g. `# Run with: python src/scripts/db/run_top_statements.py`
- `main()` should contain minimal logic — split logic into well-named methods and have `main()` orchestrate calls to them
- Scripts live under `src/scripts/`, organized into subfolders where applicable (e.g. `src/scripts/db/` for database-related scripts like `mask_db_config.py` and `run_top_statements.py`)

## Query Conventions

- DB query methods should return `List[Dict]` by default (one dict per row, keyed by column name)

## Database & Safety Rules

- Never modify credentials in `db.toml` — use `src/mask_db_config.py` if you need to show config safely
- All queries against the live database should be read-only (SELECT only); never run INSERT/UPDATE/DELETE/DROP against the connected database
- `db.toml` is in `.gitignore` — do not commit it; do not hardcode credentials anywhere

## Testing

- Run `pytest tests/test_db.py -v` to verify database connectivity and basic data integrity
- Tests require a live PostgreSQL connection configured in `db.toml`
- Do not mock the database in tests — tests are integration tests against a real connection

### Running Tests After Refactoring

Always run the following tests after any refactoring to verify correctness:

```bash
pytest tests/test_db.py -v
```

Additional tests may be added to this list as the suite grows.

## Dependencies

Managed via `pyproject.toml`. Key packages: `sqlalchemy`, `psycopg2-binary`, `pandas`, `numpy`. Pin versions in `requirements.txt` when updating.
