# Repository Guidelines

## Project Structure & Module Organization
- `app.py` runs the Streamlit dashboard for comparing holdings and totals.
- `ingest.py` fetches holdings from Amplify, validates them, and upserts into `holdings.duckdb`.
- `compare.py` provides read-only query helpers used by the app.
- `backfill_excel.py` backfills historical CSV/Excel files into DuckDB.
- `holdings.duckdb` is the tracked data store.
- `.github/workflows/daily_fetch.yml` runs the scheduled ingest and commit job.
- `requirements.txt` lists Python dependencies.

## Build, Test, and Development Commands
- `python -m venv .venv` then `.\.venv\Scripts\Activate.ps1` to create/activate a local venv.
- `pip install -r requirements.txt` installs runtime dependencies.
- `python ingest.py --date YYYY-MM-DD --db holdings.duckdb` fetches and loads holdings.
- `python backfill_excel.py <path> --recursive --date YYYY-MM-DD` imports historical files.
- `streamlit run app.py` starts the local dashboard.

## Coding Style & Naming Conventions
- Follow the existing PEP 8-ish style: 4-space indents and clear spacing between top-level defs.
- Use `snake_case` for functions/variables and `UPPER_SNAKE` for constants (e.g., `DB_PATH`).
- Keep dataframe column names consistent: `date`, `ticker`, `name`, `shares`, `market_value`, `weight`.
- Prefer explicit type hints where the code already uses them.

## Testing Guidelines
- No automated test suite or coverage gate is configured today.
- If you add tests, place them under `tests/` and follow `test_*.py` naming.

## Commit & Pull Request Guidelines
- Commit messages are short, imperative, and sentence case (examples in history: "Update YYY holdings", "Add cash position handling...").
- For PRs, include: purpose, data impact (DB schema or holdings changes), and before/after screenshots for UI edits.
- Link any related issue or tracking ticket when available.

## Automation & Data Notes
- The daily GitHub Action runs on a cron schedule and commits updates to `holdings.duckdb` with the message "Update YYY holdings".
- Avoid committing personal `.venv` or local data files beyond the tracked DuckDB.
