# Setup and testing

Requirements: Python 3.10+ and pip. The test dependency is pinned to the compatible major range `pytest>=8.3,<9` in `requirements.txt`. From the project root:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q
python -m src.pipeline
```

The default input directory is `data/input`; the SQLite database is `data/processed/logs.db`. The pipeline creates output directories as needed. To run against another folder/database, call `run_pipeline(input_dir, db_path)`. Use `run_pipeline(input_dir, db_path, force_reprocess=True)` to bypass the file hash/version skip check. Duplicate event fingerprints are still protected by the database unique constraint.

Tests cover parsing, validation, first/repeat runs, modified files, forced reprocessing, file retries, transient SQLite operational-error retry and WARNING logging, persistent file and database failures, WARNING/ERROR logging, schema drift, quarantine, referential integrity, and audit behavior. Run `python -m pytest -v` to see each test.


Latest verification in the prepared project: `python -m pytest -q` completed with **35 passed**. Re-run the command after changing code or dependencies; this count is execution evidence for this package, not a guarantee for every environment.
