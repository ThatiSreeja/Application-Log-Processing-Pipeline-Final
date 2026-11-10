import json
from pathlib import Path
from src.pipeline import run_pipeline
import sqlite3

def test_pipeline_and_rerun_are_idempotent(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    record = {"timestamp":"2026-11-10T09:30:00Z","level":"ERROR","service":"payment",
              "message":"payment failed","request_id":"R1","latency_ms":100}
    (input_dir / "logs.jsonl").write_text(json.dumps(record) + "\n", encoding="utf-8")
    db = tmp_path / "out" / "logs.db"
    first = run_pipeline(input_dir, db)
    second = run_pipeline(input_dir, db)
    conn = sqlite3.connect(db)
    count = conn.execute("SELECT COUNT(*) FROM log_events").fetchone()[0]
    conn.close()
    assert first["output"] == 1
    assert second["output"] == 0
    assert count == 1
