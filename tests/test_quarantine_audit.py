import csv
import json

from src.pipeline import run_pipeline


def test_quarantine_contains_specific_error_categories_and_audit_counts(tmp_path, monkeypatch):
    import src.pipeline as pipeline

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    rows = [
        {"timestamp": "2026-01-01T00:00:00Z", "level": "INFO", "message": "service is missing"},
        {"timestamp": "not-a-timestamp", "level": "INFO", "service": "api", "message": "invalid timestamp"},
        {"timestamp": "2026-01-01T00:00:00Z", "level": "INFO", "service": "not_registered", "message": "unknown service"},
    ]
    (input_dir / "events.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8"
    )

    quarantine = tmp_path / "quarantine"
    audit_dir = tmp_path / "audit"
    monkeypatch.setattr(pipeline.config, "QUARANTINE_DIR", quarantine)
    monkeypatch.setattr(pipeline.config, "AUDIT_DIR", audit_dir)
    monkeypatch.setattr(pipeline.config, "ERROR_LOG_PATH", audit_dir / "pipeline_errors.jsonl")
    monkeypatch.setattr(pipeline.config, "REPORT_DIR", tmp_path / "reports")
    monkeypatch.setattr(pipeline.config, "PROCESSED_DIR", tmp_path / "processed")

    result = run_pipeline(input_dir, tmp_path / "db" / "logs.db")

    with (quarantine / "rejected_logs.csv").open(newline="", encoding="utf-8") as handle:
        quarantined = list(csv.DictReader(handle))
    categories = {row["error_category"] for row in quarantined}
    assert categories == {"MISSING_FIELD", "INVALID_TIMESTAMP", "UNKNOWN_SERVICE"}
    assert result["error_category_counts"] == {
        "MISSING_FIELD": 1, "INVALID_TIMESTAMP": 1, "UNKNOWN_SERVICE": 1
    }

    with (audit_dir / "audit_log.csv").open(newline="", encoding="utf-8") as handle:
        audit_row = next(csv.DictReader(handle))
    assert json.loads(audit_row["error_category_counts"]) == result["error_category_counts"]

    error_records = [json.loads(line) for line in (audit_dir / "pipeline_errors.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {record["error_category"] for record in error_records} == categories


def test_error_code_for_duplicate_is_categorized():
    from src.pipeline import _error_code
    assert _error_code("Duplicate event (same normalized event already processed)") == "DUPLICATE_EVENT"
