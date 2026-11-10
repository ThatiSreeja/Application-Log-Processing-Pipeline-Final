import json,sqlite3
from pathlib import Path
from src.pipeline import run_pipeline
from src.normalization import normalize_timestamp,normalize_level
from src.parser import parse_log
from src.validation import validate_record
from src.deduplication import stable_record_id
def test_naive_timestamp_assumed_utc():
    assert normalize_timestamp("2026-01-01 12:00:00")=="2026-01-01T12:00:00Z"
def test_offset_timestamp_converted_to_utc():
    assert normalize_timestamp("2026-01-01T12:00:00+05:30")=="2026-01-01T06:30:00Z"
def test_unknown_level_rejected():
    r={"timestamp":"x","level":"TRACE","service":"api","message":"x","_normalized_timestamp":"x","_normalized_level":None}
    assert "Unknown severity level" in validate_record(r)
def test_extra_json_fields_preserved(tmp_path):
    inp=tmp_path/"in";inp.mkdir(); (inp/"x.jsonl").write_text(json.dumps({"timestamp":"2026-01-01T00:00:00Z","level":"INFO","service":"api","message":"ok","custom":"yes"})+"\n")
    db=tmp_path/"db"/"x.db"; run_pipeline(inp,db)
    con=sqlite3.connect(db); extra=con.execute("select extra_fields from log_events").fetchone()[0];con.close()
    assert '"custom": "yes"' in extra
def test_duplicate_across_rerun_is_not_inserted(tmp_path):
    inp=tmp_path/"in";inp.mkdir(); line=json.dumps({"timestamp":"2026-01-01T00:00:00Z","level":"INFO","service":"api","message":"ok"})
    (inp/"x.jsonl").write_text(line+"\n"); db=tmp_path/"db"/"x.db"
    a=run_pipeline(inp,db); b=run_pipeline(inp,db)
    assert a["output"]==1 and b["skipped_files"]==1 and b["input"]==0
    con=sqlite3.connect(db); assert con.execute("select count(*) from log_events").fetchone()[0]==1;con.close()
def test_late_and_out_of_order_events_preserved(tmp_path):
    inp=tmp_path/"in";inp.mkdir()
    a={"timestamp":"2026-01-02T00:00:00Z","level":"INFO","service":"api","message":"later"}
    b={"timestamp":"2026-01-01T00:00:00Z","level":"INFO","service":"api","message":"late arrival"}
    (inp/"x.jsonl").write_text(json.dumps(a)+"\n"+json.dumps(b)+"\n")
    db=tmp_path/"db"/"x.db"; result=run_pipeline(inp,db)
    assert result["output"]==2
def test_empty_input(tmp_path):
    inp=tmp_path/"in";inp.mkdir();(inp/"empty.txt").write_text("")
    result=run_pipeline(inp,tmp_path/"db"/"x.db")
    assert result["input"]==0
def test_file_level_failure_does_not_block_next_file(tmp_path):
    inp=tmp_path/"in";inp.mkdir()
    (inp/"a.txt").write_text("this is invalid\n")
    good={"timestamp":"2026-01-01T00:00:00Z","level":"INFO","service":"api","message":"ok"}
    (inp/"b.jsonl").write_text(json.dumps(good)+"\n")
    result=run_pipeline(inp,tmp_path/"db"/"x.db")
    assert result["output"]==1 and result["rejected"]==1
def test_record_id_stable():
    assert stable_record_id("f",1,"x")==stable_record_id("f",1,"x")
def test_json_array_rejected():
    try: parse_log("[1,2]")
    except ValueError: assert True
    else: assert False

def test_transient_file_failure_is_retried_and_succeeds(tmp_path, monkeypatch):
    import src.pipeline as pipeline
    inp = tmp_path / "retry_in"; inp.mkdir()
    good = {"timestamp": "2026-01-01T00:00:00Z", "level": "INFO", "service": "api", "message": "retry me"}
    (inp / "retry.jsonl").write_text(json.dumps(good) + "\n", encoding="utf-8")
    original = pipeline.read_log_file
    calls = {"count": 0}
    def fail_once(path):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("temporary file lock")
        yield from original(path)
    monkeypatch.setattr(pipeline, "read_log_file", fail_once)
    result = pipeline.run_pipeline(inp, tmp_path / "retry_db" / "logs.db")
    assert calls["count"] == 2
    assert result["output"] == 1 and result["failed_files"] == 0
    con = sqlite3.connect(tmp_path / "retry_db" / "logs.db")
    attempts = con.execute("select attempt_number,status from file_attempts order by attempt_number").fetchall()
    assert attempts == [(1, "RETRYING"), (2, "SUCCESS")]
    assert con.execute("select count(*) from log_events").fetchone()[0] == 1
    con.close()

def test_schema_drift_additive_fields_are_reported_and_preserved(tmp_path):
    inp = tmp_path / "schema_in"; inp.mkdir()
    good = {"timestamp": "2026-01-01T00:00:00Z", "level": "INFO", "service": "api",
            "message": "new field", "deployment_region": "south"}
    (inp / "schema.jsonl").write_text(json.dumps(good) + "\n", encoding="utf-8")
    result = run_pipeline(inp, tmp_path / "schema_db" / "logs.db")
    assert result["schema_drift"] == 1
    assert result["unexpected_fields"] == ["deployment_region"]
    con = sqlite3.connect(tmp_path / "schema_db" / "logs.db")
    extra = con.execute("select extra_fields from log_events").fetchone()[0]
    con.close()
    assert '"deployment_region": "south"' in extra

def test_incompatible_schema_field_type_is_quarantined(tmp_path):
    inp = tmp_path / "bad_schema_in"; inp.mkdir()
    bad = {"timestamp": "2026-01-01T00:00:00Z", "level": "INFO", "service": "api",
           "message": {"unexpected": "object"}}
    (inp / "bad.jsonl").write_text(json.dumps(bad) + "\n", encoding="utf-8")
    result = run_pipeline(inp, tmp_path / "bad_schema_db" / "logs.db")
    assert result["rejected"] == 1 and result["output"] == 0

def test_schema_change_is_persistently_audited(tmp_path):
    inp = tmp_path / "schema_audit_in"; inp.mkdir()
    row = {"timestamp": "2026-01-01T00:00:00Z", "level": "INFO", "service": "api",
           "message": "schema audit", "new_tag": 123}
    (inp / "audit.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    db = tmp_path / "schema_audit_db" / "logs.db"
    run_pipeline(inp, db)
    con = sqlite3.connect(db)
    changes = con.execute("select field_name,change_type,observed_type from schema_change_events").fetchall()
    con.close()
    assert changes == [("new_tag", "ADDITIVE_FIELD", "number")]

def test_failed_file_is_recorded_for_later_run(tmp_path, monkeypatch, caplog):
    import src.pipeline as pipeline
    inp = tmp_path / "permanent_failure_in"; inp.mkdir()
    file_path = inp / "unavailable.jsonl"
    file_path.write_text("{}\n", encoding="utf-8")
    original = pipeline.read_log_file
    def always_fail(path):
        raise OSError("simulated persistent file lock")
        yield  # pragma: no cover
    import logging
    caplog.set_level(logging.ERROR, logger="application_log_pipeline")
    monkeypatch.setattr(pipeline, "read_log_file", always_fail)
    db = tmp_path / "persistent_db" / "logs.db"
    result = pipeline.run_pipeline(inp, db)
    assert result["failed_files"] == 1
    con = sqlite3.connect(db)
    state = con.execute("select status,attempt_count,last_error from file_processing_state").fetchone()
    attempts = con.execute("select count(*) from file_attempts").fetchone()[0]
    con.close()
    assert state[0] == "FAILED" and state[1] == 3
    assert "simulated persistent file lock" in state[2]
    assert attempts == 3
    assert "File unavailable.jsonl failed" in caplog.text
    assert "simulated persistent file lock" in caplog.text
    assert (pipeline.config.QUARANTINE_DIR / "failed_files" / "failed_files.csv").exists()

def test_unknown_service_reference_is_quarantined(tmp_path):
    inp = tmp_path / "unknown_service_in"
    inp.mkdir()
    record = {"timestamp": "2026-01-01T00:00:00Z", "level": "ERROR",
              "service": "unknown_service", "message": "must be rejected"}
    (inp / "unknown.jsonl").write_text(json.dumps(record) + "\n", encoding="utf-8")
    db = tmp_path / "unknown_service_db" / "logs.db"
    result = run_pipeline(inp, db)
    assert result["rejected"] == 1 and result["output"] == 0
    con = sqlite3.connect(db)
    assert con.execute("SELECT COUNT(*) FROM log_events").fetchone()[0] == 0
    assert con.execute("PRAGMA foreign_key_list(log_events)").fetchone()[2] == "service_catalog"
    con.close()

def test_service_catalog_and_log_foreign_key_are_enforced(tmp_path):
    from src.database import connect
    db = tmp_path / "fk_db" / "logs.db"
    con = connect(db)
    assert con.execute("SELECT COUNT(*) FROM service_catalog").fetchone()[0] >= 1
    try:
        con.execute("INSERT INTO log_events (record_id,event_fingerprint,timestamp,level,service,service_id,message,source_file,source_line_number,run_id) VALUES ('r','f','2026-01-01T00:00:00Z','INFO','unknown','NO_SUCH_ID','x','x.jsonl',1,'run')")
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError("SQLite foreign-key constraint did not reject an unknown service_id")
    con.close()


def test_force_reprocess_bypasses_hash_skip(tmp_path):
    inp=tmp_path/"force_in"; inp.mkdir()
    line=json.dumps({"timestamp":"2026-01-01T00:00:00Z","level":"INFO","service":"api","message":"same"})
    (inp/"x.jsonl").write_text(line+"\n")
    db=tmp_path/"force_db"/"logs.db"
    first=run_pipeline(inp,db)
    second=run_pipeline(inp,db,force_reprocess=True)
    assert first["output"] == 1
    assert second["input"] == 1 and second["duplicate"] == 1


def test_modified_file_is_reprocessed(tmp_path):
    inp=tmp_path/"modified_in"; inp.mkdir()
    path=inp/"x.jsonl"
    base={"timestamp":"2026-01-01T00:00:00Z","level":"INFO","service":"api","message":"first"}
    path.write_text(json.dumps(base)+"\n")
    db=tmp_path/"modified_db"/"logs.db"
    run_pipeline(inp,db)
    base["message"]="second"
    path.write_text(json.dumps(base)+"\n")
    result=run_pipeline(inp,db)
    assert result["input"] == 1 and result["output"] == 1


def test_reject_has_filename_line_category_and_reason(tmp_path):
    inp = tmp_path / "reject_in"
    inp.mkdir()
    (inp / "bad.jsonl").write_text('{bad json}\n', encoding="utf-8")
    run_pipeline(inp, tmp_path / "reject_db" / "logs.db")
    import csv
    # Rejection output is rooted at the project quarantine directory; verify
    # the structured error contract through the run's configured output path.
    from src import config
    with (config.QUARANTINE_DIR / "rejected_logs.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    row = rows[-1]
    assert row["source_file"] == "bad.jsonl"
    assert row["source_line_number"] == "1"
    assert row["error_category"] == "MALFORMED_LOG"
    assert row["rejection_reason"]


def test_database_operational_failure_is_retried_and_logged(tmp_path, monkeypatch, caplog):
    """A transient SQLite write failure should roll back and retry the file."""
    import logging
    import sqlite3
    import src.pipeline as pipeline

    inp = tmp_path / "database_retry_input"
    inp.mkdir()
    record = {
        "timestamp": "2026-01-01T00:00:00Z",
        "level": "ERROR",
        "service": "api",
        "message": "database retry test",
    }
    (inp / "database_retry.jsonl").write_text(
        json.dumps(record) + "\n", encoding="utf-8"
    )

    original_insert_events = pipeline.insert_events
    calls = {"count": 0}

    def fail_once(conn, rows):
        calls["count"] += 1
        if calls["count"] == 1:
            raise sqlite3.OperationalError("simulated temporary database lock")
        return original_insert_events(conn, rows)

    monkeypatch.setattr(pipeline, "insert_events", fail_once)
    caplog.set_level(logging.WARNING, logger="application_log_pipeline")

    db_path = tmp_path / "database_retry" / "logs.db"
    result = pipeline.run_pipeline(inp, db_path)

    assert calls["count"] == 2
    assert result["output"] == 1
    assert result["failed_files"] == 0
    assert "Retrying file database_retry.jsonl" in caplog.text
    assert "simulated temporary database lock" in caplog.text

    con = sqlite3.connect(db_path)
    assert con.execute("SELECT COUNT(*) FROM log_events").fetchone()[0] == 1
    attempts = con.execute(
        "SELECT attempt_number, status FROM file_attempts ORDER BY attempt_number"
    ).fetchall()
    con.close()
    assert attempts == [(1, "RETRYING"), (2, "SUCCESS")]


def test_persistent_database_failure_is_recorded_after_retries(tmp_path, monkeypatch, caplog):
    """A persistent SQLite write failure must fail the file after max attempts."""
    import logging
    import sqlite3
    import src.pipeline as pipeline

    inp = tmp_path / "database_down_input"
    inp.mkdir()
    record = {
        "timestamp": "2026-01-01T00:00:00Z",
        "level": "ERROR",
        "service": "api",
        "message": "database unavailable test",
    }
    (inp / "database_down.jsonl").write_text(
        json.dumps(record) + "\n", encoding="utf-8"
    )

    def always_fail(conn, rows):
        raise sqlite3.OperationalError("simulated persistent database outage")

    monkeypatch.setattr(pipeline, "insert_events", always_fail)
    caplog.set_level(logging.WARNING, logger="application_log_pipeline")
    db_path = tmp_path / "database_down" / "logs.db"
    result = pipeline.run_pipeline(inp, db_path)

    assert result["failed_files"] == 1
    assert result["output"] == 0
    assert "Retrying file database_down.jsonl" in caplog.text
    assert "File database_down.jsonl failed" in caplog.text

    con = sqlite3.connect(db_path)
    attempts = con.execute(
        "SELECT attempt_number, status FROM file_attempts ORDER BY attempt_number"
    ).fetchall()
    state = con.execute(
        "SELECT status, attempt_count, last_error FROM file_processing_state"
    ).fetchone()
    stored = con.execute("SELECT COUNT(*) FROM log_events").fetchone()[0]
    con.close()

    assert attempts == [(1, "RETRYING"), (2, "RETRYING"), (3, "FAILED")]
    assert state[0] == "FAILED" and state[1] == 3
    assert "simulated persistent database outage" in state[2]
    assert stored == 0
