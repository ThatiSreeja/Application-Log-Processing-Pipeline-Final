
import sqlite3
from pathlib import Path

from . import config


SCHEMA = """
CREATE TABLE IF NOT EXISTS service_catalog (
    service_id TEXT PRIMARY KEY,
    service_name TEXT NOT NULL UNIQUE COLLATE NOCASE
);

CREATE TABLE IF NOT EXISTS log_events (
    record_id TEXT PRIMARY KEY,
    event_fingerprint TEXT NOT NULL UNIQUE,
    timestamp TEXT NOT NULL,
    level TEXT NOT NULL,
    service TEXT NOT NULL,
    service_id TEXT NOT NULL REFERENCES service_catalog(service_id),
    message TEXT NOT NULL,
    request_id TEXT,
    latency_ms REAL,
    source_file TEXT NOT NULL,
    source_line_number INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    extra_fields TEXT
);

CREATE TABLE IF NOT EXISTS file_processing_state (
    source_file TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TEXT,
    updated_at TEXT NOT NULL,
    content_hash TEXT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS schema_change_events (
    change_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    field_name TEXT NOT NULL,
    change_type TEXT NOT NULL,
    observed_type TEXT,
    observed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_attempts (
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    attempted_at TEXT NOT NULL,
    PRIMARY KEY (run_id, source_file, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_logs_timestamp
ON log_events(timestamp);

CREATE INDEX IF NOT EXISTS idx_logs_service
ON log_events(service);

CREATE INDEX IF NOT EXISTS idx_logs_level
ON log_events(level);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    start_time TEXT NOT NULL,
    end_time TEXT,
    input_count INTEGER NOT NULL DEFAULT 0,
    output_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    failed_files INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    failure_reason TEXT
);
"""


def connect(db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)

    conn.execute("PRAGMA foreign_keys = ON")

    conn.executescript(SCHEMA)

    # Migration for databases created before content_hash was added.
    columns = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info(file_processing_state)"
        )
    }

    if "content_hash" not in columns:
        conn.execute("ALTER TABLE file_processing_state ADD COLUMN content_hash TEXT")
    if "pipeline_version" not in columns:
        conn.execute("ALTER TABLE file_processing_state ADD COLUMN pipeline_version TEXT")

    conn.executemany(
        """
        INSERT OR IGNORE INTO service_catalog
        (service_id, service_name)
        VALUES (?, ?)
        """,
        [
            (service_id, name)
            for service_id, name in config.SERVICE_REGISTRY.items()
        ],
    )

    conn.commit()

    return conn


def lookup_service_id(conn, service_name):
    row = conn.execute(
        """
        SELECT service_id
        FROM service_catalog
        WHERE service_name = ? COLLATE NOCASE
        """,
        (service_name.strip(),),
    ).fetchone()

    return row[0] if row else None


def insert_event(conn, row):
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO log_events
        (
            record_id,
            event_fingerprint,
            timestamp,
            level,
            service,
            service_id,
            message,
            request_id,
            latency_ms,
            source_file,
            source_line_number,
            run_id,
            extra_fields
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row,
    )

    return cur.rowcount == 1


def insert_events(conn, rows):
    """Insert a batch of events and return the number newly inserted.

    INSERT OR IGNORE preserves the event fingerprint uniqueness guarantee.
    The returned count is SQLite's aggregate rowcount for the batch.
    """
    rows = list(rows)
    if not rows:
        return 0
    cur = conn.executemany(
        """INSERT OR IGNORE INTO log_events
        (record_id,event_fingerprint,timestamp,level,service,service_id,message,
         request_id,latency_ms,source_file,source_line_number,run_id,extra_fields)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    return max(cur.rowcount, 0)
