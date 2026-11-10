
import csv
import json
import uuid
import time
import hashlib
import logging

from datetime import datetime, timezone
from pathlib import Path

from . import config
from .ingestion import discover_input_files, read_log_file
from .parser import parse_log
from .normalization import normalize_level, normalize_timestamp
from .validation import validate_record
from .schema import inspect_schema, SCHEMA_VERSION
from .deduplication import stable_record_id, event_fingerprint
from .database import connect, insert_event, insert_events, lookup_service_id
from .audit import append_audit
from .analytics import build_summaries


logger = logging.getLogger("application_log_pipeline")
PIPELINE_VERSION = "2.0.0"

MAX_FILE_ATTEMPTS = config.MAX_FILE_ATTEMPTS
RETRY_BASE_DELAY_SECONDS = config.RETRY_BASE_DELAY_SECONDS
TRANSIENT_FILE_ERRORS = (OSError, TimeoutError)


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_file_hash(path):
    """Calculate SHA-256 hash of a file's contents."""

    sha256 = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def _observed_type(value):
    if value is None:
        return "null"

    if isinstance(value, bool):
        return "boolean"

    if isinstance(value, (int, float)):
        return "number"

    if isinstance(value, str):
        return "string"

    if isinstance(value, list):
        return "array"

    if isinstance(value, dict):
        return "object"

    return type(value).__name__


def _record_schema_changes(
    conn, run_id, source_file, record, unexpected, observed_at
):
    for field in unexpected:
        conn.execute(
            """
            INSERT INTO schema_change_events
            (
                run_id, source_file, schema_version, field_name,
                change_type, observed_type, observed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                source_file,
                SCHEMA_VERSION,
                field,
                "ADDITIVE_FIELD",
                _observed_type(record.get(field)),
                observed_at,
            ),
        )

    for field, value in record.items():
        if field in {
            "timestamp",
            "level",
            "service",
            "message",
            "request_id",
            "latency_ms",
        } and value is not None:

            expected = "number" if field == "latency_ms" else "string"
            observed = _observed_type(value)

            if observed != expected:
                conn.execute(
                    """
                    INSERT INTO schema_change_events
                    (
                        run_id, source_file, schema_version, field_name,
                        change_type, observed_type, observed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        source_file,
                        SCHEMA_VERSION,
                        field,
                        "INCOMPATIBLE_TYPE",
                        observed,
                        observed_at,
                    ),
                )


def _write_rejects(items):
    """Append rejected/duplicate records with machine-readable categories."""

    if not items:
        return

    path = config.QUARANTINE_DIR / "rejected_logs.csv"
    path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "run_id",
        "source_file",
        "source_line_number",
        "raw_log",
        "error_category",
        "rejection_reason",
    ]

    # Migrate an older quarantine CSV if its columns differ.
    if path.exists() and path.stat().st_size:
        with path.open("r", newline="", encoding="utf-8") as existing_file:
            reader = csv.DictReader(existing_file)
            old_fields = reader.fieldnames or []
            old_rows = list(reader)

        if old_fields != fields:
            migrated = []

            for old_row in old_rows:
                reason = old_row.get("rejection_reason", "")

                migrated.append({
                    **old_row,
                    "error_category": (
                        old_row.get("error_category")
                        or _error_code(reason)
                    ),
                })

            with path.open("w", newline="", encoding="utf-8") as rewritten:
                writer = csv.DictWriter(
                    rewritten,
                    fieldnames=fields,
                    extrasaction="ignore",
                )
                writer.writeheader()
                writer.writerows(migrated)

    exists = path.exists() and path.stat().st_size > 0

    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)

        if not exists:
            writer.writeheader()

        writer.writerows(items)


def _error_code(reason):
    """Map human-readable failures to stable error categories."""

    text = reason.lower()

    if "missing required field" in text or "missing required" in text:
        return "MISSING_FIELD"

    if "timestamp" in text or "date/time" in text:
        return "INVALID_TIMESTAMP"

    if "severity" in text or "level" in text:
        return "INVALID_SEVERITY"

    if "latency" in text:
        return "INVALID_LATENCY"

    if "unknown service reference" in text or "service reference" in text:
        return "UNKNOWN_SERVICE"

    if "type" in text:
        return "SCHEMA_TYPE_MISMATCH"

    if "malformed json" in text or "unsupported plain-text" in text:
        return "MALFORMED_LOG"

    if "json log must be an object" in text:
        return "INVALID_RECORD_STRUCTURE"

    if "duplicate" in text:
        return "DUPLICATE_EVENT"

    return "RECORD_VALIDATION_ERROR"


def _write_error_log(items):
    if not items:
        return

    path = config.ERROR_LOG_PATH
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a", encoding="utf-8") as handle:
        for item in items:
            handle.write(json.dumps(item, sort_keys=True) + "\n")


def _write_report(stats):
    config.REPORT_DIR.mkdir(parents=True, exist_ok=True)

    report_text = (
        "APPLICATION LOG PIPELINE VALIDATION REPORT\n"
        f"Schema contract version: {SCHEMA_VERSION}\n"
        f"Run ID: {stats['run_id']}\n"
        f"Input records: {stats['input']}\n"
        f"New valid records: {stats['output']}\n"
        f"Duration seconds: {stats.get('duration_seconds', 0)}\n"
        f"Records per second: {stats.get('records_per_second', 0)}\n"
        f"Rejected records: {stats['rejected']}\n"
        f"Duplicate records: {stats['duplicate']}\n"
        f"Skipped files: {stats['skipped_files']}\n"
        f"Quarantine categories: "
        f"{json.dumps(stats.get('error_category_counts', {}), sort_keys=True)}\n"
        f"Schema-drift records (additive fields): {stats['schema_drift']}\n"
        f"Unexpected fields observed: "
        f"{', '.join(sorted(stats['unexpected_fields'])) or 'None'}\n"
        f"Failed files after retries: {stats['failed_files']}\n"
        f"Disposition check: "
        f"{stats['input']} = "
        f"{stats['output'] + stats['rejected'] + stats['duplicate']}\n"
        f"Status: {stats['status']}\n"
    )

    (
        config.REPORT_DIR / "validation_report.txt"
    ).write_text(report_text, encoding="utf-8")


def run_pipeline(input_dir=None, db_path=None, force_reprocess=False):
    config.LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(config.LOG_FILE, encoding="utf-8")],
        force=False,
    )
    input_dir = Path(input_dir or config.INPUT_DIR)
    db_path = Path(db_path or config.DB_PATH)

    run_id = str(uuid.uuid4())
    started = now()
    run_clock = time.perf_counter()

    stats = {
        "run_id": run_id,
        "input": 0,
        "output": 0,
        "rejected": 0,
        "duplicate": 0,
        "failed_files": 0,
        "skipped_files": 0,
        "schema_drift": 0,
        "unexpected_fields": set(),
    }

    rejects = []
    failure_reasons = []
    structured_errors = []
    error_category_counts = {}

    conn = connect(db_path)
    logger.info("Pipeline run %s started (version=%s)", run_id, PIPELINE_VERSION)

    for path in discover_input_files(input_dir):

        # Calculate current file content hash.
        file_hash = get_file_hash(path)

        # Check whether this file was successfully processed before.
        previous = conn.execute(
            """
            SELECT status, content_hash, pipeline_version
            FROM file_processing_state
            WHERE source_file = ?
            """,
            (path.name,),
        ).fetchone()

        # Skip the file if it has not changed since its last successful run.
        if (
            previous
            and previous[0] == "SUCCESS"
            and previous[1] == file_hash
            and previous[2] == PIPELINE_VERSION
            and not force_reprocess
        ):
            logger.info("Skipping unchanged file %s", path.name)
            stats["skipped_files"] += 1
            continue

        completed = False
        last_error = ""

        for attempt in range(1, MAX_FILE_ATTEMPTS + 1):

            local = {
                "input": 0,
                "output": 0,
                "rejected": 0,
                "duplicate": 0,
                "schema_drift": 0,
            }

            local_rejects = []
            local_unexpected = set()
            pending_rows = []
            pending_fingerprints = set()

            conn.execute("SAVEPOINT source_file")

            try:
                for line_no, raw in read_log_file(path):

                    local["input"] += 1

                    meta = {
                        "run_id": run_id,
                        "source_file": path.name,
                        "source_line_number": line_no,
                        "raw_log": raw,
                    }

                    try:
                        rec = parse_log(raw)

                        schema_errors, unexpected = inspect_schema(rec)

                        _record_schema_changes(
                            conn,
                            run_id,
                            path.name,
                            rec,
                            unexpected,
                            now(),
                        )

                        if schema_errors:
                            raise ValueError("; ".join(schema_errors))

                        ts = normalize_timestamp(rec.get("timestamp"))
                        level = normalize_level(rec.get("level"))

                        rec["_normalized_timestamp"] = ts
                        rec["_normalized_level"] = level

                        reasons = validate_record(rec)

                        if reasons:
                            raise ValueError("; ".join(reasons))

                        latency = (
                            float(rec["latency_ms"])
                            if rec.get("latency_ms") is not None
                            else None
                        )

                        clean = {
                            "timestamp": ts,
                            "level": level,
                            "service": rec["service"].strip(),
                            "message": rec["message"].strip(),
                            "request_id": rec.get("request_id"),
                            "latency_ms": latency,
                        }

                        service_id = lookup_service_id(
                            conn,
                            clean["service"],
                        )

                        if service_id is None:
                            raise ValueError(
                                f"Unknown service reference: {clean['service']}"
                            )

                        fp = event_fingerprint(clean)

                        rid = stable_record_id(
                            path.name,
                            line_no,
                            raw,
                        )

                        extra = {
                            key: value
                            for key, value in rec.items()
                            if key not in {
                                "timestamp",
                                "level",
                                "service",
                                "message",
                                "request_id",
                                "latency_ms",
                                "_normalized_timestamp",
                                "_normalized_level",
                            }
                        }

                        row = (
                            rid,
                            fp,
                            ts,
                            level,
                            clean["service"],
                            service_id,
                            clean["message"],
                            clean["request_id"],
                            latency,
                            path.name,
                            line_no,
                            run_id,
                            json.dumps(
                                extra,
                                sort_keys=True,
                                default=str,
                            ),
                        )

                        already_exists = conn.execute(
                            "SELECT 1 FROM log_events WHERE event_fingerprint = ?",
                            (fp,),
                        ).fetchone() is not None or fp in pending_fingerprints

                        if already_exists:
                            local["duplicate"] += 1
                            duplicate_reason = (
                                "Duplicate event "
                                "(same normalized event already processed)"
                            )
                            local_rejects.append({
                                **meta,
                                "error_category": _error_code(duplicate_reason),
                                "rejection_reason": duplicate_reason,
                            })
                        else:
                            pending_rows.append(row)
                            pending_fingerprints.add(fp)

                        if unexpected:
                            local["schema_drift"] += 1
                            local_unexpected.update(unexpected)

                    except Exception as e:

                        local["rejected"] += 1

                        category = _error_code(str(e))

                        local_rejects.append({
                            **meta,
                            "error_category": category,
                            "rejection_reason": str(e),
                        })

                        structured_errors.append({
                            "run_id": run_id,
                            "source_file": path.name,
                            "line_number": line_no,
                            "error_category": category,
                            "error_code": category,
                            "message": str(e),
                            "occurred_at": now(),
                        })

                # Flush accepted records in one SQLite executemany batch.
                local["output"] += insert_events(conn, pending_rows)

                # Mark this file attempt as successful.
                conn.execute("RELEASE SAVEPOINT source_file")

                conn.execute(
                    """
                    INSERT INTO file_attempts
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        path.name,
                        attempt,
                        "SUCCESS",
                        None,
                        now(),
                    ),
                )

                # Save successful file state and its content hash.
                conn.execute(
                    """
                    INSERT INTO file_processing_state
                    (
                        source_file,
                        status,
                        attempt_count,
                        last_error,
                        last_attempt_at,
                        updated_at,
                        content_hash,
                        pipeline_version
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)

                    ON CONFLICT(source_file) DO UPDATE SET
                        status = 'SUCCESS',
                        attempt_count =
                            file_processing_state.attempt_count + 1,
                        last_error = NULL,
                        last_attempt_at = excluded.last_attempt_at,
                        updated_at = excluded.updated_at,
                        content_hash = excluded.content_hash
                    """,
                    (
                        path.name,
                        "SUCCESS",
                        1,
                        None,
                        now(),
                        now(),
                        file_hash,
                        PIPELINE_VERSION,
                    ),
                )

                conn.commit()

                # Update global statistics only after successful file commit.
                for key in (
                    "input",
                    "output",
                    "rejected",
                    "duplicate",
                    "schema_drift",
                ):
                    stats[key] += local[key]

                rejects.extend(local_rejects)

                for rejected_item in local_rejects:

                    category = (
                        rejected_item.get("error_category")
                        or _error_code(
                            rejected_item.get("rejection_reason", "")
                        )
                    )

                    error_category_counts[category] = (
                        error_category_counts.get(category, 0) + 1
                    )

                stats["unexpected_fields"].update(local_unexpected)

                completed = True
                break

            except Exception as e:

                last_error = f"{type(e).__name__}: {e}"

                conn.execute("ROLLBACK TO SAVEPOINT source_file")
                conn.execute("RELEASE SAVEPOINT source_file")

                retryable = (
                    isinstance(e, TRANSIENT_FILE_ERRORS)
                    or e.__class__.__module__ == "sqlite3"
                )

                attempt_status = (
                    "RETRYING"
                    if retryable and attempt < MAX_FILE_ATTEMPTS
                    else "FAILED"
                )

                attempted_at = now()

                conn.execute(
                    """
                    INSERT INTO file_attempts
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        path.name,
                        attempt,
                        attempt_status,
                        last_error,
                        attempted_at,
                    ),
                )

                conn.execute(
                    """
                    INSERT INTO file_processing_state
                    (
                        source_file,
                        status,
                        attempt_count,
                        last_error,
                        last_attempt_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)

                    ON CONFLICT(source_file) DO UPDATE SET
                        status = excluded.status,
                        attempt_count =
                            file_processing_state.attempt_count + 1,
                        last_error = excluded.last_error,
                        last_attempt_at = excluded.last_attempt_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        path.name,
                        attempt_status,
                        1,
                        last_error,
                        attempted_at,
                        attempted_at,
                    ),
                )

                conn.commit()

                if not retryable or attempt >= MAX_FILE_ATTEMPTS:
                    break

                logger.warning(
                    "Retrying file %s after attempt %s/%s due to %s: %s",
                    path.name,
                    attempt,
                    MAX_FILE_ATTEMPTS,
                    type(e).__name__,
                    e,
                )

                time.sleep(
                    RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                )

        if not completed:
            logger.error("File %s failed: %s", path.name, last_error)
            stats["failed_files"] += 1

            failure_reasons.append(
                f"{path.name} failed after "
                f"{MAX_FILE_ATTEMPTS} attempts: {last_error}"
            )

    # Record files that failed after all retry attempts.
    if failure_reasons:

        dead_dir = config.QUARANTINE_DIR / "failed_files"
        dead_dir.mkdir(parents=True, exist_ok=True)

        with (
            dead_dir / "failed_files.csv"
        ).open("a", newline="", encoding="utf-8") as handle:

            writer = csv.writer(handle)

            if handle.tell() == 0:
                writer.writerow([
                    "run_id",
                    "source_file",
                    "reason",
                    "recorded_at",
                ])

            for reason in failure_reasons:
                writer.writerow([
                    run_id,
                    reason.split(" failed after ")[0],
                    reason,
                    now(),
                ])

    # Calculate final database count.
    db_total = conn.execute(
        "SELECT COUNT(*) FROM log_events"
    ).fetchone()[0]

    duration = time.perf_counter() - run_clock

    stats["duration_seconds"] = round(duration, 4)

    stats["records_per_second"] = (
        round(stats["input"] / duration, 2)
        if duration
        else 0
    )

    status = (
        "FAILED"
        if stats["failed_files"] and not stats["output"]
        else (
            "PARTIAL_SUCCESS"
            if (
                stats["rejected"]
                or stats["duplicate"]
                or stats["failed_files"]
            )
            else "SUCCESS"
        )
    )

    # Store this pipeline execution in pipeline_runs.
    conn.execute(
        """
        INSERT INTO pipeline_runs
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            started,
            now(),
            stats["input"],
            stats["output"],
            stats["rejected"],
            stats["duplicate"],
            stats["failed_files"],
            status,
            "; ".join(failure_reasons),
        ),
    )

    conn.commit()
    conn.close()

    stats["database_total"] = db_total
    stats["status"] = status
    stats["error_category_counts"] = error_category_counts

    _write_rejects(rejects)

    build_summaries(db_path, config.PROCESSED_DIR)

    _write_report(stats)

    append_audit(
        config.AUDIT_DIR / "audit_log.csv",
        {
            "run_id": run_id,
            "start_time": started,
            "end_time": now(),
            "input_count": stats["input"],
            "output_count": stats["output"],
            "rejected_count": stats["rejected"],
            "duplicate_count": stats["duplicate"],
            "failed_files": stats["failed_files"],
            "status": status,
            "failure_reason": "; ".join(failure_reasons),
            "error_category_counts": json.dumps(
                error_category_counts,
                sort_keys=True,
            ),
        },
    )

    _write_error_log(structured_errors)

    logger.info("Pipeline run %s finished with status %s", run_id, status)

    stats["unexpected_fields"] = sorted(
        stats["unexpected_fields"]
    )

    return stats


if __name__ == "__main__":
    print(json.dumps(run_pipeline(), indent=2))

