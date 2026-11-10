# Data Dictionary

## Grain and entities
`log_events` has one row per accepted normalized event. `pipeline_runs` has one row per pipeline execution. `request_id` is a correlation identifier and may repeat; there is no external foreign-key entity in the supplied source contract.

## log_events
- `record_id` TEXT PRIMARY KEY: deterministic SHA-256 source record identifier.
- `event_fingerprint` TEXT UNIQUE: normalized event fingerprint; prevents duplicate events across files and reruns.
- `timestamp` TEXT NOT NULL: UTC ISO-8601 event time.
- `level` TEXT NOT NULL: DEBUG, INFO, WARNING, ERROR or FATAL.
- `service` TEXT NOT NULL: service identifier.
- `message` TEXT NOT NULL: log message.
- `request_id` TEXT NULL: request correlation ID.
- `latency_ms` REAL NULL: non-negative response latency in milliseconds.
- `source_file` TEXT NOT NULL: original filename.
- `source_line_number` INTEGER NOT NULL: one-based line number.
- `run_id` TEXT NOT NULL: run that first accepted this event.
- `extra_fields` TEXT: JSON string holding unrecognized optional JSON fields.

## pipeline_runs
One row per run: run ID, start/end UTC timestamps, input/output/rejected/duplicate counts, failed-file count, status and failure reason.

## rejected_logs.csv
`run_id`, `source_file`, `source_line_number`, `raw_log`, `rejection_reason`. Invalid and duplicate source lines remain traceable.

## Derived metrics
- `error_rate_pct` = (ERROR + FATAL accepted events / all accepted events) * 100.
- Average, P50, P95, minimum and maximum latency are calculated from non-null accepted latency values per service.
- `record_id` is stable for a given filename, line number and raw line. `event_fingerprint` deduplicates normalized event content.


## Referential integrity

- `service_catalog.service_id` TEXT PRIMARY KEY: stable service reference key.
- `service_catalog.service_name` TEXT UNIQUE: canonical service name registered in `src/config.py` (`SERVICE_REGISTRY`).
- `log_events.service_id` TEXT NOT NULL: foreign key to `service_catalog.service_id`. SQLite foreign-key enforcement is enabled for every connection.
- A log whose `service` value is not in the registry is rejected and quarantined with an `Unknown service reference` reason. Register a legitimate new service in configuration before processing it.

## Nullability and constraints

See [`nullability_and_constraints.md`](nullability_and_constraints.md) for the full source and target nullability matrix, table keys, optional-field behaviour, referential-integrity rules, derived-field null behaviour, and history policy. The machine-readable source contract is [`source_schema.json`](source_schema.json).

Summary: `timestamp`, `level`, `service`, and `message` are required and non-null/non-blank. `request_id` and `latency_ms` are optional and may be null. `extra_fields` is nullable because records need not contain additional JSON properties. Target columns and operational history fields are documented individually in the nullability specification.


## Incremental processing metadata

`file_processing_state` stores `source_file`, `status`, `attempt_count`, `last_error`, `last_attempt_at`, `updated_at`, `content_hash` (SHA-256), and `pipeline_version`. A file is skipped only when its last status is SUCCESS and both hash and pipeline version match. `force_reprocess=True` bypasses this skip rule.

`rejected_logs.csv` includes `run_id`, `source_file`, `source_line_number`, `raw_log`, `error_category`, and `rejection_reason`. `pipeline_errors.jsonl` stores structured error records with source filename and line number.
