# Nullability and Constraint Specification

**Source contract version:** 1.0  
**Target database:** SQLite  
**Policy:** Required source fields must be present, non-null, and non-blank. Optional source fields may be absent or explicitly `null`. Empty strings are not treated as valid values for required fields.

## 1. Source log record

| Field | Source type | Nullable? | Required? | Rules |
|---|---|---:|---:|---|
| `timestamp` | string | No | Yes | Must parse as a timestamp; normalized to UTC ISO-8601. |
| `level` | string | No | Yes | Must normalize to DEBUG, INFO, WARNING, ERROR, or FATAL. |
| `service` | string | No | Yes | Must match a registered service in `SERVICE_REGISTRY`. |
| `message` | string | No | Yes | Must not be blank. |
| `request_id` | string | Yes | No | Optional correlation identifier; may repeat and is not a key. |
| `latency_ms` | number | Yes | No | If supplied, must be finite and between 0 and configured maximum. |
| Additional JSON fields | Any JSON type | Yes | No | Additive fields are preserved in `extra_fields` and recorded in schema-change history. |

A missing or `null` required field, or a blank required string, causes that source record to be quarantined. An absent or `null` optional field is accepted. An optional field with an incompatible non-null type is rejected. Text-log records are mapped to the same source contract; fields not represented by that format remain null only when optional.

## 2. Target table nullability and keys

### `service_catalog`

| Column | SQLite type | Nullable? | Key / constraint |
|---|---|---:|---|
| `service_id` | TEXT | No | Primary key |
| `service_name` | TEXT | No | Unique, case-insensitive |

### `log_events`

| Column | SQLite type | Nullable? | Key / constraint |
|---|---|---:|---|
| `record_id` | TEXT | No | Primary key; deterministic source-record ID |
| `event_fingerprint` | TEXT | No | Unique; normalized-event deduplication key |
| `timestamp` | TEXT | No | UTC ISO-8601 event time |
| `level` | TEXT | No | Normalized severity |
| `service` | TEXT | No | Required source service name |
| `service_id` | TEXT | No | Foreign key to `service_catalog.service_id` |
| `message` | TEXT | No | Required log message |
| `request_id` | TEXT | Yes | Optional correlation ID; not unique |
| `latency_ms` | REAL | Yes | Optional non-negative finite latency |
| `source_file` | TEXT | No | Source filename for lineage |
| `source_line_number` | INTEGER | No | One-based source line number |
| `run_id` | TEXT | No | Run that first inserted the event |
| `extra_fields` | TEXT | Yes | JSON text for additive source fields |

### Operational and history tables

| Table | Column | Nullable? | Key / meaning |
|---|---|---:|---|
| `pipeline_runs` | `run_id` | No | Primary key |
| `pipeline_runs` | `start_time` | No | Run start UTC time |
| `pipeline_runs` | `end_time` | Yes | Null while a run is in progress |
| `pipeline_runs` | `input_count`, `output_count`, `rejected_count`, `duplicate_count`, `failed_files` | No | Non-null counts; default to zero |
| `pipeline_runs` | `status` | No | Run status |
| `pipeline_runs` | `failure_reason` | Yes | Populated when a run-level failure reason exists |
| `file_processing_state` | `source_file` | No | Primary key; latest state for a file |
| `file_processing_state` | `status`, `attempt_count`, `updated_at` | No | Latest processing state and count |
| `file_processing_state` | `last_error`, `last_attempt_at` | Yes | Most recent error/time; absent before an attempt/error |
| `file_attempts` | `run_id`, `source_file`, `attempt_number` | No | Composite primary key |
| `file_attempts` | `status`, `attempted_at` | No | Attempt result and time |
| `file_attempts` | `error_message` | Yes | Null for successful attempts |
| `schema_change_events` | `change_id` | No | Autoincrement primary key |
| `schema_change_events` | `run_id`, `source_file`, `schema_version`, `field_name`, `change_type`, `observed_at` | No | Required schema-change audit attributes |
| `schema_change_events` | `observed_type` | Yes | Observed type where available |

## 3. Relationship and integrity rules

- `log_events.service_id` must reference an existing `service_catalog.service_id`; SQLite foreign-key enforcement is enabled on each connection.
- `record_id` and `event_fingerprint` must be unique in `log_events`.
- `service_catalog.service_name` must be unique without regard to case.
- `request_id` is deliberately not unique and has no foreign-key relationship in this source contract.
- Rejected records are written to `data/quarantine/rejected_logs.csv` with run ID, source file, source line, raw input, and rejection reason. They are not inserted into `log_events`.

## 4. Derived fields and null behaviour

- `record_id`: SHA-256 derived from source filename, line number, and raw line.
- `event_fingerprint`: SHA-256 derived from normalized event content.
- `error_rate_pct`: `(ERROR + FATAL accepted events / all accepted events) * 100`; reported as zero when there are no accepted events.
- Average, P50, P95, minimum, and maximum latency are calculated from non-null accepted `latency_ms` values. If a service has no non-null latency values, latency statistics are reported as unavailable/blank rather than treating missing latency as zero.

## 5. History policy

`log_events`, `pipeline_runs`, `file_attempts`, and `schema_change_events` are historical/append-oriented records. `file_processing_state` is intentionally a current-state table and is updated as a file progresses; detailed attempts remain in `file_attempts`. A rerun does not overwrite previously accepted event history or create duplicate normalized events.
