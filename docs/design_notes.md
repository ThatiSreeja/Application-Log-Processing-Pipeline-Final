# Design Notes and Trade-offs

- Python standard library keeps installation lightweight and avoids native compilation; pytest is the only project dependency.
- JSON uses the standard JSON parser; documented semi-structured text uses a compiled regular expression.
- Datetime normalizes timezone-aware values to UTC; naive values are explicitly assumed UTC.
- SQLite is the durable local store. Primary/unique constraints enforce stable source IDs and normalized event deduplication.
- Each file is isolated by a savepoint. Bad individual lines are quarantined; a file-level failure can roll back that file while later files continue.
- Accepted events are append-only history. Late/out-of-order events are retained by event timestamp.
- Extra JSON fields are retained rather than silently dropped. Unsupported source-format changes are quarantined or require a new versioned parser.
- The pipeline streams input line-by-line, indexes timestamp/service/severity and measures throughput with a reproducible benchmark script.
- Trade-off: SQLite and one process are appropriate for a 0–3 year, 72-hour local vertical slice. Kafka/Spark/Airflow/cloud storage would add operational complexity not required by this scope.
- Limitation: there is no external scheduler or automatic retry queue. A rerun is safe after a transient file problem is fixed.

## Retry, restart and recovery policy

- Each source file is processed in its own SQLite savepoint. If reading or processing the file raises a file-level exception, all writes from that attempt are rolled back; other files can still be processed.
- A failed file is retried automatically up to 3 total attempts. Delays use exponential backoff (0.25 seconds, then 0.5 seconds). Each attempt is recorded in the SQLite `file_attempts` table with attempt number, status, timestamp and error.
- After all attempts fail, the file is marked failed in the run history. Fix the transient cause (for example, file lock or temporary access issue) and rerun the same command. Stable record IDs and unique event fingerprints make the restart idempotent: previously committed events are not inserted twice.
- Line-level invalid data is not retried; it is quarantined because retrying unchanged malformed content cannot fix it. Correct the source and rerun if the record should be reprocessed.

## Explicit schema-change policy

- The input contract is version `1.0`. Required fields (`timestamp`, `level`, `service`, `message`) must be strings; optional `request_id` must be a string and `latency_ms` must be numeric.
- Additive/unknown JSON fields are treated as non-breaking schema drift: they are counted and named in the validation report and retained in `extra_fields` so information is not silently lost.
- A type change to a known field, missing required field, invalid severity/timestamp, or unsupported record format is rejected to quarantine with a reason. The pipeline does not guess how to coerce a breaking schema change.
- For a breaking upstream change, increment the documented schema contract version and implement/test a version-specific parser or migration before accepting it. This is a local contract check, not a full schema registry or automatic migration service.

## Retry, restart, and failed-file recovery

- Each source-file attempt is isolated with a SQLite savepoint. An unsuccessful attempt is rolled back before another attempt begins.
- Transient file/timeout and SQLite operational failures are retried up to three times with exponential backoff (0.25s, 0.5s). Record-level validation and schema errors are quarantined and are not retried as file failures.
- `file_attempts` stores an append-only attempt history. `file_processing_state` stores the latest state, attempt count, and last error for each source filename across pipeline runs.
- A later pipeline invocation rediscovers input files and tries failed files again. This is the restart/resume mechanism; it does not require a separate scheduler.
- Files still failing after all attempts are listed in `data/quarantine/failed_files/failed_files.csv`. Correct the underlying issue and rerun to recover them. Stable IDs and unique fingerprints make the rerun safe.

## Schema evolution and compatibility policy

- The current input contract is version `1.0`, defined in `src/schema.py`.
- New additive fields are classified as `ADDITIVE_FIELD`, preserved in `extra_fields`, and recorded in SQLite `schema_change_events` with their observed type and source file.
- Incompatible types for known fields are classified as `INCOMPATIBLE_TYPE` and the affected record is quarantined; the rest of the file continues.
- Missing required fields are rejected by validation. Optional fields may be absent.
- Breaking contract changes should be introduced as a deliberate new schema version with updated validators and tests; this project records drift but does not automatically migrate historical database rows.

## Additional operational controls
- Data-quality thresholds (maximum latency and allowed future clock skew) and retry settings are centralized in `src/config.py`.
- Non-finite, negative and over-limit latency values are rejected. Event timestamps too far ahead of processing time are rejected to catch clock/data errors; late historical events remain accepted.
- Structured record errors are appended to `data/audit/pipeline_errors.jsonl` with error codes and source coordinates, without copying raw log messages into the operational error log.
- Benchmark output includes elapsed seconds, throughput and peak Python memory. SQLite indexes support timestamp/service/severity queries; per-file savepoints preserve atomicity.
- Referential integrity is enforced for the `service` dimension. A seeded `service_catalog` table maps canonical service names to stable service IDs; `log_events.service_id` has a SQLite foreign-key constraint. Unknown service names are quarantined before insertion, and SQLite independently enforces the FK. `request_id` remains a correlation identifier because the input contract supplies no request parent table.


## Operational logging levels

- `INFO`: pipeline start/completion and unchanged-file skip decisions.
- `WARNING`: a recoverable file or SQLite operational failure before the next retry attempt.
- `ERROR`: a file that remains failed after its configured retry attempts.

The configured logger writes to the console and `data/audit/pipeline.log`. Structured record-level validation errors are separately appended to `data/audit/pipeline_errors.jsonl`, and rejected raw records with source line numbers are stored in quarantine.
