# Requirements Traceability Matrix

This document maps the application-log pipeline brief to the implementation and the evidence available in this repository.

## Business workflow and deliverables

| Requirement | Implementation / evidence | Status |
|---|---|---|
| Parse structured JSONL and semi-structured text | `src/parser.py`, `src/ingestion.py`; parser tests | Implemented |
| Normalize timestamps and severity | `src/normalization.py`; UTC and severity tests | Implemented |
| Extract fields and preserve additive fields | `src/pipeline.py` stores known fields and serializes extra fields to `extra_fields` | Implemented |
| Validate and publish analytical records | `src/validation.py`, `src/database.py`, `src/analytics.py` | Implemented |
| Profile error-rate and latency summaries | `src/analytics.py`; `data/processed/log_summary.csv` | Implemented |
| Reproducible sample and scalable synthetic data | `scripts/generate_data.py`; fixed-seed generator test | Implemented |
| Safe reruns and incremental processing | SHA-256 content hash, pipeline version, `force_reprocess`, unique event fingerprint | Implemented |
| Audit run IDs, counts, status and failure reasons | `src/audit.py`, `pipeline_runs`, `file_attempts`, `file_processing_state` | Implemented |
| Traceable quarantine | `data/quarantine/rejected_logs.csv` includes source file, line, category and reason | Implemented |
| Reconciliation evidence | `docs/reconciliation_queries.sql`, `reports/validation_report.txt` | Included |
| Automated tests | `tests/`; latest prepared run: 35 passed | Verified in this package |
| Performance measurement and optimization | `scripts/benchmark.py`, `scripts/benchmark_inserts.py`, `reports/performance_report.csv`; SQLite `executemany` batch insert | Measured |
| Setup and design documentation | README and documents in `docs/` | Included |

## Required edge cases

| Edge case | Evidence |
|---|---|
| Malformed input | `test_malformed_text`, `test_json_array_rejected` |
| Missing required field | `test_missing_service_is_rejected` and quarantine tests |
| Duplicate event/key | `test_duplicate_across_rerun_is_not_inserted`, `test_error_code_for_duplicate_is_categorized` |
| Unexpected/additive schema field | `test_schema_drift_additive_fields_are_reported_and_preserved` |
| Incompatible schema type | `test_incompatible_schema_field_type_is_quarantined` |
| Empty batch | `test_empty_input` |
| Partial file failure and retry | `test_transient_file_failure_is_retried_and_succeeds`, `test_failed_file_is_recorded_for_later_run` |
| Late/out-of-order events | `test_late_and_out_of_order_events_preserved` |
| Invalid reference key | `test_unknown_service_reference_is_quarantined`, `test_service_catalog_and_log_foreign_key_are_enforced` |
| Same-input rerun | `test_pipeline_and_rerun_are_idempotent` |
| Modified input and forced reprocessing | `test_modified_file_is_reprocessed`, `test_force_reprocess_bypasses_hash_skip` |
| Transient and persistent database write failure | `test_database_operational_failure_is_retried_and_logged`, `test_persistent_database_failure_is_recorded_after_retries` |

## Conventions and data relationships

- Python modules, database tables and columns use `snake_case`.
- Timestamps are normalized to UTC ISO-8601 strings.
- `record_id` is deterministic for a source file/line/raw record; `event_fingerprint` identifies equivalent normalized events and is unique.
- `service_catalog.service_id` is the referenced service key for `log_events.service_id`.
- `run_id`, `source_file`, and `source_line_number` preserve operational and source lineage.
- Accepted events are append-only; late events are retained rather than replacing earlier events.
- Required and optional fields, uniqueness and foreign-key rules are described in `data_dictionary.md` and `nullability_and_constraints.md`.

## Operational levels and recovery

- INFO: run start/completion and unchanged-file skips.
- WARNING: recoverable file/database failures before retry.
- ERROR: a file that remains failed after the retry limit.
- File-level attempts use a savepoint and retry up to three times with exponential backoff. Record-level invalid rows are quarantined, not retried.
- A later run rediscovers failed files. Stable IDs and unique fingerprints prevent duplicate accepted events on restart.

## Scope limitations

This is a local, single-process SQLite vertical slice. It does not include a cloud deployment, distributed processing, a scheduler, a managed schema registry, or multi-worker orchestration. Those are optional future extensions, not claimed deliverables of this implementation.
