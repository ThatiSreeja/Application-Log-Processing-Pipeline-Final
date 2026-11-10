Application Log Processing Pipeline

1. Project Overview

The Application Log Processing Pipeline is a Python-based data engineering project that processes application logs from structured JSONL files and semi-structured text files.

The pipeline reads log files, parses and normalizes records, validates the data, identifies duplicate events, and stores valid records in the database. Invalid records are rejected and recorded with specific error categories.

The project also supports safe reruns, file change detection, failure handling, audit tracking, and performance measurement.

2. Key Features

Multiple Log Formats: Supports structured JSONL and semi-structured text log files.

Log Parsing: Extracts relevant information from application logs.

Timestamp Normalization: Converts timestamps into a consistent UTC format.

Data Validation: Checks required fields, timestamps, severity levels, and latency values.

Duplicate Detection: Uses deterministic event fingerprints to prevent duplicate event insertion.

Data Quarantine: Separates invalid records and records their rejection reasons.

Schema Drift Detection: Identifies and reports unexpected fields or schema changes.

SHA-256 Change Detection: Calculates file content hashes to identify unchanged and modified files.

Pipeline Version Tracking: Uses the pipeline version along with the file hash to determine whether a file needs reprocessing.

Forced Reprocessing: Allows files to be processed again even when their content has not changed.

Retry Mechanism: Retries failed operations up to the configured number of attempts.

Exponential Backoff: Increases the waiting time between retry attempts.

Audit Tracking: Records pipeline execution details and processing metrics.

Performance Benchmarking: Measures processing time, throughput, and memory usage.

Automated Testing: Includes tests for parsing, validation, deduplication, retries, and idempotency.

3. Technologies Used

Python

SQLite / database storage

Pytest

JSON and JSONL

Git and GitHub

4. Project Structure

Application-Log-Processing-Pipeline/
│
├── src/
│   ├── __init__.py
│   ├── pipeline.py
│   ├── parser.py
│   ├── validation.py
│   └── ...
│
├── tests/
│   ├── test_parser.py
│   ├── test_pipeline.py
│   ├── test_validation.py
│   ├── test_deduplication.py
│   ├── test_generator.py
│   ├── test_quarantine_audit.py
│   └── test_comprehensive.py
│
├── scripts/
│   ├── generate_data.py
│   └── benchmark.py
│
├── data/
│   ├── input/
│   ├── output/
│   └── ...
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── design_notes.md
│   ├── setup_and_testing.md
│   ├── benchmark.md
│   └── ...
│
├── reports/
├── requirements.txt
├── README.md
└── force_run.py

The structure above is representative; retain the actual filenames and folders present in your repository.

5. Installation and Setup

Step 1: Clone the repository

git clone YOUR_GITHUB_REPOSITORY_URL

Step 2: Navigate to the project folder

cd Application-Log-Processing-Pipeline

Step 3: Create a virtual environment

python -m venv .venv

Step 4: Activate the virtual environment

For Windows PowerShell:

.venv\Scripts\Activate.ps1

Step 5: Install dependencies

python -m pip install -r requirements.txt

6. Generate Synthetic Data

Generate reproducible sample log files using:

python -m scripts.generate_data --count 1000 --seed 42

This generates:

1,000 structured JSONL records.

1,000 semi-structured text records.

The generated files are placed in the data/input directory.

7. Run the Pipeline

Execute the pipeline using:

python -m src.pipeline

The pipeline performs the following operations:

Reads input log files.

Checks file hashes and pipeline version.

Skips unchanged files when appropriate.

Parses and normalizes log records.

Validates records.

Identifies duplicate events.

Stores valid records.

Quarantines invalid records.

Records audit information.

Displays the execution summary.

8. Change Detection and Idempotency

The pipeline uses SHA-256 hashing to detect changes in input files.

A file is skipped when both its content hash and pipeline version match the last successful run.

If the file is unchanged, it is skipped.

If the file content changes, it is reprocessed.

If the pipeline version changes, the file is eligible for reprocessing.

This avoids unnecessary processing and improves pipeline efficiency.

Forced Reprocessing

Sometimes, we may intentionally want to process the same files again.

The pipeline supports forced reprocessing through the force_reprocess parameter.

Create a file named force_run.py with:

from src.pipeline import run_pipeline

result = run_pipeline(force_reprocess=True)
print(result)

Execute it using:

python force_run.py

Forced reprocessing bypasses the file-level skip check.

However, event fingerprints still identify previously processed events and prevent duplicate insertion.

This ensures that processing the same files again does not unnecessarily increase the number of database records.

9. Duplicate Detection

The pipeline uses deterministic event fingerprints to identify previously processed events.

When the same event is encountered again:

The fingerprint is compared with existing events.

Existing events are not inserted again.

New events can be inserted normally.

This helps maintain data consistency during repeated or forced pipeline executions.

10. Error Handling and Retry Mechanism

The pipeline handles temporary failures using retry logic.

Examples include:

Temporary database connection failures.

Temporary file access failures.

Storage operation failures.

Failed operations are retried up to the configured maximum of three attempts.

Exponential backoff increases the waiting time between retry attempts.

If the operation continues to fail, the failure is recorded for later investigation and retry.

After resolving the underlying issue, the pipeline can be executed again.

11. Pipeline Execution Status

The pipeline reports execution status based on processing results.

Status

Meaning

SUCCESS

Processing completed without reported record-level rejection or failure.

PARTIAL_SUCCESS

Processing completed, but some records were rejected or data-quality issues were detected.

FAILURE

The pipeline encountered a failure that prevented successful completion.

A PARTIAL_SUCCESS status does not necessarily mean the pipeline is broken.

For example, invalid timestamps, missing required fields, and invalid latency values may cause records to be rejected while valid records are processed successfully.

12. Run Automated Tests

Execute the test suite using:

python -m pytest -v

The tests cover:

JSON and text log parsing.

Timestamp normalization.

Record validation.

Duplicate detection.

Deterministic record IDs.

Idempotent reruns.

Forced reprocessing.

Modified file detection.

Schema drift handling.

Quarantine and audit tracking.

Temporary and persistent failure handling.

The verified test run for this project completed with 35 passed tests.

13. Performance Benchmarking

Run the benchmark using:

python -m scripts.benchmark

The benchmark evaluates:

Different input data sizes.

Processing execution time.

Records processed per second.

Peak Python memory usage.

Performance results are saved in the reports directory.

Refer to docs/benchmark.md for additional details.

14. Documentation

Additional project documentation is available in the docs directory.

It includes:

Architecture documentation.

Data dictionary.

Design notes.

Setup and testing instructions.

Benchmark results.

Data validation and schema information.

15. Conclusion

This project demonstrates an application log processing pipeline with parsing, validation, deduplication, data quarantine, audit tracking, change detection, safe reruns, and failure handling.

It focuses on reliable data processing, data quality, maintainability, and reproducible testing.