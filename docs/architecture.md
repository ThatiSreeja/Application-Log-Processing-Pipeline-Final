# Architecture

```text
Input files (JSONL / text)
        |
        v
File discovery + SHA-256 fingerprint
        |
        v
Parse -> schema inspection -> normalization -> validation
        |                         |
        | valid                   +--> rejects CSV + structured JSONL errors
        v
SQLite batch insert (executemany)
        |
        +--> file state / attempts / schema changes / pipeline runs
        +--> audit CSV + Python application log
        +--> analytics summaries + validation report
```

Each source file is processed in a SQLite savepoint. Successful files record a SHA-256 content hash and pipeline version. Unchanged files are skipped; changed files or files processed with `force_reprocess=True` are read again. Event fingerprints prevent duplicate events from being inserted. Accepted rows are flushed in a batch per file.


Operational visibility: INFO logs record run start/completion and skip decisions; WARNING logs identify recoverable file/database errors before retry; ERROR logs identify files that remain failed after all attempts. Console and file logging are enabled, with structured record errors and audit counts stored separately.
