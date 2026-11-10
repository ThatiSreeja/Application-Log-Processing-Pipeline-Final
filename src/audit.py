import csv
from pathlib import Path

FIELDS = [
    "run_id", "start_time", "end_time", "input_count", "output_count",
    "rejected_count", "duplicate_count", "failed_files", "status",
    "failure_reason", "error_category_counts",
]


def append_audit(path, row):
    """Append one run summary, preserving older audit rows when the schema grows."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and path.stat().st_size:
        with path.open("r", newline="", encoding="utf-8") as existing_file:
            reader = csv.DictReader(existing_file)
            old_fields = reader.fieldnames or []
            old_rows = list(reader)
        if old_fields != FIELDS:
            with path.open("w", newline="", encoding="utf-8") as rewritten:
                writer = csv.DictWriter(rewritten, fieldnames=FIELDS)
                writer.writeheader()
                for old_row in old_rows:
                    writer.writerow({field: old_row.get(field, "") for field in FIELDS})

    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in FIELDS})
