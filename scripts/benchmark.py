"""Reproducible end-to-end benchmark (runtime and peak Python memory)."""
import argparse
import csv
import tempfile
import time
import tracemalloc
from pathlib import Path
from scripts.generate_data import generate
from src.pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--counts", default="1000,10000,100000")
    args = parser.parse_args()
    output = Path("reports/performance_report.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for count in [int(value) for value in args.counts.split(",")]:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir, db_path = root / "input", root / "processed" / "logs.db"
            generate(count, input_dir, seed=42)
            tracemalloc.start()
            started = time.perf_counter()
            result = run_pipeline(input_dir, db_path)
            elapsed = time.perf_counter() - started
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            rows.append({
                "records_generated_per_format": count,
                "input_records": result["input"],
                "new_valid_records": result["output"],
                "rejected_records": result["rejected"],
                "duplicate_records": result["duplicate"],
                "elapsed_seconds": round(elapsed, 4),
                "records_per_second": round(result["input"] / elapsed, 2) if elapsed else 0,
                "peak_python_memory_mb": round(peak_bytes / (1024 * 1024), 2),
            })
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Benchmark saved to {output}")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
