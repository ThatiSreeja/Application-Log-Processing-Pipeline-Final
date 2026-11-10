"""Compare SQLite row-at-a-time inserts with executemany batch inserts."""
import argparse
import sqlite3
import tempfile
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()
    rows = [(i, f"message-{i}") for i in range(args.rows)]
    with tempfile.TemporaryDirectory() as folder:
        db = sqlite3.connect(f"{folder}/bench.db")
        db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, message TEXT)")
        start = time.perf_counter()
        for row in rows:
            db.execute("INSERT INTO events VALUES (?, ?)", row)
        db.commit()
        row_seconds = time.perf_counter() - start
        db.execute("DELETE FROM events"); db.commit()
        start = time.perf_counter()
        db.executemany("INSERT INTO events VALUES (?, ?)", rows)
        db.commit()
        batch_seconds = time.perf_counter() - start
        db.close()
    print(f"Rows: {args.rows}")
    print(f"Row-by-row: {row_seconds:.4f}s")
    print(f"Batch:      {batch_seconds:.4f}s")
    print(f"Speedup:    {row_seconds / batch_seconds:.2f}x" if batch_seconds else "Speedup: n/a")

if __name__ == "__main__":
    main()
