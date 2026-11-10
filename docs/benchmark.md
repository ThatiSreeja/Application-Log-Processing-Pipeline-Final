# Benchmarking

Run `python -m scripts.benchmark --counts 1000,10000,100000` from the repository root. The benchmark creates isolated temporary inputs and databases and reports generated/input/accepted/rejected counts, elapsed seconds, throughput, and peak Python memory. Results are written to `reports/performance_report.csv`.

The production path uses SQLite `executemany` (`insert_events`) to batch accepted records per source file. For a repeatable micro-comparison of individual inserts and batched inserts, run `python -m scripts.benchmark_inserts --rows 100000`. Timings depend on disk, Python, and SQLite versions; compare on the same machine and do not treat one run as a universal performance guarantee.


## Example execution evidence (Windows / Python 3.10)

One local run of `python -m scripts.benchmark --counts 1000,10000,100000` produced the following measurements. The count is generated per input format, so the total input is approximately twice that count plus the generator's three additional edge-case records.

| Records per format | Input records | Elapsed seconds | Throughput (input records/sec) | Peak Python memory (MiB) |
|---:|---:|---:|---:|---:|
| 1,000 | 2,003 | 0.3767 | 5,317.64 | 4.69 |
| 10,000 | 20,003 | 2.3945 | 8,353.63 | 44.49 |
| 100,000 | 200,003 | 31.3369 | 6,382.34 | 444.46 |

The same machine's 100,000-row insertion microbenchmark reported 0.1070 seconds for row-by-row insertion and 0.0856 seconds for batch insertion (1.25x speedup, approximately 20% lower elapsed time). These are illustrative observations, not performance guarantees. Peak memory grows with input size; for substantially larger files, consider bounded/chunked ingestion and external storage.
