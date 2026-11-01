Benchmark run 2026-11-01T00:38:10+00:00 on Windows AMD64, 20 logical CPUs; 45 s per phase, 10 s trigger.

| Target rate (ev/s) | Achieved publish rate | Events | In silver | Freshness p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| 100 | 100.0 | 4,400 | 4,400 | 16.2 s | 20.2 s | 20.6 s | 20.8 s |
| 1,000 | 999.9 | 44,800 | 44,800 | 15.2 s | 20.1 s | 20.6 s | 20.8 s |
| 5,000 | 4,999.5 | 224,800 | 224,800 | 16.6 s | 20.7 s | 21.2 s | 21.4 s |
| 15,000 | 9,787.3 | 674,800 | 674,800 | 16.9 s | 21.4 s | 21.9 s | 22.2 s |

| Silver layout | Data files | Row groups | Avg file | DuckDB query (median) |
|---|---|---|---|---|
| Before compaction | 21 | 21 | 719.0 KB | 29.7 ms |
| After compaction (5.2 s) | 1 | 3 | 14745.1 KB | 56.0 ms |
