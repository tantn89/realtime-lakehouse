Benchmark run 2026-10-31T16:41:40+00:00 on Windows AMD64, 20 logical CPUs; 45 s per phase, 10 s trigger.

| Target rate (ev/s) | Achieved publish rate | Events | In silver | Freshness p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| 100 | 100.0 | 4,400 | 4,400 | 15.3 s | 20.1 s | 20.5 s | 20.7 s |
| 1,000 | 999.9 | 44,800 | 44,800 | 16.2 s | 20.3 s | 20.7 s | 21.1 s |
| 5,000 | 4,999.4 | 224,800 | 224,800 | 15.8 s | 20.8 s | 21.2 s | 22.0 s |
| 15,000 | 10,535.6 | 674,800 | 674,800 | 18.2 s | 23.0 s | 23.9 s | 24.2 s |

| Silver layout | Data files | Avg file | DuckDB query (median) |
|---|---|---|---|
| Before compaction | 21 | 716.9 KB | 29.2 ms |
| After compaction (3.8 s) | 1 | 14805.5 KB | 118.8 ms |
