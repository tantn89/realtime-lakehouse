Benchmark run 2026-11-01T04:14:54+00:00 on Windows AMD64, 20 logical CPUs; 45 s per phase, 10 s trigger.

| Target rate (ev/s) | Achieved publish rate | Events published | Edit/new expected | In silver | Freshness p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| 100 | 100.0 | 4,400 | 2,156 | 2,156 | 15.4 s | 20.2 s | 20.6 s | 20.9 s |
| 1,000 | 999.9 | 44,800 | 21,952 | 21,952 | 16.1 s | 20.2 s | 20.6 s | 20.8 s |
| 5,000 | 4,999.6 | 224,800 | 110,152 | 110,152 | 15.6 s | 20.6 s | 21.1 s | 21.3 s |
| 15,000 | 9,596.3 | 674,800 | 330,652 | 330,652 | 16.6 s | 21.0 s | 21.6 s | 21.9 s |

| Silver layout | Data files | Row groups | Avg file | DuckDB query (median) |
|---|---|---|---|---|
| Before compaction | 22 | 22 | 378.6 KB | 22.2 ms |
| After compaction (3.1 s) | 1 | 2 | 8016.5 KB | 53.0 ms |
