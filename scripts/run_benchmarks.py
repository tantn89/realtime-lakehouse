"""Load the pipeline at increasing rates and measure freshness, then the effect of compaction.

    python scripts/run_benchmarks.py [--seconds 45] [--rates 100,1000,5000,15000]

Isolated compose project (lakehouse-bench), production stream settings (10 s trigger, up to
100k offsets per batch). Each phase replays the recorded sample at a fixed rate with fresh
ids and event times set at publish, so freshness = pipeline latency. Writes
benchmarks/results.json and benchmarks/results.md. Standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-p", "lakehouse-bench",
           "-f", str(ROOT / "docker-compose.yml"), "-f", str(ROOT / "docker-compose.it.yml")]
SAMPLE_SIZE = 400


def compose(*args: str, capture: bool = False, check: bool = True) -> str:
    print("+ docker compose " + " ".join(args), flush=True)
    result = subprocess.run([*COMPOSE, *args], text=True, cwd=ROOT, check=check,
                            capture_output=capture)
    if capture:
        sys.stdout.write(result.stdout[-2000:])
    return result.stdout if capture else ""


def results(output: str) -> list[dict[str, object]]:
    return [json.loads(line.split("RESULT ", 1)[1]) for line in output.splitlines()
            if "RESULT " in line]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=45, help="Duration of each phase.")
    parser.add_argument("--rates", default="100,1000,5000,15000")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    rates = [int(r) for r in args.rates.split(",")]
    os.environ.update({"LAKEHOUSE_TRIGGER": "10 seconds", "LAKEHOUSE_MAX_OFFSETS": "100000"})

    compose("down", "-v", "--remove-orphans", check=False)
    compose("build", "stream", "producer", "dagster-webserver")
    report: dict[str, object] = {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "machine": f"{platform.system()} {platform.machine()}, {os.cpu_count()} logical CPUs",
        "phase_seconds": args.seconds, "phases": [],
    }
    try:
        compose("up", "-d", "stream")
        compose("run", "--rm", "--no-deps", "spark", "python", "-m",
                "wiki_lakehouse.smoke_check")  # waits for the catalog; warms the stack
        expected: dict[str, int] = {}
        for rate in rates:
            loops = max(1, rate * args.seconds // SAMPLE_SIZE)
            name = f"b{rate}"
            out = compose("run", "--rm", "--no-deps", "producer", "replay",
                          "data/recordings/sample.jsonl", "--rate", str(rate), "--loops",
                          str(loops), "--shift-to-now", "--id-prefix", f"{name}-", capture=True)
            published = json.loads([line for line in out.splitlines()
                                    if line.startswith("{")][-1])
            expected[name] = loops * SAMPLE_SIZE
            report["phases"].append({"phase": name, "target_rate": rate,  # type: ignore[union-attr]
                                     "achieved_rate": published["events_per_s"],
                                     "events": expected[name]})
        phase_args = [a for name, n in expected.items() for a in ("--phase", f"{name}={n}")]
        fresh = results(compose("run", "--rm", "--no-deps", "spark", "python", "-m",
                                "wiki_lakehouse.benchmarks.freshness_report", "freshness",
                                *phase_args, capture=True))
        for phase, measured in zip(report["phases"], fresh, strict=True):  # type: ignore[arg-type]
            phase.update({k: measured[k] for k in ("seen", "p50_s", "p95_s", "p99_s", "max_s")})

        def duckdb_latency(label: str) -> dict[str, object]:
            out = compose("run", "--rm", "--no-deps", "dagster-webserver", "python", "-m",
                          "wiki_lakehouse.benchmarks.duckdb_query_latency", "--label", label,
                          capture=True)
            return results(out)[0]["duckdb_query"]  # type: ignore[return-value]

        before = duckdb_latency("before compaction")
        compaction = results(compose("run", "--rm", "--no-deps", "spark", "python", "-m",
                                     "wiki_lakehouse.benchmarks.freshness_report", "compact",
                                     capture=True))[0]["compaction"]
        after = duckdb_latency("after compaction")
        report["compaction"] = {**compaction, "duckdb_before": before,  # type: ignore[dict-item]
                                "duckdb_after": after}
    finally:
        if not args.keep:
            compose("down", "-v", "--remove-orphans", check=False)

    out_dir = ROOT / "benchmarks"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "results.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (out_dir / "results.md").write_text(render(report), encoding="utf-8")
    print(render(report))


def render(report: dict[str, object]) -> str:
    lines = [
        f"Benchmark run {report['run_at']} on {report['machine']}; "
        f"{report['phase_seconds']} s per phase, 10 s trigger.",
        "",
        "| Target rate (ev/s) | Achieved publish rate | Events | In silver "
        "| Freshness p50 | p95 | p99 | max |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for p in report["phases"]:  # type: ignore[attr-defined]
        lines.append(f"| {p['target_rate']:,} | {p['achieved_rate']:,} | {p['events']:,} | "
                     f"{p['seen']:,} | {p['p50_s']} s | {p['p95_s']} s | {p['p99_s']} s | "
                     f"{p['max_s']} s |")
    c = report.get("compaction")
    if c:
        lines += [
            "",
            "| Silver layout | Data files | Row groups | Avg file | DuckDB query (median) |",
            "|---|---|---|---|---|",
            f"| Before compaction | {c['before']['files']:,} | {c['before']['row_groups']:,} | "
            f"{c['before']['avg_kb']} KB | {c['duckdb_before']['median_ms']} ms |",
            f"| After compaction ({c['seconds']} s) | {c['after']['files']:,} | "
            f"{c['after']['row_groups']:,} | {c['after']['avg_kb']} KB | "
            f"{c['duckdb_after']['median_ms']} ms |",
        ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
