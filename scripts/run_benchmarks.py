"""Load the pipeline at increasing rates and measure freshness, then the effect of compaction.

    python scripts/run_benchmarks.py [--seconds 45] [--rates 100,1000,5000,15000]

Isolated compose project (lakehouse-bench), production stream settings (10 s trigger, up to
100k offsets per batch). Each phase replays the recorded sample at a fixed rate with fresh
ids and event times set at publish, so freshness = pipeline latency. Silver keeps only `edit`
and `new` events, so each phase expects that share of the published events in silver.
Writes benchmarks/results.json and benchmarks/results.md. Standard library only.
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
from typing import Any, TypedDict

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-p", "lakehouse-bench",
           "-f", str(ROOT / "docker-compose.yml"), "-f", str(ROOT / "docker-compose.it.yml")]
SAMPLE = ROOT / "data" / "recordings" / "sample.jsonl"
SILVER_TYPES = {"edit", "new"}  # the event types silver.edits keeps

# Parsed JSON from the containers' RESULT lines: shape owned by the benchmark modules.
Json = dict[str, Any]


class Phase(TypedDict, total=False):
    phase: str
    target_rate: int
    achieved_rate: float | None
    events: int  # published to Kafka
    silver_expected: int  # of which silver keeps
    seen: int
    p50_s: float
    p95_s: float
    p99_s: float
    max_s: float


class Report(TypedDict, total=False):
    run_at: str
    machine: str
    phase_seconds: int
    phases: list[Phase]
    compaction: Json


def sample_sizes() -> tuple[int, int]:
    """(events in the sample, of which silver keeps), counted from the recording itself."""
    events = [json.loads(line) for line in SAMPLE.read_text(encoding="utf-8").splitlines()
              if line.strip()]
    return len(events), sum(1 for e in events if e.get("type") in SILVER_TYPES)


def compose(*args: str, capture: bool = False, check: bool = True) -> str:
    if capture and args and args[0] == "run":
        args = ("run", "-T", *args[1:])  # no TTY, so a local terminal cannot change the output
    print("+ docker compose " + " ".join(args), flush=True)
    result = subprocess.run([*COMPOSE, *args], text=True, cwd=ROOT, check=False,
                            capture_output=capture)
    if capture:
        sys.stdout.write(result.stdout[-2000:])
    if check and result.returncode != 0:
        if capture:  # stdout tail is shown above; stderr would otherwise vanish
            sys.stderr.write(result.stderr or "")
        raise subprocess.CalledProcessError(result.returncode, result.args,
                                            result.stdout, result.stderr)
    return result.stdout if capture else ""


def results(output: str, marker: str = "RESULT ") -> list[Json]:
    """Every JSON payload after `marker`; exits with a clear message when there is none."""
    found = [json.loads(line.split(marker, 1)[1]) for line in output.splitlines()
             if marker in line]
    if not found:
        sys.exit(f"expected {marker.strip()!r} lines in the output above, found none")
    return found


def replay_summary(output: str) -> Json:
    """The producer's JSON summary line (the last line starting with '{')."""
    lines = [line for line in output.splitlines() if line.startswith("{")]
    if not lines:
        sys.exit("the producer printed no JSON summary line")
    summary: Json = json.loads(lines[-1])
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=45, help="Duration of each phase.")
    parser.add_argument("--rates", default="100,1000,5000,15000")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    rates = [int(r) for r in args.rates.split(",")]
    sample_size, silver_per_loop = sample_sizes()
    os.environ.update({"LAKEHOUSE_TRIGGER": "10 seconds", "LAKEHOUSE_MAX_OFFSETS": "100000"})

    compose("down", "-v", "--remove-orphans", check=False)
    compose("build", "stream", "producer", "dagster-webserver")
    phases: list[Phase] = []
    report: Report = {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "machine": f"{platform.system()} {platform.machine()}, {os.cpu_count()} logical CPUs",
        "phase_seconds": args.seconds, "phases": phases,
    }
    try:
        compose("up", "-d", "stream")
        compose("run", "--rm", "--no-deps", "spark", "python", "-m",
                "wiki_lakehouse.smoke_check")  # waits for the catalog; warms the stack
        for rate in rates:
            loops = max(1, rate * args.seconds // sample_size)
            name = f"b{rate}"
            # A failed or unconfirmed delivery makes the replay exit 1, which stops the run.
            published = replay_summary(compose(
                "run", "--rm", "--no-deps", "producer", "replay", "data/recordings/sample.jsonl",
                "--rate", str(rate), "--loops", str(loops), "--shift-to-now",
                "--id-prefix", f"{name}-", capture=True))
            phases.append({"phase": name, "target_rate": rate,
                           "achieved_rate": published["events_per_s"],
                           "events": loops * sample_size,
                           "silver_expected": loops * silver_per_loop})
        phase_args = [a for p in phases
                      for a in ("--phase", f"{p['phase']}={p['silver_expected']}")]
        fresh = results(compose("run", "--rm", "--no-deps", "spark", "python", "-m",
                                "wiki_lakehouse.benchmarks.freshness_report", "freshness",
                                *phase_args, capture=True))
        for phase, measured in zip(phases, fresh, strict=True):
            phase.update({"seen": measured["seen"], "p50_s": measured["p50_s"],
                          "p95_s": measured["p95_s"], "p99_s": measured["p99_s"],
                          "max_s": measured["max_s"]})

        def duckdb_latency(label: str) -> Json:
            out = compose("run", "--rm", "--no-deps", "dagster-webserver", "python", "-m",
                          "wiki_lakehouse.benchmarks.duckdb_query_latency", "--label", label,
                          capture=True)
            latency: Json = results(out)[0]["duckdb_query"]
            return latency

        before = duckdb_latency("before compaction")
        compaction: Json = results(compose("run", "--rm", "--no-deps", "spark", "python", "-m",
                                           "wiki_lakehouse.benchmarks.freshness_report",
                                           "compact", capture=True))[0]["compaction"]
        after = duckdb_latency("after compaction")
        report["compaction"] = {**compaction, "duckdb_before": before, "duckdb_after": after}
    finally:
        if not args.keep:
            compose("down", "-v", "--remove-orphans", check=False)

    out_dir = ROOT / "benchmarks"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "results.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (out_dir / "results.md").write_text(render(report), encoding="utf-8")
    print(render(report))


def render(report: Report) -> str:
    lines = [
        f"Benchmark run {report['run_at']} on {report['machine']}; "
        f"{report['phase_seconds']} s per phase, 10 s trigger.",
        "",
        "| Target rate (ev/s) | Achieved publish rate | Events published | Edit/new expected "
        "| In silver | Freshness p50 | p95 | p99 | max |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for p in report["phases"]:
        lines.append(f"| {p['target_rate']:,} | {p['achieved_rate']:,} | {p['events']:,} | "
                     f"{p['silver_expected']:,} | {p['seen']:,} | {p['p50_s']} s | "
                     f"{p['p95_s']} s | {p['p99_s']} s | {p['max_s']} s |")
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
