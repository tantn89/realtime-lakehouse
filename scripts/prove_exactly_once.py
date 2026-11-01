"""Crash the stream mid-flight and prove nothing was lost or duplicated.

    python scripts/prove_exactly_once.py [--keep]

Runs in its own compose project (lakehouse-it) with no published ports, so it never touches
the dev stack. Standard library only: it drives `docker compose`.

Scenario (expected outcome in brackets). Bronze and Kafka receive every event; silver keeps
only `edit` and `new` events, so silver expectations count just those types of the sample:
  1. 50 loops of the 400-event sample, unique ids   [every edit/new event in silver once]
  2. the 400 events of loop 0 resent, as on resume  [collapsed by deduplication]
  3. 25 contract violations                         [dead-letter topic only]
  4. SIGKILL the stream while it works through the backlog, then restart it
  5. 100 events stamped 30 minutes in the past      [dropped by the 10-minute watermark]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-p", "lakehouse-it",
           "-f", str(ROOT / "docker-compose.yml"), "-f", str(ROOT / "docker-compose.it.yml")]
SAMPLE = ROOT / "data" / "recordings" / "sample.jsonl"
SILVER_TYPES = {"edit", "new"}  # the event types silver.edits keeps
LOOPS, POISON, LATE = 50, 25, 100


def sample_sizes() -> tuple[int, int]:
    """(events in the sample, of which silver keeps), counted from the recording itself."""
    events = [json.loads(line) for line in SAMPLE.read_text(encoding="utf-8").splitlines()
              if line.strip()]
    return len(events), sum(1 for e in events if e.get("type") in SILVER_TYPES)


def compose(*args: str, check: bool = True, capture: bool = False) -> str:
    if capture and args and args[0] == "run":
        args = ("run", "-T", *args[1:])  # no TTY, so a local terminal cannot change the output
    print("+ docker compose " + " ".join(args), flush=True)
    result = subprocess.run([*COMPOSE, *args], check=False, text=True,
                            capture_output=capture, cwd=ROOT)
    if check and result.returncode != 0:
        if capture:  # captured output would otherwise vanish with the exception
            sys.stdout.write(result.stdout or "")
            sys.stderr.write(result.stderr or "")
        raise subprocess.CalledProcessError(result.returncode, result.args,
                                            result.stdout, result.stderr)
    return result.stdout if capture else ""


def marker_json(output: str, marker: str) -> Any:
    """The JSON after the first `marker` line in `output`; exits with the output if missing."""
    for line in output.splitlines():
        if marker in line:
            return json.loads(line.split(marker, 1)[1])
    sys.stdout.write(output[-4000:])
    sys.exit(f"expected a line with {marker.strip()!r} in the output above, found none")


def wait_for_log(service: str, needle: str, timeout_s: int = 300, after: int = 0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        logs = compose("logs", service, check=False, capture=True)
        if logs.count(needle) > after:
            return
        time.sleep(3)
    sys.exit(f"timed out waiting for {needle!r} in {service} logs")


def publish(*args: str) -> None:
    compose("run", "--rm", "--no-deps", "--entrypoint", "python", "producer",
            "-m", "wiki_lakehouse.reliability.scenario_publisher", *args)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="Leave the stack up afterwards.")
    args = parser.parse_args()
    sample_size, silver_per_loop = sample_sizes()
    if sample_size < LATE:
        sys.exit(f"LATE ({LATE}) cannot exceed the sample size ({sample_size})")

    compose("down", "-v", "--remove-orphans", check=False)
    compose("build", "stream", "producer")
    try:
        compose("up", "-d", "stream")
        wait_for_log("stream", "started queries")

        publish("base", "--loops", str(LOOPS))
        publish("duplicates")
        publish("poison", "--count", str(POISON))

        time.sleep(12)  # let a few micro-batches commit, then crash in the middle of the backlog
        compose("kill", "-s", "SIGKILL", "stream")
        at_crash = marker_json(compose(
            "run", "--rm", "--no-deps", "spark", "python", "-m",
            "wiki_lakehouse.reliability.verify_exactly_once", "--counts-only",
            capture=True), "COUNTS ")
        total = LOOPS * sample_size + sample_size  # every event reaches bronze, duplicates too
        print(f"stream killed with SIGKILL; committed at crash: {at_crash} of {total}", flush=True)
        if not 0 < at_crash["bronze_rows"] < total:
            sys.exit("inconclusive: the crash did not land in the middle of the backlog")
        compose("start", "stream")  # same container and checkpoint volume, logs kept
        wait_for_log("stream", "started queries", after=1)

        publish("late", "--count", str(LATE), "--minutes", "30")

        compose("run", "--rm", "--no-deps", "spark", "python", "-m",
                "wiki_lakehouse.reliability.verify_exactly_once",
                "--expect-kafka", str(total + LATE),
                "--expect-silver", str(LOOPS * silver_per_loop),
                "--expect-dlq", str(POISON))
    finally:
        if not args.keep:
            compose("down", "-v", "--remove-orphans", check=False)


if __name__ == "__main__":
    main()
