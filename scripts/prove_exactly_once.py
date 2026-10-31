"""Crash the stream mid-flight and prove nothing was lost or duplicated.

    python scripts/prove_exactly_once.py [--keep]

Runs in its own compose project (lakehouse-it) with no published ports, so it never touches
the dev stack. Standard library only: it drives `docker compose`.

Scenario (expected outcome in brackets):
  1. 20,000 unique events                       [all reach silver once]
  2. 400 of them resent, as after a resume       [collapsed by deduplication]
  3. 25 contract violations                      [dead-letter topic only]
  4. SIGKILL the stream while it works through the backlog, then restart it
  5. 100 events stamped 30 minutes in the past   [dropped by the 10-minute watermark]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-p", "lakehouse-it",
           "-f", str(ROOT / "docker-compose.yml"), "-f", str(ROOT / "docker-compose.it.yml")]
LOOPS, SAMPLE_SIZE, POISON, LATE = 50, 400, 25, 100


def compose(*args: str, check: bool = True, capture: bool = False) -> str:
    print("+ docker compose " + " ".join(args), flush=True)
    result = subprocess.run([*COMPOSE, *args], check=check, text=True,
                            capture_output=capture, cwd=ROOT)
    return result.stdout if capture else ""


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
        at_crash = json.loads(compose(
            "run", "--rm", "--no-deps", "spark", "python", "-m",
            "wiki_lakehouse.reliability.verify_exactly_once", "--counts-only",
            "--expect-kafka", "0", "--expect-silver", "0", "--expect-dlq", "0",
            capture=True).split("COUNTS ", 1)[1].splitlines()[0])
        total = LOOPS * SAMPLE_SIZE + SAMPLE_SIZE
        print(f"stream killed with SIGKILL; committed at crash: {at_crash} of {total}", flush=True)
        if not 0 < at_crash["bronze_rows"] < total:
            sys.exit("inconclusive: the crash did not land in the middle of the backlog")
        compose("start", "stream")  # same container and checkpoint volume, logs kept
        wait_for_log("stream", "started queries", after=1)

        publish("late", "--count", str(LATE), "--minutes", "30")

        compose("run", "--rm", "--no-deps", "spark", "python", "-m",
                "wiki_lakehouse.reliability.verify_exactly_once",
                "--expect-kafka", str(LOOPS * SAMPLE_SIZE + SAMPLE_SIZE + LATE),
                "--expect-silver", str(LOOPS * SAMPLE_SIZE),
                "--expect-dlq", str(POISON))
    finally:
        if not args.keep:
            compose("down", "-v", "--remove-orphans", check=False)


if __name__ == "__main__":
    main()
