"""Publish traffic with a known expected outcome, through the producer's own routing.

    python -m wiki_lakehouse.reliability.scenario_publisher base --loops 50
    python -m wiki_lakehouse.reliability.scenario_publisher duplicates
    python -m wiki_lakehouse.reliability.scenario_publisher poison --count 25
    python -m wiki_lakehouse.reliability.scenario_publisher late --count 100 --minutes 30

- base: the recorded sample, re-timed to now, with a fresh id prefix per loop (all unique);
- duplicates: loop 0 again with the same ids, as a producer resume would resend them;
- poison: events that break the data contract (they must land in the dead-letter topic);
- late: unique events stamped `minutes` in the past, beyond the 10-minute watermark.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from wiki_lakehouse.producer.kafka_publisher import KafkaPublisher
from wiki_lakehouse.producer.producer_runs import publish_events, retime_event

SAMPLE = Path(os.environ.get("SCENARIO_SAMPLE", "data/recordings/sample.jsonl"))


def _sample() -> list[str]:
    return [line for line in SAMPLE.read_text(encoding="utf-8").splitlines() if line.strip()]


def base_events(loops: int) -> list[str]:
    now = datetime.now(timezone.utc)
    return [retime_event(line, now, f"it{loop}-") for loop in range(loops) for line in _sample()]


def duplicate_events() -> list[str]:
    return [retime_event(line, datetime.now(timezone.utc), "it0-") for line in _sample()]


def late_events(count: int, minutes: int) -> list[str]:
    then = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return [retime_event(line, then, "late-") for line in _sample()[:count]]


def poison_events(count: int) -> list[str]:
    valid = json.loads(_sample()[0])
    broken = [
        "{this is not json",
        json.dumps({**valid, "bot": "sometimes"}),
        json.dumps({k: v for k, v in valid.items() if k != "wiki"}),
    ]
    return [broken[i % len(broken)] for i in range(count)]


def main() -> None:
    parser = argparse.ArgumentParser(prog="scenario_publisher")
    parser.add_argument("--bootstrap", default=os.environ.get("LAKEHOUSE_KAFKA_BOOTSTRAP",
                                                              "localhost:19092"))
    sub = parser.add_subparsers(dest="kind", required=True)
    sub.add_parser("base").add_argument("--loops", type=int, default=50)
    sub.add_parser("duplicates")
    sub.add_parser("poison").add_argument("--count", type=int, default=25)
    late = sub.add_parser("late")
    late.add_argument("--count", type=int, default=100)
    late.add_argument("--minutes", type=int, default=30)
    args = parser.parse_args()

    if args.kind == "base":
        bodies = base_events(args.loops)
    elif args.kind == "duplicates":
        bodies = duplicate_events()
    elif args.kind == "poison":
        bodies = poison_events(args.count)
    else:
        bodies = late_events(args.count, args.minutes)
    counts = publish_events(bodies, KafkaPublisher(args.bootstrap))
    print(json.dumps({"scenario": args.kind, **counts}))


if __name__ == "__main__":
    main()
