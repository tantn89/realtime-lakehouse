"""The two ways events enter the lakehouse: live from Wikimedia, or replayed from a recording.

Replay exists for determinism (tests, CI) and for load: it can publish faster than the live
stream (about 40 events/s) and can rewrite event times to "now", so freshness measured
downstream reflects the pipeline, not the age of the recording.
"""

from __future__ import annotations

import json
import logging
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import IO

from wiki_lakehouse.producer.event_routing import MAIN_TOPIC, route
from wiki_lakehouse.producer.kafka_publisher import Publisher
from wiki_lakehouse.producer.sse_stream import SseEvent

log = logging.getLogger(__name__)


def publish_events(
    bodies: Iterable[str],
    publisher: Publisher,
    *,
    recorder: IO[str] | None = None,
    report_every_s: float = 30.0,
    clock: Callable[[], float] = time.monotonic,
) -> Counter[str]:
    """Route and publish raw event bodies; returns counts per destination."""
    counts: Counter[str] = Counter()
    last_report = clock()
    for body in bodies:
        if recorder is not None:
            recorder.write(body.replace("\n", " ") + "\n")
        message = route(body)
        if message is None:
            counts["dropped_canary"] += 1
            continue
        publisher.publish(message)
        counts["main" if message.topic == MAIN_TOPIC else "dlq"] += 1
        if clock() - last_report >= report_every_s:
            log.info("published so far: %s", dict(counts))
            last_report = clock()
    publisher.flush()
    return counts


def live_bodies(events: Iterable[SseEvent], state_file: Path | None) -> Iterator[str]:
    """Event bodies from the live stream, persisting the resume position as we go."""
    for i, event in enumerate(events):
        yield event.data
        if state_file is not None and event.id and i % 100 == 0:
            state_file.write_text(event.id, encoding="utf-8")


def read_last_event_id(state_file: Path | None) -> str | None:
    if state_file is not None and state_file.exists():
        return state_file.read_text(encoding="utf-8").strip() or None
    return None


def _retime(body: str, now: datetime, id_prefix: str) -> str:
    """Set the event time to `now` and prefix its id, so the event looks like new traffic."""
    try:
        event = json.loads(body)
    except json.JSONDecodeError:
        return body  # let routing send it to the DLQ unchanged
    if isinstance(event, dict) and isinstance(event.get("meta"), dict):
        event["meta"]["dt"] = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        event["timestamp"] = int(now.timestamp())
        if id_prefix and isinstance(event["meta"].get("id"), str):
            event["meta"]["id"] = id_prefix + event["meta"]["id"]
    return json.dumps(event, ensure_ascii=False)


def replay_bodies(
    path: Path,
    *,
    rate_per_s: float | None = None,
    loops: int = 1,
    shift_to_now: bool = False,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[str]:
    """Bodies from a recording, optionally paced to `rate_per_s` and re-timestamped.

    Each loop gives events fresh ids when shifting time, so a multi-loop replay looks like
    new traffic rather than duplicates of the first pass.
    """
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    start, sent = clock(), 0
    for loop in range(loops):
        for body in lines:
            if shift_to_now:
                prefix = f"replay{loop}-" if loop else ""
                body = _retime(body, datetime.now(timezone.utc), prefix)
            if rate_per_s:
                due = start + sent / rate_per_s
                if (wait := due - clock()) > 0:
                    sleep(wait)
            sent += 1
            yield body
