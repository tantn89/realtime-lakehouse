"""The two ways events enter the lakehouse: live from Wikimedia, or replayed from a recording.

Replay exists for determinism (tests, CI) and for load: it can publish faster than the live
stream (about 40 events/s) and can rewrite event times to "now", so freshness measured
downstream reflects the pipeline, not the age of the recording.

Delivery guarantee: every run ends with a flush, also on Ctrl+C or SIGTERM (`finally`), and
raises `DeliveryError` if Kafka rejected or never confirmed any message. In live mode the resume
cursor is saved only after such a confirmed flush, so the producer is at-least-once into Kafka.
"""

from __future__ import annotations

import json
import logging
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any

from wiki_lakehouse.producer.event_routing import DLQ_TOPIC, MAIN_TOPIC, Routed, route
from wiki_lakehouse.producer.kafka_publisher import DeliveryError, Publisher
from wiki_lakehouse.producer.resume_cursor import ResumeCursor

log = logging.getLogger(__name__)


def _route_or_dlq(body: str) -> Routed | None:
    """`route`, but a bug or an unforeseen event shape sends the body to the DLQ, never crashes."""
    try:
        return route(body)
    except Exception as e:  # deliberately broad: one poison event must not stop the stream
        log.exception("routing failed; sending the event to the DLQ")
        return Routed(DLQ_TOPIC, None, body,
                      {"error": f"routing failed: {type(e).__name__}: {e}"[:1000]})


def _raise_if_undelivered(publisher: Publisher, undelivered: int, counts: Counter[str]) -> None:
    if undelivered or publisher.failed:
        raise DeliveryError(publisher.failed, undelivered, counts)


def publish_events(
    bodies: Iterable[str],
    publisher: Publisher,
    *,
    recorder: IO[str] | None = None,
    cursor: ResumeCursor | None = None,
    confirm_every: int = 100,
    report_every_s: float = 30.0,
    clock: Callable[[], float] = time.monotonic,
) -> Counter[str]:
    """Route and publish raw event bodies; returns counts per destination.

    With a `cursor`, every `confirm_every` events the publisher is flushed and, only if every
    delivery so far is confirmed, the resume position is saved. Raises `DeliveryError` as soon
    as a delivery failure is seen, so the cursor never moves past a lost event.
    """
    counts: Counter[str] = Counter()
    last_report = clock()
    try:
        for i, body in enumerate(bodies, start=1):
            if recorder is not None:
                recorder.write(body.replace("\n", " ") + "\n")
            message = _route_or_dlq(body)
            if message is None:
                counts["dropped_canary"] += 1
            else:
                publisher.publish(message)
                counts["main" if message.topic == MAIN_TOPIC else "dlq"] += 1
            if cursor is not None:
                cursor.mark_published()
                if i % confirm_every == 0:
                    _raise_if_undelivered(publisher, publisher.flush(), counts)
                    cursor.commit()
            if clock() - last_report >= report_every_s:
                log.info("published so far: %s", dict(counts))
                last_report = clock()
    finally:
        # Runs on normal end, on errors and on KeyboardInterrupt/SystemExit (SIGTERM), so a
        # stop drains librdkafka's queue instead of dropping it.
        _raise_if_undelivered(publisher, publisher.flush(), counts)
        if cursor is not None:
            cursor.commit()
    return counts


def publish_summary(bodies: Iterable[str], publisher: Publisher) -> tuple[dict[str, Any], bool]:
    """Publish everything and summarise it as one JSON-able dict; the bool is False on loss.

    `published` counts what was routed to each destination; `failed` and `undelivered` say how
    many of those Kafka rejected or never confirmed. The benchmark driver reads `events_per_s`.
    """
    started = time.monotonic()
    failed = undelivered = 0
    try:
        counts = dict(publish_events(bodies, publisher))
    except DeliveryError as e:
        counts, failed, undelivered = e.counts, e.failed, e.undelivered
    elapsed = time.monotonic() - started
    sent = counts.get("main", 0) + counts.get("dlq", 0)  # canaries are dropped, not sent
    summary = {"published": counts, "failed": failed, "undelivered": undelivered,
               "seconds": round(elapsed, 2),
               "events_per_s": round(sent / elapsed, 1) if elapsed else None}
    return summary, failed == 0 and undelivered == 0


def rewrite_event(body: str, *, now: datetime | None, id_prefix: str) -> str:
    """Optionally set the event time to `now`, and prefix the event id with `id_prefix`."""
    if now is None and not id_prefix:
        return body
    try:
        event = json.loads(body)
    except json.JSONDecodeError:
        return body  # let routing send it to the DLQ unchanged
    if isinstance(event, dict) and isinstance(event.get("meta"), dict):
        if now is not None:
            event["meta"]["dt"] = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            event["timestamp"] = int(now.timestamp())
        if id_prefix and isinstance(event["meta"].get("id"), str):
            event["meta"]["id"] = id_prefix + event["meta"]["id"]
    return json.dumps(event, ensure_ascii=False)


def retime_event(body: str, now: datetime, id_prefix: str) -> str:
    """Set the event time to `now` and prefix its id, so the event looks like new traffic."""
    return rewrite_event(body, now=now, id_prefix=id_prefix)


def _loop_prefix(id_prefix: str | None, loop: int) -> str:
    if id_prefix:
        return f"{id_prefix}{loop}-"
    return f"replay{loop}-" if loop else ""  # loop 0 keeps the recorded ids


def replay_bodies(
    path: Path,
    *,
    rate_per_s: float | None = None,
    loops: int = 1,
    shift_to_now: bool = False,
    id_prefix: str | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[str]:
    """Bodies from a recording, optionally paced to `rate_per_s` and re-timestamped.

    Every loop after the first gets fresh ids, so a multi-loop replay looks like new traffic
    rather than duplicates of the first pass. `id_prefix` prefixes every loop, including the
    first, so separate replays (benchmark phases) never collide either. Ids are rewritten
    whether or not times are shifted.
    """
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    start, sent = clock(), 0
    for loop in range(loops):
        prefix = _loop_prefix(id_prefix, loop)
        for line in lines:
            now = datetime.now(timezone.utc) if shift_to_now else None
            body = rewrite_event(line, now=now, id_prefix=prefix)
            if rate_per_s:
                due = start + sent / rate_per_s
                if (wait := due - clock()) > 0:
                    sleep(wait)
            sent += 1
            yield body
