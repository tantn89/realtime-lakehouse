"""Server-sent events client for stream.wikimedia.org that survives disconnects.

Wikimedia ends long-lived connections regularly. Each event carries an `id` (Kafka offsets
upstream); sending it back as `Last-Event-ID` resumes where the stream left off, so a
reconnect loses nothing. Resuming can replay a few events, which is why the lakehouse
deduplicates on `meta.id` downstream: delivery here is at-least-once by design.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)

RECENTCHANGE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


@dataclass(frozen=True)
class SseEvent:
    id: str | None
    event: str
    data: str


def parse_sse(lines: Iterable[str]) -> Iterator[SseEvent]:
    """Parse the text/event-stream format: fields until a blank line end one event."""
    event_id: str | None = None
    event_type = "message"
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if not line:
            if data:
                yield SseEvent(event_id, event_type, "\n".join(data))
            event_type, data = "message", []
            continue
        if line.startswith(":"):
            continue  # comment / keep-alive
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value
        elif field == "event":
            event_type = value
    if data:
        yield SseEvent(event_id, event_type, "\n".join(data))


def stream_events(
    user_agent: str,
    *,
    url: str = RECENTCHANGE_URL,
    last_event_id: str | None = None,
    client: httpx.Client | None = None,
    max_backoff_s: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
    max_reconnects: int | None = None,
) -> Iterator[SseEvent]:
    """Yield events forever, reconnecting with exponential backoff and Last-Event-ID resume.

    Wikimedia requires a descriptive User-Agent with contact information.
    """
    client = client or httpx.Client(timeout=httpx.Timeout(10.0, read=60.0))
    backoff, reconnects = 1.0, 0
    while True:
        headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        try:
            with client.stream("GET", url, headers=headers) as response:
                response.raise_for_status()
                backoff = 1.0
                for event in parse_sse(response.iter_lines()):
                    if event.id:
                        last_event_id = event.id
                    if event.event == "message":
                        yield event
            log.info("stream ended by server; resuming")
        except (httpx.HTTPError, httpx.StreamError) as e:
            log.warning("stream error (%s); reconnecting in %.0fs", e, backoff)
            sleep(backoff)
            backoff = min(backoff * 2, max_backoff_s)
        reconnects += 1
        if max_reconnects is not None and reconnects > max_reconnects:
            return
