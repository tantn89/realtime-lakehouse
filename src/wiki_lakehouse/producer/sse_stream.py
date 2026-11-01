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
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

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
    # Fields without a closing blank line are an incomplete event: the SSE spec discards them.
    # Emitting it would publish a truncated body and move the resume position past the real one.


def _retry_after_s(response: httpx.Response) -> float | None:
    """The server's Retry-After (seconds or an HTTP date), if it sent a usable one."""
    value = response.headers.get("retry-after")
    if not value:
        return None
    if value.strip().isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


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

    Every reconnect waits, also after a clean close by the server, so a proxy that accepts and
    immediately closes cannot cause a tight loop. The backoff doubles up to `max_backoff_s` and
    resets only once an event actually arrives; a Retry-After header lengthens the wait (capped).
    Wikimedia requires a descriptive User-Agent with contact information.
    """
    client = client or httpx.Client(timeout=httpx.Timeout(10.0, read=60.0))
    backoff, reconnects = 1.0, 0
    while True:
        headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        wait = backoff
        try:
            with client.stream("GET", url, headers=headers) as response:
                response.raise_for_status()
                for event in parse_sse(response.iter_lines()):
                    backoff = wait = 1.0  # the connection works: start over from a short wait
                    if event.id:
                        last_event_id = event.id
                    if event.event == "message":
                        yield event
            log.info("stream ended by server; reconnecting in %.0fs", wait)
        except httpx.HTTPStatusError as e:
            retry_after = _retry_after_s(e.response)
            wait = min(max(backoff, retry_after or 0.0), max_backoff_s)
            log.warning("stream refused (HTTP %d); reconnecting in %.0fs",
                        e.response.status_code, wait)
        except (httpx.HTTPError, httpx.StreamError) as e:
            log.warning("stream error (%s); reconnecting in %.0fs", e, wait)
        reconnects += 1
        if max_reconnects is not None and reconnects > max_reconnects:
            return
        sleep(wait)
        backoff = min(backoff * 2, max_backoff_s)
