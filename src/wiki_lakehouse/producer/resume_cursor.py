"""Where the live stream resumes after a restart, saved only once Kafka has the events before it.

The SSE `id` of an event is enough to resume right after it (`Last-Event-ID`). Saving that id
as soon as an event is queued would be wrong: librdkafka only buffers the message, and a crash
or a delivery timeout would lose it while the saved position already points past it. So the
cursor tracks three things:

- the candidate: the id of the event currently being handed to the publisher;
- the published id: the last event the publisher accepted (`mark_published`);
- the saved id: written by `commit`, which callers invoke only after a flush confirmed every
  delivery so far.

A crash between commits replays events after the saved id; downstream deduplicates on
`meta.id`, so the producer is at-least-once into Kafka.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Iterator
from pathlib import Path

from wiki_lakehouse.producer.sse_stream import SseEvent

log = logging.getLogger(__name__)


class ResumeCursor:
    def __init__(self, state_file: Path | None) -> None:
        self.state_file = state_file
        self._candidate: str | None = None
        self._published: str | None = None
        self._saved: str | None = None

    def read(self) -> str | None:
        """The saved resume position, or None to start from now."""
        if self.state_file is not None and self.state_file.exists():
            self._saved = self.state_file.read_text(encoding="utf-8").strip() or None
        return self._saved

    def track(self, events: Iterable[SseEvent]) -> Iterator[str]:
        """Event bodies, remembering which event each one came from."""
        for event in events:
            # Set before the yield: if the consumer stops before publishing this body,
            # `mark_published` is never called and the cursor stays on the previous event.
            self._candidate = event.id or self._candidate
            yield event.data

    def mark_published(self) -> None:
        """The body last yielded by `track` was handed to the publisher (or dropped on purpose)."""
        self._published = self._candidate

    def commit(self) -> None:
        """Persist the published position. Call only after Kafka confirmed every delivery."""
        published = self._published
        if self.state_file is None or published is None or published == self._saved:
            return
        # Write then rename: a kill mid-write leaves the old file intact, never an empty one.
        tmp = self.state_file.with_name(self.state_file.name + ".tmp")
        tmp.write_text(published, encoding="utf-8")
        os.replace(tmp, self.state_file)
        self._saved = published


def resume_state_file(path: Path) -> Path | None:
    """The state file to use, or None (with a warning) when its directory does not exist."""
    if path.parent.is_dir():
        return path
    log.warning("state directory %s does not exist: resume is disabled, a restart will start "
                "from now and skip events in between (set --state-file or PRODUCER_STATE_FILE)",
                path.parent)
    return None
