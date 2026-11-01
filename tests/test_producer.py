from __future__ import annotations

import io
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from wiki_lakehouse.producer.event_routing import DLQ_TOPIC, MAIN_TOPIC, Routed, route
from wiki_lakehouse.producer.kafka_publisher import DeliveryError, ListPublisher
from wiki_lakehouse.producer.producer_runs import publish_events, publish_summary, replay_bodies
from wiki_lakehouse.producer.resume_cursor import ResumeCursor, resume_state_file
from wiki_lakehouse.producer.sse_stream import SseEvent, parse_sse, stream_events

SAMPLE = Path(__file__).parents[1] / "data" / "recordings" / "sample.jsonl"


def sample_events() -> list[dict[str, Any]]:
    return [json.loads(line) for line in SAMPLE.read_text(encoding="utf-8").splitlines()]


def with_meta(**meta: Any) -> str:
    event = sample_events()[0]
    return json.dumps({**event, "meta": {**event["meta"], **meta}})


class UnackedPublisher(ListPublisher):
    """Queues messages that Kafka never confirms (broker down) or explicitly rejects."""

    def __init__(self, *, undelivered: int = 0, failed: int = 0) -> None:
        super().__init__()
        self.undelivered, self.failed, self.flushes = undelivered, failed, 0

    def flush(self) -> int:
        self.flushes += 1
        return self.undelivered


def live_events(n: int) -> list[SseEvent]:
    return [SseEvent(f"id-{i}", "message", json.dumps(sample_events()[0])) for i in range(1, n + 1)]


# ---- SSE parsing and reconnects --------------------------------------------------------


def test_parse_sse_handles_ids_comments_and_multiline_data() -> None:
    lines = [":ok", "", "event: message", "id: 1", "data: {\"a\":", "data: 1}", "",
             "id: 2", "data: x", ""]
    assert list(parse_sse(lines)) == [
        SseEvent("1", "message", "{\"a\":\n1}"),
        SseEvent("2", "message", "x"),
    ]


def test_parse_sse_discards_an_unterminated_event_at_end_of_stream() -> None:
    assert list(parse_sse(["id: 8", "data: whole", "", "id: 9", 'data: {"trunc'])) == [
        SseEvent("8", "message", "whole"),
    ]


def test_stream_resumes_with_last_event_id_after_a_disconnect() -> None:
    seen_headers: list[str | None] = []
    bodies = ["id: a\ndata: one\n\n", "id: b\ndata: two\n\n"]

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers.get("last-event-id"))
        if len(seen_headers) == 2:
            raise httpx.ReadError("connection reset")
        return httpx.Response(200, text=bodies[min(len(seen_headers), 3) // 2])

    client = httpx.Client(transport=httpx.MockTransport(handler))
    events = list(stream_events("test-agent", client=client, sleep=lambda s: None,
                                max_reconnects=2))
    assert [e.data for e in events] == ["one", "two"]
    # First call fresh, the retry after the error and the next call both resume from "a".
    assert seen_headers == [None, "a", "a"]


def test_reconnects_back_off_honour_retry_after_and_reset_only_after_an_event() -> None:
    responses = [
        httpx.Response(503),
        httpx.Response(429, headers={"Retry-After": "120"}),  # capped at max_backoff_s
        httpx.Response(200, text=""),  # clean close without events: still backs off
        httpx.Response(200, text="id: a\ndata: one\n\n"),  # an event resets the backoff
        httpx.Response(200, text=""),
    ]
    slept: list[float] = []
    client = httpx.Client(transport=httpx.MockTransport(lambda request: responses.pop(0)))
    events = list(stream_events("test-agent", client=client, sleep=slept.append,
                                max_backoff_s=60, max_reconnects=4))
    assert [e.data for e in events] == ["one"]
    assert slept == [1, 60, 4, 1]


# ---- routing and the data contract -----------------------------------------------------


def test_real_events_pass_the_contract() -> None:
    routed = [route(json.dumps(e)) for e in sample_events()]
    assert all(r is not None and r.topic == MAIN_TOPIC for r in routed)
    assert routed[0] is not None and routed[0].key == sample_events()[0]["wiki"]


def test_contract_violations_and_garbage_go_to_the_dlq_with_a_reason() -> None:
    event = sample_events()[0]
    broken = {**event, "bot": "yes"}
    del broken["wiki"]
    message = route(json.dumps(broken))
    assert message is not None and message.topic == DLQ_TOPIC
    assert "bot" in message.headers["error"] and "wiki" in message.headers["error"]
    garbage = route("{not json")
    assert garbage is not None and garbage.topic == DLQ_TOPIC
    not_an_object = route("[1, 2]")
    assert not_an_object is not None and not_an_object.topic == DLQ_TOPIC
    assert not_an_object.headers["error"] == "event is not a JSON object"


@pytest.mark.parametrize("meta", [None, "x", [1], 7])
def test_non_object_meta_goes_to_the_dlq_instead_of_crashing(meta: object) -> None:
    message = route(json.dumps({**sample_events()[0], "meta": meta}))
    assert message is not None and message.topic == DLQ_TOPIC
    assert "meta" in message.headers["error"]


@pytest.mark.parametrize("dt", ["not-a-date", "2026-13-45T25:61:61.000Z",
                                "2026-10-31 15:03:16Z", "2026-10-31T15:03:16.404+07:00", ""])
def test_unparseable_event_times_go_to_the_dlq(dt: str) -> None:
    message = route(with_meta(dt=dt))
    assert message is not None and message.topic == DLQ_TOPIC
    assert "meta/dt" in message.headers["error"]


@pytest.mark.parametrize("dt", ["2026-10-31T15:03:16.404Z", "2026-10-31T15:03:16Z"])
def test_event_times_the_lakehouse_parses_pass(dt: str) -> None:
    message = route(with_meta(dt=dt))
    assert message is not None and message.topic == MAIN_TOPIC


def test_canary_events_are_dropped() -> None:
    assert route(with_meta(domain="canary")) is None


# ---- publishing, recording, replay ----------------------------------------------------


def test_publish_counts_destinations_and_records_raw_events() -> None:
    publisher, recorder = ListPublisher(), io.StringIO()
    good = json.dumps(sample_events()[0])
    counts = publish_events([good, "{bad", good], publisher, recorder=recorder)
    assert counts == {"main": 2, "dlq": 1}
    assert len(recorder.getvalue().splitlines()) == 3


def test_an_unexpected_routing_error_sends_the_event_to_the_dlq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_route(raw: str) -> Routed | None:
        raise KeyError("surprise")

    monkeypatch.setattr("wiki_lakehouse.producer.producer_runs.route", broken_route)
    publisher = ListPublisher()
    assert publish_events(["{}"], publisher) == {"dlq": 1}
    assert publisher.messages[0].topic == DLQ_TOPIC
    assert "KeyError" in publisher.messages[0].headers["error"]


def test_cursor_is_saved_only_after_a_confirmed_flush(tmp_path: Path) -> None:
    cursor = ResumeCursor(tmp_path / "last_event_id")
    publisher = UnackedPublisher()
    publish_events(cursor.track(live_events(150)), publisher, cursor=cursor, confirm_every=100)
    assert publisher.flushes == 2  # at event 100 and at the end
    assert ResumeCursor(tmp_path / "last_event_id").read() == "id-150"
    assert not (tmp_path / "last_event_id.tmp").exists()  # written atomically, then renamed
    assert ResumeCursor(tmp_path / "missing").read() is None


@pytest.mark.parametrize("unacked", [{"undelivered": 3}, {"failed": 1}])
def test_cursor_does_not_advance_past_unacked_or_failed_deliveries(
    tmp_path: Path, unacked: dict[str, int],
) -> None:
    state = tmp_path / "last_event_id"
    state.write_text("id-0", encoding="utf-8")
    cursor = ResumeCursor(state)
    assert cursor.read() == "id-0"
    with pytest.raises(DeliveryError):
        publish_events(cursor.track(live_events(250)), UnackedPublisher(**unacked),
                       cursor=cursor, confirm_every=100)
    assert ResumeCursor(state).read() == "id-0"


def test_an_interrupt_flushes_and_saves_the_last_published_event(tmp_path: Path) -> None:
    def interrupted() -> Iterator[SseEvent]:
        yield from live_events(42)
        raise KeyboardInterrupt  # SIGTERM raises SystemExit the same way

    cursor, publisher = ResumeCursor(tmp_path / "last_event_id"), UnackedPublisher()
    with pytest.raises(KeyboardInterrupt):
        publish_events(cursor.track(interrupted()), publisher, cursor=cursor)
    assert publisher.flushes == 1 and len(publisher.messages) == 42
    assert ResumeCursor(tmp_path / "last_event_id").read() == "id-42"


def test_an_event_taken_but_not_published_is_not_saved(tmp_path: Path) -> None:
    class StopOnSecond(UnackedPublisher):
        def publish(self, message: Routed) -> None:
            if self.messages:
                raise SystemExit(0)  # e.g. SIGTERM while event 2 is being published
            super().publish(message)

    cursor = ResumeCursor(tmp_path / "last_event_id")
    with pytest.raises(SystemExit):
        publish_events(cursor.track(live_events(5)), StopOnSecond(), cursor=cursor)
    assert ResumeCursor(tmp_path / "last_event_id").read() == "id-1"


def test_missing_state_directory_disables_resume_with_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    assert resume_state_file(tmp_path / "state") == tmp_path / "state"
    assert resume_state_file(tmp_path / "absent" / "last_event_id") is None
    assert "resume is disabled" in caplog.text


def test_delivery_failures_fail_the_summary_and_are_reported() -> None:
    good = json.dumps(sample_events()[0])
    summary, delivered = publish_summary([good, good], UnackedPublisher(failed=2))
    assert not delivered
    assert summary["failed"] == 2 and summary["published"] == {"main": 2}
    summary, delivered = publish_summary([good, with_meta(domain="canary")], ListPublisher())
    assert delivered and summary["failed"] == 0 and summary["undelivered"] == 0


def test_replay_paces_and_retimes_with_fresh_ids_per_loop() -> None:
    now = [0.0]
    slept: list[float] = []

    def sleep(s: float) -> None:
        slept.append(s)
        now[0] += s

    bodies = list(replay_bodies(SAMPLE, rate_per_s=100, loops=2, shift_to_now=True,
                                clock=lambda: now[0], sleep=sleep))
    n = len(sample_events())
    assert len(bodies) == 2 * n
    # 2n events at 100/s take (2n - 1) / 100 seconds of pacing.
    assert sum(slept) == pytest.approx((2 * n - 1) / 100)
    first, second = json.loads(bodies[0]), json.loads(bodies[n])
    assert second["meta"]["id"] == "replay1-" + first["meta"]["id"]
    assert first["meta"]["dt"].endswith("Z") and first["meta"]["dt"].startswith("20")


def test_id_prefix_applies_without_shifting_times() -> None:
    recorded = sample_events()[0]
    bodies = list(replay_bodies(SAMPLE, loops=2, id_prefix="x"))
    first, second = json.loads(bodies[0]), json.loads(bodies[len(sample_events())])
    assert first["meta"]["id"] == "x0-" + recorded["meta"]["id"]
    assert second["meta"]["id"] == "x1-" + recorded["meta"]["id"]
    assert first["meta"]["dt"] == recorded["meta"]["dt"]  # times untouched
