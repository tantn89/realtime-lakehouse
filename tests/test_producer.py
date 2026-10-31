from __future__ import annotations

import io
import json
from pathlib import Path

import httpx

from wiki_lakehouse.producer.event_routing import DLQ_TOPIC, MAIN_TOPIC, route
from wiki_lakehouse.producer.kafka_publisher import ListPublisher
from wiki_lakehouse.producer.producer_runs import (
    live_bodies,
    publish_events,
    read_last_event_id,
    replay_bodies,
)
from wiki_lakehouse.producer.sse_stream import SseEvent, parse_sse, stream_events

SAMPLE = Path(__file__).parents[1] / "data" / "recordings" / "sample.jsonl"


def sample_events() -> list[dict[str, object]]:
    return [json.loads(line) for line in SAMPLE.read_text(encoding="utf-8").splitlines()]


# ---- SSE parsing and reconnects --------------------------------------------------------


def test_parse_sse_handles_ids_comments_and_multiline_data() -> None:
    lines = [":ok", "", "event: message", "id: 1", "data: {\"a\":", "data: 1}", "",
             "id: 2", "data: x", ""]
    assert list(parse_sse(lines)) == [
        SseEvent("1", "message", "{\"a\":\n1}"),
        SseEvent("2", "message", "x"),
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
    assert route("[1, 2]") is not None


def test_canary_events_are_dropped() -> None:
    event = sample_events()[0]
    canary = {**event, "meta": {**event["meta"], "domain": "canary"}}  # type: ignore[dict-item]
    assert route(json.dumps(canary)) is None


# ---- publishing, recording, replay ----------------------------------------------------


def test_publish_counts_destinations_and_records_raw_events() -> None:
    publisher, recorder = ListPublisher(), io.StringIO()
    good = json.dumps(sample_events()[0])
    counts = publish_events([good, "{bad", good], publisher, recorder=recorder)
    assert counts == {"main": 2, "dlq": 1}
    assert len(recorder.getvalue().splitlines()) == 3


def test_live_bodies_persist_the_resume_position(tmp_path: Path) -> None:
    state = tmp_path / "last_event_id"
    events = [SseEvent(f"id-{i}", "message", "{}") for i in range(150)]
    assert len(list(live_bodies(iter(events), state))) == 150
    assert read_last_event_id(state) == "id-100"
    assert read_last_event_id(tmp_path / "missing") is None


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
    assert sum(slept) == (2 * n - 1) / 100
    first, second = json.loads(bodies[0]), json.loads(bodies[n])
    assert second["meta"]["id"] == "replay1-" + first["meta"]["id"]
    assert first["meta"]["dt"].endswith("Z") and first["meta"]["dt"].startswith("20")
