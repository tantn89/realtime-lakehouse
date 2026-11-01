"""Decide where each raw event goes: the main topic, the dead-letter topic, or nowhere.

- Valid events go to `wiki.recentchange`, keyed by wiki so one wiki's edits stay ordered.
- Unparseable or contract-violating events go to `wiki.recentchange.dlq` with the reason,
  instead of failing the stream or silently disappearing.
- Wikimedia's synthetic "canary" events (meta.domain == "canary") are monitoring probes,
  not edits, and are dropped.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from importlib import resources
from typing import Any

from jsonschema import Draft202012Validator

MAIN_TOPIC = "wiki.recentchange"
DLQ_TOPIC = "wiki.recentchange.dlq"
CONTRACT = "recentchange.schema.json"  # in wiki_lakehouse/contracts, shipped with the package


@dataclass(frozen=True)
class Routed:
    topic: str
    key: str | None
    value: str
    headers: dict[str, str] = field(default_factory=dict)


@lru_cache(maxsize=1)
def _validator() -> Draft202012Validator:
    schema = resources.files("wiki_lakehouse.contracts").joinpath(CONTRACT).read_text("utf-8")
    return Draft202012Validator(json.loads(schema))


def _dt_error(event: dict[str, Any]) -> str | None:
    """A calendar check for `meta.dt`, on top of the schema's shape `pattern`.

    jsonschema ignores `format: date-time` without a format checker (which would also need
    rfc3339-validator), so the schema pins the shape the downstream Spark parser accepts and
    this rejects impossible dates such as month 13 that the pattern lets through.
    """
    meta = event.get("meta")
    dt = meta.get("dt") if isinstance(meta, dict) else None
    if not isinstance(dt, str):
        return None  # missing or mistyped: the schema already reports it
    try:
        datetime.fromisoformat(dt.replace("Z", "+00:00"))  # 3.10 does not accept "Z"
    except ValueError:
        return f"meta/dt: {dt!r} is not a valid timestamp"
    return None


def contract_errors(event: dict[str, Any]) -> list[str]:
    errors = sorted(_validator().iter_errors(event), key=lambda e: list(e.path))
    messages = [f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in errors]
    if not any(m.startswith("meta/dt:") for m in messages) and (dt_error := _dt_error(event)):
        messages.append(dt_error)
    return messages


def route(raw: str) -> Routed | None:
    """Routing for one raw event body; None means drop it."""
    try:
        event = json.loads(raw)
    except json.JSONDecodeError as e:
        return Routed(DLQ_TOPIC, None, raw, {"error": f"invalid JSON: {e.msg}"})
    if not isinstance(event, dict):
        return Routed(DLQ_TOPIC, None, raw, {"error": "event is not a JSON object"})
    meta = event.get("meta")  # may be any JSON type here: the contract has not been checked yet
    if isinstance(meta, dict) and meta.get("domain") == "canary":
        return None
    errors = contract_errors(event)
    if errors:
        return Routed(DLQ_TOPIC, None, raw, {"error": "; ".join(errors)[:1000]})
    return Routed(MAIN_TOPIC, str(event["wiki"]), raw)
