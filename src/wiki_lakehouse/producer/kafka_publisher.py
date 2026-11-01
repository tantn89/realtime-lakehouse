"""Kafka publishing behind a small interface, so routing and replay test without a broker."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol

from wiki_lakehouse.producer.event_routing import Routed

log = logging.getLogger(__name__)


class Publisher(Protocol):
    # Deliveries Kafka rejected so far (cumulative since the publisher was created).
    failed: int

    def publish(self, message: Routed) -> None: ...

    def flush(self) -> int:
        """Wait for outstanding deliveries; returns how many are still unconfirmed."""
        ...


class DeliveryError(RuntimeError):
    """Some messages were not confirmed by Kafka; anything after the last checkpoint may be lost.

    `counts` are the per-destination counts routed before the failure was detected.
    """

    def __init__(self, failed: int, undelivered: int, counts: Mapping[str, int]) -> None:
        super().__init__(f"{failed} deliveries failed, {undelivered} unconfirmed after flush")
        self.failed = failed
        self.undelivered = undelivered
        self.counts = dict(counts)


class KafkaPublisher:
    """Idempotent producer: retries never create duplicates inside Kafka itself."""

    def __init__(self, bootstrap: str, *, flush_timeout_s: float = 30.0) -> None:
        from confluent_kafka import Producer  # imported lazily: tests run without librdkafka

        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 50,
            "client.id": "wiki-recentchange-producer",
            "error_cb": self._on_error,
        })
        self._flush_timeout_s = flush_timeout_s
        self.failed = 0

    @staticmethod
    def _on_error(err: Any) -> None:
        # Client-level errors (broker down, fatal idempotence errors); per-message failures
        # arrive through the delivery callback instead.
        fatal = err.fatal() if hasattr(err, "fatal") else False
        log.log(logging.CRITICAL if fatal else logging.WARNING, "kafka client error: %s", err)

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed for %s: %s", msg.topic(), err)

    def publish(self, message: Routed) -> None:
        while True:
            try:
                self._producer.produce(
                    message.topic,
                    key=message.key,
                    value=message.value.encode("utf-8"),
                    headers=list(message.headers.items()),
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:  # local queue full: let it drain, then retry
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self) -> int:
        remaining = int(self._producer.flush(self._flush_timeout_s))
        if remaining:
            log.error("%d messages still undelivered after flush", remaining)
        return remaining


class ListPublisher:
    """In-memory publisher for tests and dry runs: every message counts as delivered."""

    def __init__(self) -> None:
        self.messages: list[Routed] = []
        self.failed = 0

    def publish(self, message: Routed) -> None:
        self.messages.append(message)

    def flush(self) -> int:
        return 0
