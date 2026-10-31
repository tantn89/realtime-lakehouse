"""Kafka publishing behind a small interface, so routing and replay test without a broker."""

from __future__ import annotations

import logging
from typing import Any, Protocol

from wiki_lakehouse.producer.event_routing import Routed

log = logging.getLogger(__name__)


class Publisher(Protocol):
    def publish(self, message: Routed) -> None: ...
    def flush(self) -> None: ...


class KafkaPublisher:
    """Idempotent producer: retries never create duplicates inside Kafka itself."""

    def __init__(self, bootstrap: str) -> None:
        from confluent_kafka import Producer  # imported lazily: tests run without librdkafka

        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 50,
            "client.id": "wiki-recentchange-producer",
        })
        self.failed = 0

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

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        if remaining:
            log.error("%d messages still undelivered after flush", remaining)


class ListPublisher:
    """In-memory publisher for tests and dry runs."""

    def __init__(self) -> None:
        self.messages: list[Routed] = []

    def publish(self, message: Routed) -> None:
        self.messages.append(message)

    def flush(self) -> None:
        pass
