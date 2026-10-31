"""Command line for the producer.

    python -m wiki_lakehouse.producer live   [--record FILE]
    python -m wiki_lakehouse.producer replay FILE [--rate 500] [--loops 10] [--shift-to-now]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path

from wiki_lakehouse.producer.kafka_publisher import KafkaPublisher
from wiki_lakehouse.producer.producer_runs import (
    live_bodies,
    publish_events,
    read_last_event_id,
    replay_bodies,
)
from wiki_lakehouse.producer.sse_stream import stream_events


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="wiki_lakehouse.producer")
    parser.add_argument("--bootstrap", default=os.environ.get("LAKEHOUSE_KAFKA_BOOTSTRAP",
                                                              "localhost:19092"))
    sub = parser.add_subparsers(dest="command", required=True)
    live = sub.add_parser("live", help="Stream live edits from Wikimedia.")
    live.add_argument("--record", type=Path, help="Also append raw events to this JSONL file.")
    live.add_argument("--state-file", type=Path,
                      default=Path(os.environ.get("PRODUCER_STATE_FILE", "/state/last_event_id")))
    replay = sub.add_parser("replay", help="Publish a recorded JSONL file.")
    replay.add_argument("file", type=Path)
    replay.add_argument("--rate", type=float,
                        help="Events per second (default: as fast as possible).")
    replay.add_argument("--loops", type=int, default=1)
    replay.add_argument("--shift-to-now", action="store_true",
                        help="Rewrite event times to now (for freshness measurements).")
    replay.add_argument("--id-prefix", help="Prefix for event ids (keeps separate replays unique).")
    args = parser.parse_args()

    publisher = KafkaPublisher(args.bootstrap)
    if args.command == "live":
        user_agent = os.environ.get("WIKIMEDIA_USER_AGENT")
        if not user_agent:
            parser.error("set WIKIMEDIA_USER_AGENT (Wikimedia policy: tool name + contact)")
        state_file = args.state_file if args.state_file.parent.exists() else None
        events = stream_events(user_agent, last_event_id=read_last_event_id(state_file))
        recorder = args.record.open("a", encoding="utf-8") if args.record else None
        try:
            publish_events(live_bodies(events, state_file), publisher, recorder=recorder)
        finally:
            if recorder:
                recorder.close()
    else:
        bodies = replay_bodies(args.file, rate_per_s=args.rate, loops=args.loops,
                               shift_to_now=args.shift_to_now, id_prefix=args.id_prefix)
        started = time.monotonic()
        counts = publish_events(bodies, publisher)
        elapsed = time.monotonic() - started
        total = sum(counts.values())
        # The benchmark driver parses this line for the achieved publish rate.
        print(json.dumps({"published": dict(counts), "seconds": round(elapsed, 2),
                          "events_per_s": round(total / elapsed, 1) if elapsed else None}),
              flush=True)


if __name__ == "__main__":
    main()
