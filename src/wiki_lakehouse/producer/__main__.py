"""Command line for the producer.

    python -m wiki_lakehouse.producer live   [--record FILE] [--state-file PATH]
    python -m wiki_lakehouse.producer replay FILE [--rate 500] [--loops 10] [--shift-to-now]
                                                  [--id-prefix PREFIX]

Exit status is 1 when Kafka rejected or never confirmed a message, so a lost delivery fails
the command (and, in live mode, compose restarts it from the last confirmed position).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
from pathlib import Path
from types import FrameType

from wiki_lakehouse.producer.kafka_publisher import DeliveryError, KafkaPublisher
from wiki_lakehouse.producer.producer_runs import publish_events, publish_summary, replay_bodies
from wiki_lakehouse.producer.resume_cursor import ResumeCursor, resume_state_file
from wiki_lakehouse.producer.sse_stream import stream_events

log = logging.getLogger("wiki_lakehouse.producer")


def _exit_on_sigterm(signum: int, frame: FrameType | None) -> None:
    # In the container Python is PID 1, where the kernel ignores SIGTERM unless a handler is
    # installed. Raising SystemExit unwinds through the `finally` blocks, which flush Kafka
    # and close the recorder, so `docker stop` is a clean shutdown rather than a SIGKILL.
    raise SystemExit(0)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="wiki_lakehouse.producer")
    parser.add_argument("--bootstrap", default=os.environ.get("LAKEHOUSE_KAFKA_BOOTSTRAP",
                                                              "localhost:19092"))
    sub = parser.add_subparsers(dest="command", required=True)
    live = sub.add_parser("live", help="Stream live edits from Wikimedia.")
    live.add_argument("--record", type=Path, help="Also append raw events to this JSONL file.")
    live.add_argument("--state-file", type=Path,
                      default=Path(os.environ.get("PRODUCER_STATE_FILE", "/state/last_event_id")),
                      help="Resume position; resume is disabled if its directory is missing.")
    replay = sub.add_parser("replay", help="Publish a recorded JSONL file.")
    replay.add_argument("file", type=Path)
    replay.add_argument("--rate", type=float,
                        help="Events per second (default: as fast as possible).")
    replay.add_argument("--loops", type=int, default=1)
    replay.add_argument("--shift-to-now", action="store_true",
                        help="Rewrite event times to now (for freshness measurements).")
    replay.add_argument("--id-prefix",
                        help="Prefix for event ids, with or without --shift-to-now "
                             "(keeps separate replays unique).")
    return parser


def _run_live(args: argparse.Namespace, user_agent: str) -> None:
    cursor = ResumeCursor(resume_state_file(args.state_file))
    events = stream_events(user_agent, last_event_id=cursor.read())
    publisher = KafkaPublisher(args.bootstrap)
    recorder = args.record.open("a", encoding="utf-8") if args.record else None
    try:
        publish_events(cursor.track(events), publisher, recorder=recorder, cursor=cursor)
    finally:
        if recorder:
            recorder.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = _parser()
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, _exit_on_sigterm)

    if args.command == "live":
        user_agent = os.environ.get("WIKIMEDIA_USER_AGENT")
        if not user_agent:  # validated before any connection to Kafka is made
            parser.error("set WIKIMEDIA_USER_AGENT (Wikimedia policy: tool name + contact)")
        try:
            _run_live(args, user_agent)
        except DeliveryError as e:
            log.critical("stopping: %s; the resume position stays at the last confirmed event", e)
            sys.exit(1)
        return

    if args.loops < 1:
        parser.error("--loops must be at least 1")
    if args.rate is not None and args.rate <= 0:
        parser.error("--rate must be positive")
    if not args.file.is_file():
        parser.error(f"recording not found: {args.file}")
    bodies = replay_bodies(args.file, rate_per_s=args.rate, loops=args.loops,
                           shift_to_now=args.shift_to_now, id_prefix=args.id_prefix)
    summary, delivered = publish_summary(bodies, KafkaPublisher(args.bootstrap))
    # The benchmark driver parses this line for the achieved publish rate.
    print(json.dumps(summary), flush=True)
    if not delivered:
        sys.exit(1)


if __name__ == "__main__":
    main()
