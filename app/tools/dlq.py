"""Dead-letter queue inspector / reprocessor.

Usage:
    python -m app.tools.dlq list [--count N]
    python -m app.tools.dlq stats
    python -m app.tools.dlq replay [--count N]   # re-inject originals upstream

``replay`` reads DLQ entries, restores the original message body onto its
source stream (so the pipeline retries it), and deletes the DLQ entry only
after a successful re-publish.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter

from ..config import get_settings
from ..streams import create_redis

settings = get_settings()


async def _list(count: int) -> None:
    redis = create_redis(settings)
    entries = await redis.xrange(settings.stream_dlq, count=count)
    if not entries:
        print("DLQ is empty.")
    for msg_id, fields in entries:
        print(f"\n[{msg_id}] source={fields.get('source_stream')} "
              f"reason={fields.get('reason_code')} attempts={fields.get('attempts')}")
        print(f"  error: {fields.get('error')}")
    await redis.aclose()


async def _stats() -> None:
    redis = create_redis(settings)
    total = await redis.xlen(settings.stream_dlq)
    entries = await redis.xrange(settings.stream_dlq)
    by_reason = Counter(f.get("reason_code", "?") for _, f in entries)
    by_source = Counter(f.get("source_stream", "?") for _, f in entries)
    print(f"DLQ total: {total}")
    print("by reason:", dict(by_reason))
    print("by source:", dict(by_source))
    await redis.aclose()


async def _replay(count: int) -> None:
    redis = create_redis(settings)
    entries = await redis.xrange(settings.stream_dlq, count=count)
    replayed = 0
    for msg_id, fields in entries:
        source = fields.get("source_stream")
        original = fields.get("original")
        if not source or not original:
            continue
        body = json.loads(original)
        await redis.xadd(source, body)
        await redis.xdel(settings.stream_dlq, msg_id)
        replayed += 1
        print(f"replayed {msg_id} -> {source}")
    print(f"\nReplayed {replayed} message(s).")
    await redis.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description="DLQ inspector/reprocessor")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_list = sub.add_parser("list")
    p_list.add_argument("--count", type=int, default=20)
    sub.add_parser("stats")
    p_replay = sub.add_parser("replay")
    p_replay.add_argument("--count", type=int, default=100)

    args = parser.parse_args()
    if args.cmd == "list":
        asyncio.run(_list(args.count))
    elif args.cmd == "stats":
        asyncio.run(_stats())
    elif args.cmd == "replay":
        asyncio.run(_replay(args.count))


if __name__ == "__main__":
    main()
