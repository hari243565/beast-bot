#!/usr/bin/env python3
# scripts/create_nats_streams.py
import asyncio, json, sys
from nats.aio.client import Client as NATS

# Streams we want to create and their subjects
STREAMS = [
    ("mexc_raw", ["mexc.raw.>"]),
    ("mexc_trades", ["mexc.trades.>"]),
    ("mexc_book_diff", ["mexc.book.diff.>"]),
    ("mexc_listings", ["mexc.listings"]),
]

async def main():
    nc = NATS()
    await nc.connect(servers=["nats://127.0.0.1:4222"])
    js = nc.jetstream()

    for name, subjects in STREAMS:
        try:
            # Try to add stream; if exists, exception will be raised - catch and ignore
            await js.add_stream(name=name, subjects=subjects, storage="file", max_msgs=-1)
            print("Created stream:", name, "subjects:", subjects)
        except Exception as e:
            # If already exists, we still want to continue
            print("Stream exists or error for", name, "-", str(e))
    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
