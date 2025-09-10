#!/usr/bin/env python3
# ingest/nats_client.py
# Usage:
#   python ingest/nats_client.py --mode json   # publish JSON messages
#   python ingest/nats_client.py --mode msgpack  # publish MessagePack messages
# It will publish N messages and measure ack latencies.

import asyncio, time, argparse, statistics
import json, orjson, msgpack
from nats.aio.client import Client as NATS

DEFAULT_N = 200

async def publish_bench(mode="json", n=DEFAULT_N):
    nc = NATS()
    await nc.connect(servers=["nats://127.0.0.1:4222"])
    js = nc.jetstream()
    subject = "mexc.raw.test"

    latencies = []
    for i in range(n):
        payload = {
            "symbol": "TEST_XYZ",
            "price": 123.45 + (i % 10) * 0.001,
            "qty": 0.01,
            "seq_local": i,
            "ts_local": int(time.time() * 1_000_000)  # microseconds for dev
        }
        if mode == "json":
            b = orjson.dumps(payload)
        else:
            b = msgpack.packb(payload, use_bin_type=True)

        t0 = time.perf_counter()
        ack = await js.publish(subject, b)
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1_000_000)  # microseconds

        # tiny batch throttle if needed (keep latency low): no sleep by default
    await nc.close()
    return latencies

def summarize(latencies):
    print("count:", len(latencies))
    print("median_us:", statistics.median(latencies))
    print("p90_us:", statistics.quantiles(latencies, n=10)[8])
    print("max_us:", max(latencies))
    print("mean_us:", statistics.mean(latencies))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["json", "msgpack"], default="json")
    parser.add_argument("--n", type=int, default=DEFAULT_N)
    args = parser.parse_args()
    print("Mode:", args.mode, "N:", args.n)
    res = asyncio.run(publish_bench(args.mode, args.n))
    summarize(res)
