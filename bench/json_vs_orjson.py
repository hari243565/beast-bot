#!/usr/bin/env python3
import time, json, orjson

# one sample trade message
sample = {"symbol": "BTCUSDT", "price": 30000.123, "qty": 0.01}

# create a list of 10k messages with unique seq numbers
payload = [dict(sample, seq=i) for i in range(10000)]

def bench_json():
    start = time.time()
    for p in payload:
        _ = json.dumps(p)
    return time.time() - start

def bench_orjson():
    start = time.time()
    for p in payload:
        _ = orjson.dumps(p)
    return time.time() - start

if __name__ == "__main__":
    t1 = bench_json()
    t2 = bench_orjson()
    print(f"json:   {t1:.3f}s")
    print(f"orjson: {t2:.3f}s")
    print(f"speedup: {t1/t2:.2f}x")
