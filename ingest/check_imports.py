# ingest/check_imports.py
import sys
import asyncio
import time

try:
    import uvloop, orjson
    print("uvloop:", getattr(uvloop, "__version__", "unknown"))
    print("orjson:", getattr(orjson, "__version__", "unknown"))
except Exception as e:
    print("IMPORT ERROR:", type(e).__name__, e)
    sys.exit(1)

# quick event-loop + parse test
async def bench():
    start = time.time()
    s = b'{"a": 1, "b": "x", "c": [1,2,3]}' * 10000
    for _ in range(20):
        orjson.loads(s)
    print("orjson parse test OK")
    return True

if __name__ == "__main__":
    try:
        uvloop.install()
        print("uvloop installed as default loop")
    except Exception as _:
        print("uvloop not installed or couldn't be set")
    asyncio.run(bench())
