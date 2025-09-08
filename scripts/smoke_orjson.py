#!/usr/bin/env python3
import orjson

def main():
    data = {"ping": "pong"}
    encoded = orjson.dumps(data)
    decoded = orjson.loads(encoded)
    assert decoded == data
    print("orjson encode/decode: OK")

if __name__ == "__main__":
    main()
