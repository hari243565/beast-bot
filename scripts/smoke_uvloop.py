#!/usr/bin/env python3
import asyncio, uvloop

def main():
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    print("uvloop import & event loop creation: OK")

if __name__ == "__main__":
    main()
