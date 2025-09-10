import asyncio
from nats.aio.client import Client as NATS

async def main():
    async def message_handler(msg):
        print(f"Received a message on '{msg.subject}': {msg.data.decode()}")

    nc = NATS()
    await nc.connect("nats://localhost:4222")

    # Subscribe to a test subject
    await nc.subscribe("test.subject", cb=message_handler)

    # Publish a test message
    await nc.publish("test.subject", b"hello from beast bot!")

    # Allow some time to receive the message back
    await asyncio.sleep(1)

    await nc.drain()

if __name__ == "__main__":
    asyncio.run(main())
