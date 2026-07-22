import asyncio
import json
import time

import websockets
from confluent_kafka import Producer

SOCKET_URL = "wss://ws-feed.exchange.coinbase.com/"
PRODUCT_IDS = ["BTC-USD", "ETH-USD", "SOL-USD"]

INITIAL_DELAY = 1.0
MAX_DELAY = 60.0
BACKOFF_FACTOR = 2.0

# Define configuration pointing to local Redpanda
config = {
    "bootstrap.servers": "localhost:19092",  # Change to 9092 if using default local port
    "client.id": "local-python-producer",
}

producer = Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


async def coinbase_stream():
    # Websocket connection drop retry delay
    delay = INITIAL_DELAY

    while True:
        try:
            async with websockets.connect(SOCKET_URL) as websocket:
                COUNTER = 0
                N = 10

                delay = INITIAL_DELAY
                print("Connected to Coinbase server!")

                # Subscription payload
                subscribe_message = {
                    "type": "subscribe",
                    "product_ids": PRODUCT_IDS,
                    "channels": ["matches"],
                }

                # Send subscription immediately on connect
                await websocket.send(json.dumps(subscribe_message))
                print(f"Subscribed to matches channel for {PRODUCT_IDS}")

                start_time = time.time()

                # Listed to incoming messages
                async for message in websocket:
                    COUNTER = COUNTER + 1
                    data = json.loads(message)

                    # Filter for match events
                    if data.get("type") == "match":
                        # Produce message to kafka
                        topic = "trades"
                        producer.produce(
                            topic,
                            key=data.get("product_id"),
                            value=json.dumps(data).encode(),
                            callback=delivery_report,
                        )

                        producer.poll(0)

                        if COUNTER % N == 0:
                            elapsed_time = time.time() - start_time
                            print(
                                f"Heartbeat: {COUNTER / elapsed_time} messages/second"
                            )
        except (websockets.exceptions.ConnectionClosed, OSError) as e:
            print(f"Connection error encountered: {e}")
            print(f"Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay = min(delay * BACKOFF_FACTOR, MAX_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(coinbase_stream())
    except KeyboardInterrupt:
        print("\nStream Stopped")

    producer.flush()
