"""FastAPI application for Reddit streaming via Kafka + WebSocket."""

import json
import logging
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

app = FastAPI(title="Reddit Streamer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_POSTS = "reddit-posts"
TOPIC_CONTROL = "reddit-control"


def create_producer() -> AIOKafkaProducer:
    """Create a Kafka producer."""
    return AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)


def create_consumer(subreddit: str) -> AIOKafkaConsumer:
    """Create a Kafka consumer for reddit-posts topic."""
    return AIOKafkaConsumer(
        TOPIC_POSTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=None,
        auto_offset_reset="latest",
    )


@app.websocket("/ws/{subreddit}")
async def stream_subreddit(websocket: WebSocket, subreddit: str) -> None:
    """Stream Reddit posts for a subreddit via WebSocket."""
    await websocket.accept()
    subreddit = subreddit.removeprefix("r/")

    producer = create_producer()
    consumer = create_consumer(subreddit)

    await producer.start()
    await consumer.start()

    await producer.send_and_wait(
        TOPIC_CONTROL,
        key=subreddit.encode(),
        value=json.dumps({"action": "subscribe", "subreddit": subreddit}).encode(),
    )
    logger.info("Client subscribed to r/%s", subreddit)

    try:
        async for msg in consumer:
            if msg.key and msg.key.decode() == subreddit:
                post = json.loads(msg.value)
                await websocket.send_json(post)
    except WebSocketDisconnect:
        logger.info("Client disconnected from r/%s", subreddit)
    finally:
        try:
            await producer.send_and_wait(
                TOPIC_CONTROL,
                key=subreddit.encode(),
                value=json.dumps({"action": "unsubscribe", "subreddit": subreddit}).encode(),
            )
        except Exception:
            logger.exception("Failed to send unsubscribe for r/%s", subreddit)
        await consumer.stop()
        await producer.stop()


@app.get("/api/health")
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}
