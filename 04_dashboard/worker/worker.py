"""Reddit Kafka streaming worker — polls subreddit RSS feeds, produces to Kafka."""

import asyncio
import html
import json
import logging
import os
import re
from datetime import UTC, datetime

import feedparser
import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDDIT_BASE_URL = "https://www.reddit.com"
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "python:reddit-rss-streamer:v3.0 (streaming worker)",
)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
TOPIC_POSTS = "reddit-posts"
TOPIC_CONTROL = "reddit-control"


def _strip_html(raw: str) -> str:
    """Reduce an HTML fragment to collapsed plain text."""
    text = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _entry_to_post(entry, subreddit: str) -> dict:
    """Map a feedparser Atom entry to the post schema used across the pipeline.

    The public RSS feed exposes no score or comment count, so those fields are
    set to 0; every other field keeps the same shape as the former OAuth path
    so the Kafka payload, storage worker, and dashboard are unaffected.
    """
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed:
        created = datetime(*parsed[:6], tzinfo=UTC).isoformat()
    else:
        created = entry.get("published") or entry.get("updated") or ""

    summary = entry.get("summary") or ""
    if not summary and entry.get("content"):
        summary = entry["content"][0].get("value", "")

    link = entry.get("link", "")
    return {
        "post_id": entry.get("id", ""),
        "title": entry.get("title", ""),
        "author": (entry.get("author") or "").removeprefix("/u/"),
        "score": 0,  # not exposed by the RSS feed
        "url": link,
        "permalink": link,
        "selftext": _strip_html(summary)[:500] or "(no text)",
        "created_utc": created,
        "num_comments": 0,  # not exposed by the RSS feed
        "subreddit": subreddit,
    }


def fetch_latest_posts(subreddit: str) -> list[dict]:
    """Fetch latest posts from a subreddit's public RSS/Atom feed.

    Uses the unauthenticated ``/new/.rss`` feed, so no Reddit credentials are
    required. A descriptive User-Agent is still sent — Reddit rate-limits
    generic agents.

    Args:
        subreddit: Name of the subreddit (without r/ prefix).

    Returns:
        List of post dictionaries with a ``post_id`` field.

    Raises:
        httpx.HTTPStatusError: If the request fails.
        ValueError: If the feed contains no entries.
    """
    url = f"{REDDIT_BASE_URL}/r/{subreddit}/new/.rss"
    headers = {"User-Agent": REDDIT_USER_AGENT}

    response = httpx.get(url, headers=headers, follow_redirects=True, timeout=10)
    response.raise_for_status()

    feed = feedparser.parse(response.content)
    if not feed.entries:
        raise ValueError(f"No posts found in r/{subreddit}")

    return [_entry_to_post(entry, subreddit) for entry in feed.entries]


class RedditStreamer:
    """Streams Reddit posts to Kafka based on control messages."""

    def __init__(self, kafka_bootstrap: str, producer: AIOKafkaProducer | None = None) -> None:
        self.kafka_bootstrap = kafka_bootstrap
        self.producer = producer
        self.subscriptions: set[str] = set()
        self.seen_ids: dict[str, set[str]] = {}

    async def handle_control_message(self, msg) -> None:
        """Process a subscribe/unsubscribe control message."""
        data = json.loads(msg.value)
        action = data.get("action")
        subreddit = data.get("subreddit", "").removeprefix("r/")

        if action == "subscribe":
            self.subscriptions.add(subreddit)
            self.seen_ids.setdefault(subreddit, set())
            logger.info("Subscribed to r/%s (active: %d)", subreddit, len(self.subscriptions))
        elif action == "unsubscribe":
            self.subscriptions.discard(subreddit)
            self.seen_ids.pop(subreddit, None)
            logger.info("Unsubscribed from r/%s (active: %d)", subreddit, len(self.subscriptions))

    async def produce_new_posts(self, subreddit: str) -> None:
        """Fetch and publish new posts for a subreddit.

        Any failure fetching or publishing is logged and contained so a single
        bad poll (missing/invalid creds, a Reddit hiccup, a Kafka blip) never
        tears down the worker — the next poll cycle simply retries. ``except
        Exception`` deliberately excludes ``CancelledError`` so shutdown still
        propagates cleanly.
        """
        try:
            posts = fetch_latest_posts(subreddit)

            seen = self.seen_ids.setdefault(subreddit, set())
            for post in posts:
                if post["post_id"] not in seen:
                    seen.add(post["post_id"])
                    await self.producer.send_and_wait(
                        TOPIC_POSTS,
                        key=subreddit.encode(),
                        value=json.dumps(post).encode(),
                    )
                    logger.info("Published post %s from r/%s", post["post_id"], subreddit)

            # Cap seen set to prevent unbounded growth
            if len(seen) > 500:
                excess = len(seen) - 500
                for _ in range(excess):
                    seen.pop()
        except Exception:
            logger.exception("Skipping r/%s this cycle", subreddit)

    async def poll_loop(self) -> None:
        """Periodically poll all subscribed subreddits."""
        while True:
            for subreddit in list(self.subscriptions):
                await self.produce_new_posts(subreddit)
            await asyncio.sleep(POLL_INTERVAL)

    async def control_loop(self, consumer: AIOKafkaConsumer) -> None:
        """Listen for control messages."""
        async for msg in consumer:
            await self.handle_control_message(msg)


async def main() -> None:
    """Start the streaming worker."""
    logger.info("Worker starting, Kafka bootstrap: %s", KAFKA_BOOTSTRAP)

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    consumer = AIOKafkaConsumer(
        TOPIC_CONTROL,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="reddit-worker",
        auto_offset_reset="latest",
    )

    await producer.start()
    await consumer.start()
    logger.info("Worker ready, listening for control messages...")

    streamer = RedditStreamer(kafka_bootstrap=KAFKA_BOOTSTRAP, producer=producer)

    try:
        await asyncio.gather(
            streamer.control_loop(consumer),
            streamer.poll_loop(),
        )
    except asyncio.CancelledError:
        logger.info("Worker shutting down")
    finally:
        await producer.stop()
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
