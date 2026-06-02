"""Reddit Kafka streaming worker — polls subreddits, produces to Kafka."""

import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDDIT_BASE_URL = "https://www.reddit.com"
REDDIT_OAUTH_URL = "https://oauth.reddit.com"
REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "python:reddit-kafka-streamer:v2.0 (streaming worker)",
)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
TOPIC_POSTS = "reddit-posts"
TOPIC_CONTROL = "reddit-control"

_token_cache: dict = {"token": None, "expires_at": 0.0}


def get_access_token() -> str:
    """Return a valid Reddit OAuth access token, refreshing if needed.

    Raises:
        RuntimeError: If REDDIT_CLIENT_ID/REDDIT_CLIENT_SECRET aren't set.
        httpx.HTTPStatusError: If the token request fails.
    """
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        raise RuntimeError(
            "REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET must be set; "
            "create a 'script' app at https://www.reddit.com/prefs/apps"
        )

    if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
        return _token_cache["token"]

    response = httpx.post(
        REDDIT_TOKEN_URL,
        auth=(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET),
        data={"grant_type": "client_credentials"},
        headers={"User-Agent": REDDIT_USER_AGENT},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    _token_cache["token"] = payload["access_token"]
    # Refresh 60s before actual expiry to avoid edge-case 401s mid-request.
    _token_cache["expires_at"] = time.time() + payload["expires_in"] - 60
    return _token_cache["token"]


def fetch_latest_posts(subreddit: str) -> list[dict]:
    """Fetch latest posts from a subreddit via the OAuth API.

    Args:
        subreddit: Name of the subreddit (without r/ prefix).

    Returns:
        List of post dictionaries with post_id field.

    Raises:
        httpx.HTTPStatusError: If the request fails.
        ValueError: If no posts are found.
    """
    url = f"{REDDIT_OAUTH_URL}/r/{subreddit}/new?limit=10"
    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "User-Agent": REDDIT_USER_AGENT,
    }

    response = httpx.get(url, headers=headers, follow_redirects=True, timeout=10)
    response.raise_for_status()

    data = response.json()
    children = data.get("data", {}).get("children", [])

    if not children:
        raise ValueError(f"No posts found in r/{subreddit}")

    posts = []
    for child in children:
        post = child["data"]
        created = datetime.fromtimestamp(post["created_utc"], tz=UTC)
        posts.append({
            "post_id": post["name"],
            "title": post["title"],
            "author": post["author"],
            "score": post["score"],
            "url": post["url"],
            "permalink": f"{REDDIT_BASE_URL}{post['permalink']}",
            "selftext": post.get("selftext", "")[:500] or "(no text)",
            "created_utc": created.isoformat(),
            "num_comments": post["num_comments"],
            "subreddit": post["subreddit"],
        })
    return posts


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
