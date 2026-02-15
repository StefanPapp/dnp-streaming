"""Reddit fetcher worker — consumes jobs from Redis queue."""

import asyncio
import json
import logging
import os
from datetime import UTC, datetime

import httpx
from redis.asyncio import Redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDDIT_BASE_URL = "https://www.reddit.com"
JOB_TTL = 300


def fetch_latest_post(subreddit: str) -> dict:
    """Fetch the latest post from a subreddit.

    Args:
        subreddit: Name of the subreddit (without r/ prefix).

    Returns:
        Dictionary with post details.

    Raises:
        httpx.HTTPStatusError: If the request fails.
        ValueError: If no posts are found.
    """
    url = f"{REDDIT_BASE_URL}/r/{subreddit}/new.json?limit=1"
    headers = {"User-Agent": "python:reddit-fetcher-worker:v1.0 (job worker)"}

    response = httpx.get(url, headers=headers, follow_redirects=True, timeout=10)
    response.raise_for_status()

    data = response.json()
    children = data.get("data", {}).get("children", [])

    if not children:
        raise ValueError(f"No posts found in r/{subreddit}")

    post = children[0]["data"]
    created = datetime.fromtimestamp(post["created_utc"], tz=UTC)

    return {
        "title": post["title"],
        "author": post["author"],
        "score": post["score"],
        "url": post["url"],
        "permalink": f"{REDDIT_BASE_URL}{post['permalink']}",
        "selftext": post.get("selftext", "")[:500] or "(no text)",
        "created_utc": created.isoformat(),
        "num_comments": post["num_comments"],
        "subreddit": post["subreddit"],
    }


async def process_job(redis: Redis, job_id: str) -> None:
    """Process a single fetch job.

    Args:
        redis: Async Redis connection.
        job_id: The job ID to process.
    """
    raw = await redis.get(f"job:{job_id}")
    if raw is None:
        logger.warning("Job %s not found, skipping", job_id)
        return

    job_data = json.loads(raw)
    subreddit = job_data["subreddit"]
    logger.info("Processing job %s for r/%s", job_id, subreddit)

    try:
        result = fetch_latest_post(subreddit)
        job_data["status"] = "completed"
        job_data["result"] = result
    except (httpx.HTTPStatusError, ValueError) as e:
        logger.exception("Failed to fetch r/%s for job %s", subreddit, job_id)
        job_data["status"] = "failed"
        job_data["error"] = str(e)

    await redis.set(f"job:{job_id}", json.dumps(job_data), ex=JOB_TTL)


async def main() -> None:
    """Main worker loop — blocks on Redis queue."""
    logger.info("Worker starting, connecting to %s", REDIS_URL)
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    try:
        logger.info("Worker ready, waiting for jobs...")
        while True:
            result = await redis.blpop("job:queue", timeout=0)
            if result is None:
                continue
            _, job_id = result
            await process_job(redis, job_id)
    except asyncio.CancelledError:
        logger.info("Worker shutting down")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
