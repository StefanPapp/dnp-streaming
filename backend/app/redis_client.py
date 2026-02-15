"""Redis client helpers for job management."""

import json
import uuid

from redis.asyncio import Redis

JOB_TTL = 300  # 5 minutes


async def create_job(redis: Redis, subreddit: str) -> str:
    """Create a new fetch job and enqueue it.

    Args:
        redis: Async Redis connection.
        subreddit: Subreddit name to fetch.

    Returns:
        The generated job ID.
    """
    job_id = uuid.uuid4().hex[:12]
    job_data = json.dumps({"status": "pending", "subreddit": subreddit})
    await redis.set(f"job:{job_id}", job_data, ex=JOB_TTL)
    await redis.rpush("job:queue", job_id)
    return job_id


async def get_job_status(redis: Redis, job_id: str) -> dict | None:
    """Get the current status of a job.

    Args:
        redis: Async Redis connection.
        job_id: The job ID to look up.

    Returns:
        Job data dict or None if not found.
    """
    raw = await redis.get(f"job:{job_id}")
    if raw is None:
        return None
    return json.loads(raw)
