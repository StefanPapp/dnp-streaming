"""Tests for Redis client helpers."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from app.redis_client import create_job, get_job_status, JOB_TTL


@pytest.mark.asyncio
async def test_create_job_enqueues():
    mock_redis = AsyncMock()
    job_id = await create_job(mock_redis, "python")

    assert isinstance(job_id, str)
    assert len(job_id) > 0

    # Should set job status
    mock_redis.set.assert_called_once()
    call_args = mock_redis.set.call_args
    key = call_args[0][0]
    value = json.loads(call_args[0][1])
    assert key == f"job:{job_id}"
    assert value["status"] == "pending"
    assert value["subreddit"] == "python"

    # Should push to queue
    mock_redis.rpush.assert_called_once_with("job:queue", job_id)


@pytest.mark.asyncio
async def test_get_job_status_found():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = json.dumps(
        {"status": "completed", "subreddit": "python", "result": {"title": "Hi"}}
    )

    result = await get_job_status(mock_redis, "abc123")
    assert result["status"] == "completed"
    assert result["result"]["title"] == "Hi"
    mock_redis.get.assert_called_once_with("job:abc123")


@pytest.mark.asyncio
async def test_get_job_status_not_found():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    result = await get_job_status(mock_redis, "missing")
    assert result is None
