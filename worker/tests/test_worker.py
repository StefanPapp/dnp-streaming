"""Tests for the Reddit fetcher worker."""

import json
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from worker import fetch_latest_post, process_job


def test_fetch_latest_post_success():
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": {
            "children": [
                {
                    "data": {
                        "title": "Test Post",
                        "author": "testuser",
                        "score": 42,
                        "url": "https://example.com",
                        "permalink": "/r/python/comments/abc/test/",
                        "selftext": "Hello world",
                        "created_utc": 1700000000.0,
                        "num_comments": 5,
                        "subreddit": "python",
                    }
                }
            ]
        }
    }
    mock_response.raise_for_status = MagicMock()

    with patch("worker.httpx.get", return_value=mock_response):
        result = fetch_latest_post("python")

    assert result["title"] == "Test Post"
    assert result["author"] == "testuser"
    assert result["score"] == 42


def test_fetch_latest_post_empty():
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"children": []}}
    mock_response.raise_for_status = MagicMock()

    with patch("worker.httpx.get", return_value=mock_response):
        with pytest.raises(ValueError, match="No posts found"):
            fetch_latest_post("nonexistent_sub_xyz")


@pytest.mark.asyncio
async def test_process_job_success():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = json.dumps(
        {"status": "pending", "subreddit": "python"}
    )

    with patch("worker.fetch_latest_post", return_value={"title": "Test"}):
        await process_job(mock_redis, "job123")

    # Verify result was stored
    mock_redis.set.assert_called_once()
    call_args = mock_redis.set.call_args
    stored = json.loads(call_args[0][1])
    assert stored["status"] == "completed"
    assert stored["result"]["title"] == "Test"


@pytest.mark.asyncio
async def test_process_job_fetch_failure():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = json.dumps(
        {"status": "pending", "subreddit": "bad_sub"}
    )

    with patch("worker.fetch_latest_post", side_effect=ValueError("No posts found")):
        await process_job(mock_redis, "job123")

    call_args = mock_redis.set.call_args
    stored = json.loads(call_args[0][1])
    assert stored["status"] == "failed"
    assert "No posts found" in stored["error"]
