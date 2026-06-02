"""Tests for the Reddit Kafka streaming worker."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker import fetch_latest_posts, RedditStreamer


SAMPLE_REDDIT_RESPONSE = {
    "data": {
        "children": [
            {
                "data": {
                    "name": "t3_abc123",
                    "title": "Test Post 1",
                    "author": "user1",
                    "score": 42,
                    "url": "https://example.com/1",
                    "permalink": "/r/python/comments/abc123/test1/",
                    "selftext": "Hello world",
                    "created_utc": 1700000000.0,
                    "num_comments": 5,
                    "subreddit": "python",
                }
            },
            {
                "data": {
                    "name": "t3_def456",
                    "title": "Test Post 2",
                    "author": "user2",
                    "score": 10,
                    "url": "https://example.com/2",
                    "permalink": "/r/python/comments/def456/test2/",
                    "selftext": "",
                    "created_utc": 1700000100.0,
                    "num_comments": 2,
                    "subreddit": "python",
                }
            },
        ]
    }
}


def test_fetch_latest_posts_success():
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_REDDIT_RESPONSE
    mock_response.raise_for_status = MagicMock()

    with (
        patch("worker.httpx.get", return_value=mock_response),
        patch("worker.get_access_token", return_value="fake_token"),
    ):
        posts = fetch_latest_posts("python")

    assert len(posts) == 2
    assert posts[0]["title"] == "Test Post 1"
    assert posts[0]["post_id"] == "t3_abc123"
    assert posts[1]["post_id"] == "t3_def456"


def test_fetch_latest_posts_empty():
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"children": []}}
    mock_response.raise_for_status = MagicMock()

    with (
        patch("worker.httpx.get", return_value=mock_response),
        patch("worker.get_access_token", return_value="fake_token"),
    ):
        with pytest.raises(ValueError, match="No posts found"):
            fetch_latest_posts("nonexistent_sub")


def test_fetch_latest_posts_includes_post_id():
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_REDDIT_RESPONSE
    mock_response.raise_for_status = MagicMock()

    with (
        patch("worker.httpx.get", return_value=mock_response),
        patch("worker.get_access_token", return_value="fake_token"),
    ):
        posts = fetch_latest_posts("python")

    for post in posts:
        assert "post_id" in post


@pytest.mark.asyncio
async def test_streamer_produce_new_posts():
    """Only unseen posts are sent to Kafka."""
    mock_producer = AsyncMock()
    streamer = RedditStreamer(
        kafka_bootstrap="localhost:9092",
        producer=mock_producer,
    )
    streamer.subscriptions = {"python"}

    posts = [
        {"post_id": "t3_abc", "title": "Post A", "subreddit": "python"},
        {"post_id": "t3_def", "title": "Post B", "subreddit": "python"},
    ]

    with patch("worker.fetch_latest_posts", return_value=posts):
        await streamer.produce_new_posts("python")

    assert mock_producer.send_and_wait.call_count == 2


@pytest.mark.asyncio
async def test_streamer_deduplicates_posts():
    """Second call doesn't re-publish already-seen posts."""
    mock_producer = AsyncMock()
    streamer = RedditStreamer(
        kafka_bootstrap="localhost:9092",
        producer=mock_producer,
    )
    streamer.subscriptions = {"python"}

    posts = [
        {"post_id": "t3_abc", "title": "Post A", "subreddit": "python"},
    ]

    with patch("worker.fetch_latest_posts", return_value=posts):
        await streamer.produce_new_posts("python")
        mock_producer.send_and_wait.reset_mock()
        await streamer.produce_new_posts("python")

    assert mock_producer.send_and_wait.call_count == 0


@pytest.mark.asyncio
async def test_handle_control_subscribe():
    mock_producer = AsyncMock()
    streamer = RedditStreamer(
        kafka_bootstrap="localhost:9092",
        producer=mock_producer,
    )

    msg = MagicMock()
    msg.value = json.dumps({"action": "subscribe", "subreddit": "python"}).encode()

    await streamer.handle_control_message(msg)
    assert "python" in streamer.subscriptions


@pytest.mark.asyncio
async def test_handle_control_unsubscribe():
    mock_producer = AsyncMock()
    streamer = RedditStreamer(
        kafka_bootstrap="localhost:9092",
        producer=mock_producer,
    )
    streamer.subscriptions = {"python"}

    msg = MagicMock()
    msg.value = json.dumps({"action": "unsubscribe", "subreddit": "python"}).encode()

    await streamer.handle_control_message(msg)
    assert "python" not in streamer.subscriptions
