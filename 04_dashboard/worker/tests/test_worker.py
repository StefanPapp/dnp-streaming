"""Tests for the Reddit Kafka streaming worker."""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker import fetch_latest_posts, RedditStreamer


SAMPLE_ATOM = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>newest submissions : python</title>
  <entry>
    <author><name>/u/user1</name></author>
    <id>t3_abc123</id>
    <link href="https://www.reddit.com/r/python/comments/abc123/test1/" />
    <published>2023-11-14T22:13:20+00:00</published>
    <updated>2023-11-14T22:13:20+00:00</updated>
    <title>Test Post 1</title>
    <content type="html">&lt;p&gt;Hello world&lt;/p&gt;</content>
  </entry>
  <entry>
    <author><name>/u/user2</name></author>
    <id>t3_def456</id>
    <link href="https://www.reddit.com/r/python/comments/def456/test2/" />
    <published>2023-11-14T22:15:00+00:00</published>
    <updated>2023-11-14T22:15:00+00:00</updated>
    <title>Test Post 2</title>
    <content type="html">&lt;p&gt;Second post&lt;/p&gt;</content>
  </entry>
</feed>
"""

EMPTY_ATOM = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>newest submissions : nothing</title>
</feed>
"""


def _rss_response(content: bytes) -> MagicMock:
    resp = MagicMock()
    resp.content = content
    resp.raise_for_status = MagicMock()
    return resp


def test_fetch_latest_posts_parses_rss():
    with patch("worker.httpx.get", return_value=_rss_response(SAMPLE_ATOM)):
        posts = fetch_latest_posts("python")

    assert len(posts) == 2
    first = posts[0]
    assert first["post_id"] == "t3_abc123"
    assert first["title"] == "Test Post 1"
    assert first["author"] == "user1"  # "/u/" prefix stripped
    assert first["url"] == "https://www.reddit.com/r/python/comments/abc123/test1/"
    assert first["subreddit"] == "python"
    # Fields the RSS feed cannot provide default to 0.
    assert first["score"] == 0
    assert first["num_comments"] == 0
    # created_utc must be an ISO-8601 string the storage worker can parse.
    assert datetime.fromisoformat(first["created_utc"])
    assert posts[1]["post_id"] == "t3_def456"


def test_fetch_latest_posts_needs_no_auth():
    """The RSS path must carry no OAuth/credential machinery."""
    import worker

    assert not hasattr(worker, "get_access_token")
    with patch("worker.httpx.get", return_value=_rss_response(SAMPLE_ATOM)):
        posts = fetch_latest_posts("python")
    assert all(not p["author"].startswith("/u/") for p in posts)


def test_fetch_latest_posts_empty():
    with patch("worker.httpx.get", return_value=_rss_response(EMPTY_ATOM)):
        with pytest.raises(ValueError, match="No posts found"):
            fetch_latest_posts("nonexistent_sub")


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
async def test_produce_new_posts_survives_fetch_error():
    """A non-HTTP error from fetch (e.g. missing creds → RuntimeError) is
    contained so the poll loop keeps running instead of crashing the worker."""
    mock_producer = AsyncMock()
    streamer = RedditStreamer(
        kafka_bootstrap="localhost:9092",
        producer=mock_producer,
    )
    streamer.subscriptions = {"python"}

    with patch("worker.fetch_latest_posts", side_effect=RuntimeError("creds missing")):
        # Must not propagate.
        await streamer.produce_new_posts("python")

    mock_producer.send_and_wait.assert_not_called()


@pytest.mark.asyncio
async def test_produce_new_posts_survives_producer_error():
    """A Kafka send failure for one subreddit is contained, not fatal."""
    mock_producer = AsyncMock()
    mock_producer.send_and_wait.side_effect = RuntimeError("kafka unavailable")
    streamer = RedditStreamer(
        kafka_bootstrap="localhost:9092",
        producer=mock_producer,
    )
    streamer.subscriptions = {"python"}

    posts = [{"post_id": "t3_abc", "title": "Post A", "subreddit": "python"}]

    with patch("worker.fetch_latest_posts", return_value=posts):
        # Must not propagate.
        await streamer.produce_new_posts("python")


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
