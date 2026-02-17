"""Tests for the FastAPI streaming application."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.testclient import TestClient

from app.main import app


@pytest.mark.asyncio
async def test_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_websocket_receives_posts():
    """WebSocket endpoint sends Kafka messages to client."""
    mock_consumer = AsyncMock()
    mock_producer = AsyncMock()

    post_data = {
        "post_id": "t3_abc",
        "title": "Test Post",
        "author": "user1",
        "subreddit": "python",
        "score": 42,
    }
    mock_msg = MagicMock()
    mock_msg.key = b"python"
    mock_msg.value = json.dumps(post_data).encode()

    async def mock_consume(self):
        yield mock_msg

    mock_consumer.__aiter__ = mock_consume

    with (
        patch("app.main.create_consumer", return_value=mock_consumer),
        patch("app.main.create_producer", return_value=mock_producer),
    ):
        client = TestClient(app)
        with client.websocket_connect("/ws/python") as ws:
            data = ws.receive_json()
            assert data["title"] == "Test Post"
            assert data["post_id"] == "t3_abc"


def test_websocket_sends_subscribe_control():
    """Connecting to WebSocket sends subscribe control message."""
    mock_consumer = AsyncMock()
    mock_producer = AsyncMock()

    async def mock_consume(self):
        return
        yield

    mock_consumer.__aiter__ = mock_consume

    with (
        patch("app.main.create_consumer", return_value=mock_consumer),
        patch("app.main.create_producer", return_value=mock_producer),
    ):
        client = TestClient(app)
        with client.websocket_connect("/ws/python"):
            pass

        mock_producer.send_and_wait.assert_any_call(
            "reddit-control",
            key=b"python",
            value=json.dumps({"action": "subscribe", "subreddit": "python"}).encode(),
        )
