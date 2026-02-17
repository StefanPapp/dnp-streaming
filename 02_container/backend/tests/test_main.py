"""Tests for the FastAPI application."""

import json
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
def mock_redis():
    return AsyncMock()


@pytest.mark.asyncio
async def test_create_job():
    with patch("app.main.get_redis") as mock_get_redis:
        mock_r = AsyncMock()
        mock_get_redis.return_value = mock_r
        mock_r.set = AsyncMock()
        mock_r.rpush = AsyncMock()

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/api/jobs", json={"subreddit": "python"})

        assert resp.status_code == 201
        data = resp.json()
        assert "job_id" in data
        assert data["status"] == "pending"


@pytest.mark.asyncio
async def test_create_job_strips_prefix():
    with patch("app.main.get_redis") as mock_get_redis:
        mock_r = AsyncMock()
        mock_get_redis.return_value = mock_r
        mock_r.set = AsyncMock()
        mock_r.rpush = AsyncMock()

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/api/jobs", json={"subreddit": "r/python"})

        assert resp.status_code == 201


@pytest.mark.asyncio
async def test_get_job_found():
    with patch("app.main.get_redis") as mock_get_redis:
        mock_r = AsyncMock()
        mock_get_redis.return_value = mock_r
        mock_r.get = AsyncMock(
            return_value=json.dumps(
                {"status": "completed", "subreddit": "python", "result": {"title": "Hi"}}
            )
        )

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/jobs/abc123")

        assert resp.status_code == 200
        assert resp.json()["status"] == "completed"


@pytest.mark.asyncio
async def test_get_job_not_found():
    with patch("app.main.get_redis") as mock_get_redis:
        mock_r = AsyncMock()
        mock_get_redis.return_value = mock_r
        mock_r.get = AsyncMock(return_value=None)

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/jobs/missing")

        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")
    assert resp.status_code == 200
