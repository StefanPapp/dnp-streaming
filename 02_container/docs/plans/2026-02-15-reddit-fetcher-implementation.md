# Reddit Fetcher Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a 3-container web app (frontend + FastAPI backend + Redis worker) that fetches the latest Reddit post for a given subreddit.

**Architecture:** Frontend (nginx) serves static HTML/JS and proxies `/api/*` to FastAPI backend. Backend enqueues jobs to Redis. Worker consumes jobs via `BLPOP`, fetches from Reddit, stores results in Redis. Frontend polls for completion.

**Tech Stack:** Python 3.14, FastAPI, redis.asyncio, httpx, nginx, Docker Compose

---

### Task 1: Backend — Pydantic Models

**Files:**
- Create: `backend/app/__init__.py`
- Create: `backend/app/models.py`
- Create: `backend/tests/__init__.py`
- Create: `backend/tests/test_models.py`

**Step 1: Write the failing test**

Create `backend/tests/test_models.py`:

```python
"""Tests for Pydantic models."""

from app.models import JobCreate, JobStatus


def test_job_create_valid():
    job = JobCreate(subreddit="python")
    assert job.subreddit == "python"


def test_job_create_strips_prefix():
    job = JobCreate(subreddit="r/python")
    assert job.subreddit == "python"


def test_job_status_pending():
    status = JobStatus(job_id="abc123", status="pending")
    assert status.job_id == "abc123"
    assert status.status == "pending"
    assert status.result is None
    assert status.error is None


def test_job_status_completed():
    result = {"title": "Test", "author": "user1"}
    status = JobStatus(job_id="abc123", status="completed", result=result)
    assert status.result == result


def test_job_status_failed():
    status = JobStatus(job_id="abc123", status="failed", error="Not found")
    assert status.error == "Not found"
```

**Step 2: Run test to verify it fails**

Run: `cd backend && pip install pydantic pytest && python -m pytest tests/test_models.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app'`

**Step 3: Write minimal implementation**

Create `backend/app/__init__.py`:
```python
```

Create `backend/tests/__init__.py`:
```python
```

Create `backend/app/models.py`:
```python
"""Pydantic models for the Reddit fetcher API."""

from typing import Any

from pydantic import BaseModel, field_validator


class JobCreate(BaseModel):
    """Request body for creating a fetch job."""

    subreddit: str

    @field_validator("subreddit")
    @classmethod
    def strip_prefix(cls, v: str) -> str:
        """Remove r/ prefix if present."""
        return v.removeprefix("r/")


class JobStatus(BaseModel):
    """Response model for job status."""

    job_id: str
    status: str
    result: dict[str, Any] | None = None
    error: str | None = None
```

**Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_models.py -v`
Expected: All 5 PASS

**Step 5: Commit**

```bash
git add backend/app/ backend/tests/
git commit -m "feat: add pydantic models for job create and status"
```

---

### Task 2: Backend — Redis Client

**Files:**
- Create: `backend/app/redis_client.py`
- Create: `backend/tests/test_redis_client.py`
- Create: `backend/requirements.txt`

**Step 1: Create requirements.txt**

Create `backend/requirements.txt`:
```
fastapi==0.115.12
uvicorn[standard]==0.34.2
redis==5.3.0
httpx==0.28.1
pydantic==2.11.1
```

**Step 2: Write the failing test**

Create `backend/tests/test_redis_client.py`:
```python
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
```

**Step 3: Run test to verify it fails**

Run: `cd backend && pip install redis pytest-asyncio && python -m pytest tests/test_redis_client.py -v`
Expected: FAIL with `ModuleNotFoundError`

**Step 4: Write minimal implementation**

Create `backend/app/redis_client.py`:
```python
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
```

**Step 5: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_redis_client.py -v`
Expected: All 3 PASS

**Step 6: Commit**

```bash
git add backend/
git commit -m "feat: add redis client for job queue management"
```

---

### Task 3: Backend — FastAPI App

**Files:**
- Create: `backend/app/main.py`
- Create: `backend/tests/test_main.py`

**Step 1: Write the failing test**

Create `backend/tests/test_main.py`:
```python
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
```

**Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_main.py -v`
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write minimal implementation**

Create `backend/app/main.py`:
```python
"""FastAPI application for the Reddit fetcher."""

import logging
import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis

from app.models import JobCreate, JobStatus
from app.redis_client import create_job, get_job_status

logger = logging.getLogger(__name__)

app = FastAPI(title="Reddit Fetcher API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def get_redis() -> Redis:
    """Get a Redis connection."""
    return Redis.from_url(REDIS_URL, decode_responses=True)


@app.post("/api/jobs", response_model=JobStatus, status_code=201)
async def create_fetch_job(body: JobCreate) -> JobStatus:
    """Create a new Reddit fetch job."""
    redis = get_redis()
    try:
        job_id = await create_job(redis, body.subreddit)
        return JobStatus(job_id=job_id, status="pending")
    finally:
        await redis.aclose()


@app.get("/api/jobs/{job_id}", response_model=JobStatus)
async def get_fetch_job(job_id: str) -> JobStatus:
    """Get the status of a fetch job."""
    redis = get_redis()
    try:
        data = await get_job_status(redis, job_id)
        if data is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobStatus(
            job_id=job_id,
            status=data["status"],
            result=data.get("result"),
            error=data.get("error"),
        )
    finally:
        await redis.aclose()


@app.get("/api/health")
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}
```

**Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_main.py -v`
Expected: All 5 PASS

**Step 5: Commit**

```bash
git add backend/
git commit -m "feat: add FastAPI endpoints for job create and status"
```

---

### Task 4: Worker

**Files:**
- Create: `worker/worker.py`
- Create: `worker/requirements.txt`
- Create: `worker/tests/__init__.py`
- Create: `worker/tests/test_worker.py`

**Step 1: Write the failing test**

Create `worker/tests/__init__.py`:
```python
```

Create `worker/tests/test_worker.py`:
```python
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
```

**Step 2: Run test to verify it fails**

Run: `cd worker && pip install httpx redis pytest pytest-asyncio && python -m pytest tests/test_worker.py -v`
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write minimal implementation**

Create `worker/requirements.txt`:
```
redis==5.3.0
httpx==0.28.1
```

Create `worker/worker.py`:
```python
"""Reddit fetcher worker — consumes jobs from Redis queue."""

import asyncio
import json
import logging
import os
import sys
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
```

**Step 4: Run test to verify it passes**

Run: `cd worker && python -m pytest tests/test_worker.py -v`
Expected: All 4 PASS

**Step 5: Commit**

```bash
git add worker/
git commit -m "feat: add redis worker for reddit fetching"
```

---

### Task 5: Frontend — HTML/JS + nginx

**Files:**
- Create: `frontend/index.html`
- Create: `frontend/nginx.conf`

**Step 1: Create the frontend**

Create `frontend/index.html`:
```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reddit Fetcher</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: system-ui, sans-serif; max-width: 640px; margin: 2rem auto; padding: 0 1rem; color: #1a1a1a; }
        h1 { margin-bottom: 1.5rem; }
        .input-row { display: flex; gap: 0.5rem; margin-bottom: 1.5rem; }
        input { flex: 1; padding: 0.5rem 0.75rem; border: 1px solid #ccc; border-radius: 4px; font-size: 1rem; }
        button { padding: 0.5rem 1.25rem; background: #0066ff; color: white; border: none; border-radius: 4px; font-size: 1rem; cursor: pointer; }
        button:hover { background: #0052cc; }
        button:disabled { background: #999; cursor: not-allowed; }
        .result { border: 1px solid #ddd; border-radius: 6px; padding: 1rem; margin-top: 1rem; }
        .result h2 { margin-bottom: 0.5rem; font-size: 1.1rem; }
        .result .meta { color: #666; font-size: 0.85rem; margin-bottom: 0.5rem; }
        .result .text { margin-top: 0.75rem; white-space: pre-wrap; }
        .result a { color: #0066ff; }
        .error { color: #cc0000; margin-top: 1rem; }
        .loading { color: #666; margin-top: 1rem; }
    </style>
</head>
<body>
    <h1>Reddit Fetcher</h1>
    <div class="input-row">
        <input type="text" id="subreddit" placeholder="Subreddit name, e.g. python" />
        <button id="fetch-btn" onclick="fetchPost()">Fetch</button>
    </div>
    <div id="output"></div>

    <script>
        async function fetchPost() {
            const sub = document.getElementById('subreddit').value.trim();
            const output = document.getElementById('output');
            const btn = document.getElementById('fetch-btn');

            if (!sub) return;

            btn.disabled = true;
            output.innerHTML = '<div class="loading">Fetching...</div>';

            try {
                const createResp = await fetch('/api/jobs', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ subreddit: sub })
                });

                if (!createResp.ok) {
                    throw new Error('Failed to create job');
                }

                const { job_id } = await createResp.json();

                // Poll for result
                for (let i = 0; i < 30; i++) {
                    await new Promise(r => setTimeout(r, 500));
                    const statusResp = await fetch(`/api/jobs/${job_id}`);
                    const data = await statusResp.json();

                    if (data.status === 'completed') {
                        renderPost(data.result);
                        return;
                    }
                    if (data.status === 'failed') {
                        output.innerHTML = `<div class="error">Error: ${data.error}</div>`;
                        return;
                    }
                }

                output.innerHTML = '<div class="error">Timeout waiting for result</div>';
            } catch (e) {
                output.innerHTML = `<div class="error">Error: ${e.message}</div>`;
            } finally {
                btn.disabled = false;
            }
        }

        function renderPost(post) {
            document.getElementById('output').innerHTML = `
                <div class="result">
                    <h2>${escapeHtml(post.title)}</h2>
                    <div class="meta">
                        r/${escapeHtml(post.subreddit)} &middot;
                        u/${escapeHtml(post.author)} &middot;
                        ${post.score} points &middot;
                        ${post.num_comments} comments &middot;
                        ${new Date(post.created_utc).toLocaleString()}
                    </div>
                    <div><a href="${escapeHtml(post.permalink)}" target="_blank">View on Reddit</a></div>
                    ${post.selftext !== '(no text)' ? `<div class="text">${escapeHtml(post.selftext)}</div>` : ''}
                </div>
            `;
        }

        function escapeHtml(str) {
            const div = document.createElement('div');
            div.textContent = str;
            return div.innerHTML;
        }

        document.getElementById('subreddit').addEventListener('keydown', e => {
            if (e.key === 'Enter') fetchPost();
        });
    </script>
</body>
</html>
```

**Step 2: Create nginx config**

Create `frontend/nginx.conf`:
```nginx
server {
    listen 80;

    location / {
        root /usr/share/nginx/html;
        index index.html;
    }

    location /api/ {
        proxy_pass http://backend:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

**Step 3: Commit**

```bash
git add frontend/
git commit -m "feat: add frontend with subreddit input and result display"
```

---

### Task 6: Dockerfiles

**Files:**
- Create: `backend/Dockerfile`
- Create: `worker/Dockerfile`
- Create: `frontend/Dockerfile`

**Step 1: Backend Dockerfile**

Create `backend/Dockerfile`:
```dockerfile
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Step 2: Worker Dockerfile**

Create `worker/Dockerfile`:
```dockerfile
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py .

CMD ["python", "worker.py"]
```

**Step 3: Frontend Dockerfile**

Create `frontend/Dockerfile`:
```dockerfile
FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html

EXPOSE 80
```

**Step 4: Commit**

```bash
git add backend/Dockerfile worker/Dockerfile frontend/Dockerfile
git commit -m "feat: add dockerfiles for all three services"
```

---

### Task 7: Docker Compose

**Files:**
- Create: `docker-compose.yml`

**Step 1: Create docker-compose.yml**

Create `docker-compose.yml`:
```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3

  backend:
    build: ./backend
    ports:
      - "8000:8000"
    environment:
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      redis:
        condition: service_healthy

  worker:
    build: ./worker
    environment:
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      redis:
        condition: service_healthy

  frontend:
    build: ./frontend
    ports:
      - "8080:80"
    depends_on:
      - backend
```

**Step 2: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add docker-compose for full stack orchestration"
```

---

### Task 8: Build and Smoke Test

**Step 1: Build all containers**

Run: `docker compose build`
Expected: All 3 images build successfully

**Step 2: Start the stack**

Run: `docker compose up -d`
Expected: All 4 containers running

**Step 3: Verify health**

Run: `curl http://localhost:8000/api/health`
Expected: `{"status":"ok"}`

**Step 4: Test end-to-end via CLI**

Run:
```bash
JOB=$(curl -s -X POST http://localhost:8000/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"subreddit":"python"}')
echo $JOB
JOB_ID=$(echo $JOB | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")
sleep 2
curl -s http://localhost:8000/api/jobs/$JOB_ID | python3 -m json.tool
```
Expected: JSON with `"status": "completed"` and post data

**Step 5: Open frontend**

Open: `http://localhost:8080`
Type "python" in the input, click "Fetch", verify post appears.

**Step 6: Commit**

```bash
git add -A
git commit -m "chore: smoke test passed, phase 1 complete"
```
