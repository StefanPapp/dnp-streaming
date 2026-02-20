# Kafka Streaming Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the Redis job-queue architecture with Kafka streaming so the worker continuously publishes Reddit posts to Kafka and the frontend displays them in real-time via WebSocket.

**Architecture:** Worker polls Reddit every 30s, deduplicates posts, produces to Kafka topic `reddit-posts`. Backend consumes from Kafka, bridges to frontend via WebSocket. A `reddit-control` topic coordinates subscribe/unsubscribe between backend and worker. Redis is removed entirely.

**Tech Stack:** aiokafka, FastAPI WebSocket, bitnami/kafka (KRaft), httpx, vanilla JS

---

### Task 1: Update worker requirements and Dockerfile

**Files:**
- Modify: `worker/requirements.txt`
- Modify: `worker/Dockerfile`

**Step 1: Update requirements.txt**

Replace contents of `worker/requirements.txt` with:
```
aiokafka==0.12.0
httpx==0.28.1
```

Remove `redis` dependency, add `aiokafka`.

**Step 2: Update Dockerfile**

No changes needed — Dockerfile already copies `requirements.txt`, installs deps, copies `worker.py`, runs `python worker.py`.

**Step 3: Commit**

```bash
git add worker/requirements.txt
git commit -m "feat(worker): replace redis with aiokafka dependency"
```

---

### Task 2: Write worker tests

**Files:**
- Create: `worker/tests/test_worker.py` (overwrite existing)

**Step 1: Write failing tests for the new worker**

Replace `worker/tests/test_worker.py` with tests for:
- `fetch_latest_posts(subreddit)` returns list of posts with `post_id` field
- `fetch_latest_posts(subreddit)` raises `ValueError` when no posts found
- `fetch_latest_posts(subreddit)` raises on HTTP error
- `RedditStreamer.produce_new_posts()` publishes only unseen posts to Kafka
- `RedditStreamer.produce_new_posts()` deduplicates by `post_id`
- `RedditStreamer.handle_control_message()` adds subreddit on subscribe
- `RedditStreamer.handle_control_message()` removes subreddit on unsubscribe

```python
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

    with patch("worker.httpx.get", return_value=mock_response):
        posts = fetch_latest_posts("python")

    assert len(posts) == 2
    assert posts[0]["title"] == "Test Post 1"
    assert posts[0]["post_id"] == "t3_abc123"
    assert posts[1]["post_id"] == "t3_def456"


def test_fetch_latest_posts_empty():
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"children": []}}
    mock_response.raise_for_status = MagicMock()

    with patch("worker.httpx.get", return_value=mock_response):
        with pytest.raises(ValueError, match="No posts found"):
            fetch_latest_posts("nonexistent_sub")


def test_fetch_latest_posts_includes_post_id():
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_REDDIT_RESPONSE
    mock_response.raise_for_status = MagicMock()

    with patch("worker.httpx.get", return_value=mock_response):
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
```

**Step 2: Run tests to verify they fail**

Run: `cd worker && pip install aiokafka httpx pytest pytest-asyncio && python -m pytest tests/test_worker.py -v`
Expected: FAIL — `RedditStreamer` and `fetch_latest_posts` don't exist yet.

**Step 3: Commit**

```bash
git add worker/tests/test_worker.py
git commit -m "test(worker): add tests for Kafka streaming worker"
```

---

### Task 3: Implement the worker

**Files:**
- Modify: `worker/worker.py` (full rewrite)

**Step 1: Implement `fetch_latest_posts` and `RedditStreamer`**

Replace `worker/worker.py` with:

```python
"""Reddit Kafka streaming worker — polls subreddits, produces to Kafka."""

import asyncio
import json
import logging
import os
from datetime import UTC, datetime

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDDIT_BASE_URL = "https://www.reddit.com"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
TOPIC_POSTS = "reddit-posts"
TOPIC_CONTROL = "reddit-control"


def fetch_latest_posts(subreddit: str) -> list[dict]:
    """Fetch latest posts from a subreddit.

    Args:
        subreddit: Name of the subreddit (without r/ prefix).

    Returns:
        List of post dictionaries with post_id field.

    Raises:
        httpx.HTTPStatusError: If the request fails.
        ValueError: If no posts are found.
    """
    url = f"{REDDIT_BASE_URL}/r/{subreddit}/new.json?limit=10"
    headers = {"User-Agent": "python:reddit-kafka-streamer:v2.0 (streaming worker)"}

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
        self.seen_ids: dict[str, set[str]] = {}  # subreddit -> set of post_ids

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
        """Fetch and publish new posts for a subreddit."""
        try:
            posts = fetch_latest_posts(subreddit)
        except (httpx.HTTPStatusError, ValueError) as e:
            logger.exception("Failed to fetch r/%s", subreddit)
            return

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
```

**Step 2: Run tests to verify they pass**

Run: `cd worker && python -m pytest tests/test_worker.py -v`
Expected: All 7 tests PASS.

**Step 3: Commit**

```bash
git add worker/worker.py
git commit -m "feat(worker): rewrite worker for Kafka streaming with dedup"
```

---

### Task 4: Update backend requirements and remove Redis modules

**Files:**
- Modify: `backend/requirements.txt`
- Delete: `backend/app/redis_client.py`
- Modify: `backend/app/models.py`

**Step 1: Update requirements.txt**

Replace contents of `backend/requirements.txt` with:
```
fastapi==0.115.12
uvicorn[standard]==0.34.2
aiokafka==0.12.0
httpx==0.28.1
pydantic==2.11.1
```

Remove `redis`, add `aiokafka`.

**Step 2: Delete redis_client.py**

Delete `backend/app/redis_client.py` — no longer needed.

**Step 3: Replace models.py**

Replace `backend/app/models.py` with a single model for the WebSocket stream:

```python
"""Pydantic models for the Reddit streaming API."""

from pydantic import BaseModel, field_validator


class StreamRequest(BaseModel):
    """Request to start streaming a subreddit."""

    subreddit: str

    @field_validator("subreddit")
    @classmethod
    def strip_prefix(cls, v: str) -> str:
        """Remove r/ prefix if present."""
        return v.removeprefix("r/")
```

**Step 4: Commit**

```bash
git add backend/requirements.txt backend/app/models.py
git rm backend/app/redis_client.py
git commit -m "feat(backend): replace redis deps with aiokafka, simplify models"
```

---

### Task 5: Write backend tests

**Files:**
- Modify: `backend/tests/test_main.py` (overwrite)
- Delete: `backend/tests/test_redis_client.py`
- Delete: `backend/tests/test_models.py`

**Step 1: Write failing tests for WebSocket endpoint**

Replace `backend/tests/test_main.py` with:

```python
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

    # Simulate one Kafka message then stop
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

    async def mock_consume():
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

    async def mock_consume():
        # Yield nothing, just keep alive briefly
        return
        yield  # make it a generator

    mock_consumer.__aiter__ = mock_consume

    with (
        patch("app.main.create_consumer", return_value=mock_consumer),
        patch("app.main.create_producer", return_value=mock_producer),
    ):
        client = TestClient(app)
        with client.websocket_connect("/ws/python"):
            pass  # Just connect and disconnect

        # Verify subscribe was sent
        mock_producer.send_and_wait.assert_any_call(
            "reddit-control",
            key=b"python",
            value=json.dumps({"action": "subscribe", "subreddit": "python"}).encode(),
        )
```

**Step 2: Delete old test files**

```bash
git rm backend/tests/test_redis_client.py backend/tests/test_models.py
```

**Step 3: Run tests to verify they fail**

Run: `cd backend && pip install aiokafka fastapi uvicorn pydantic httpx pytest pytest-asyncio && python -m pytest tests/test_main.py -v`
Expected: FAIL — `create_consumer`, `create_producer`, WebSocket endpoint don't exist yet.

**Step 4: Commit**

```bash
git add backend/tests/test_main.py
git commit -m "test(backend): add WebSocket and Kafka integration tests"
```

---

### Task 6: Implement the backend

**Files:**
- Modify: `backend/app/main.py` (full rewrite)

**Step 1: Implement FastAPI with WebSocket and Kafka**

Replace `backend/app/main.py` with:

```python
"""FastAPI application for Reddit streaming via Kafka + WebSocket."""

import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

app = FastAPI(title="Reddit Streamer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_POSTS = "reddit-posts"
TOPIC_CONTROL = "reddit-control"


def create_producer() -> AIOKafkaProducer:
    """Create a Kafka producer."""
    return AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)


def create_consumer(subreddit: str) -> AIOKafkaConsumer:
    """Create a Kafka consumer for reddit-posts topic."""
    return AIOKafkaConsumer(
        TOPIC_POSTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=None,  # No group — each WebSocket gets all messages
        auto_offset_reset="latest",
    )


@app.websocket("/ws/{subreddit}")
async def stream_subreddit(websocket: WebSocket, subreddit: str) -> None:
    """Stream Reddit posts for a subreddit via WebSocket."""
    await websocket.accept()
    subreddit = subreddit.removeprefix("r/")

    producer = create_producer()
    consumer = create_consumer(subreddit)

    await producer.start()
    await consumer.start()

    # Send subscribe control message
    await producer.send_and_wait(
        TOPIC_CONTROL,
        key=subreddit.encode(),
        value=json.dumps({"action": "subscribe", "subreddit": subreddit}).encode(),
    )
    logger.info("Client subscribed to r/%s", subreddit)

    try:
        async for msg in consumer:
            if msg.key and msg.key.decode() == subreddit:
                post = json.loads(msg.value)
                await websocket.send_json(post)
    except WebSocketDisconnect:
        logger.info("Client disconnected from r/%s", subreddit)
    finally:
        # Send unsubscribe control message
        try:
            await producer.send_and_wait(
                TOPIC_CONTROL,
                key=subreddit.encode(),
                value=json.dumps({"action": "unsubscribe", "subreddit": subreddit}).encode(),
            )
        except Exception:
            logger.exception("Failed to send unsubscribe for r/%s", subreddit)
        await consumer.stop()
        await producer.stop()


@app.get("/api/health")
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}
```

**Step 2: Run tests to verify they pass**

Run: `cd backend && python -m pytest tests/test_main.py -v`
Expected: All 3 tests PASS.

**Step 3: Commit**

```bash
git add backend/app/main.py
git commit -m "feat(backend): add WebSocket endpoint with Kafka consumer bridge"
```

---

### Task 7: Update frontend

**Files:**
- Modify: `frontend/index.html` (full rewrite)
- Modify: `frontend/nginx.conf` (add WebSocket proxy)

**Step 1: Update nginx.conf to proxy WebSocket**

Replace `frontend/nginx.conf` with:

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

    location /ws/ {
        proxy_pass http://backend:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 86400;
    }
}
```

**Step 2: Rewrite frontend for WebSocket streaming**

Replace `frontend/index.html` with:

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reddit Stream</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: system-ui, sans-serif; max-width: 720px; margin: 2rem auto; padding: 0 1rem; color: #1a1a1a; }
        h1 { margin-bottom: 1.5rem; }
        .input-row { display: flex; gap: 0.5rem; margin-bottom: 1rem; }
        input { flex: 1; padding: 0.5rem 0.75rem; border: 1px solid #ccc; border-radius: 4px; font-size: 1rem; }
        button { padding: 0.5rem 1.25rem; color: white; border: none; border-radius: 4px; font-size: 1rem; cursor: pointer; }
        #start-btn { background: #0066ff; }
        #start-btn:hover { background: #0052cc; }
        #stop-btn { background: #cc0000; display: none; }
        #stop-btn:hover { background: #aa0000; }
        button:disabled { background: #999; cursor: not-allowed; }
        .status { margin-bottom: 1rem; font-size: 0.9rem; color: #666; }
        .status.connected { color: #00aa44; }
        .status.error { color: #cc0000; }
        #feed { display: flex; flex-direction: column; gap: 0.75rem; }
        .post { border: 1px solid #ddd; border-radius: 6px; padding: 1rem; animation: fadeIn 0.3s ease-in; }
        .post h2 { margin-bottom: 0.5rem; font-size: 1.05rem; }
        .post .meta { color: #666; font-size: 0.85rem; margin-bottom: 0.5rem; }
        .post .text { margin-top: 0.75rem; white-space: pre-wrap; font-size: 0.9rem; color: #333; }
        .post a { color: #0066ff; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
    </style>
</head>
<body>
    <h1>Reddit Stream</h1>
    <div class="input-row">
        <input type="text" id="subreddit" placeholder="Subreddit name, e.g. python" />
        <button id="start-btn" onclick="startStream()">Stream</button>
        <button id="stop-btn" onclick="stopStream()">Stop</button>
    </div>
    <div id="status" class="status"></div>
    <div id="feed"></div>

    <script>
        let ws = null;

        function startStream() {
            const sub = document.getElementById('subreddit').value.trim().replace(/^r\//, '');
            if (!sub) return;

            stopStream();

            const statusEl = document.getElementById('status');
            const feed = document.getElementById('feed');
            feed.innerHTML = '';

            const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${location.host}/ws/${sub}`;

            statusEl.textContent = `Connecting to r/${sub}...`;
            statusEl.className = 'status';

            ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                statusEl.textContent = `Connected — streaming r/${sub}`;
                statusEl.className = 'status connected';
                document.getElementById('start-btn').style.display = 'none';
                document.getElementById('stop-btn').style.display = 'inline-block';
                document.getElementById('subreddit').disabled = true;
            };

            ws.onmessage = (event) => {
                const post = JSON.parse(event.data);
                renderPost(post);
            };

            ws.onclose = () => {
                statusEl.textContent = 'Disconnected';
                statusEl.className = 'status';
                resetButtons();
            };

            ws.onerror = () => {
                statusEl.textContent = 'Connection error';
                statusEl.className = 'status error';
                resetButtons();
            };
        }

        function stopStream() {
            if (ws) {
                ws.close();
                ws = null;
            }
            resetButtons();
        }

        function resetButtons() {
            document.getElementById('start-btn').style.display = 'inline-block';
            document.getElementById('stop-btn').style.display = 'none';
            document.getElementById('subreddit').disabled = false;
        }

        function renderPost(post) {
            const feed = document.getElementById('feed');
            const div = document.createElement('div');
            div.className = 'post';
            div.innerHTML = `
                <h2>${escapeHtml(post.title)}</h2>
                <div class="meta">
                    r/${escapeHtml(post.subreddit)} &middot;
                    u/${escapeHtml(post.author)} &middot;
                    ${post.score} points &middot;
                    ${post.num_comments} comments &middot;
                    ${new Date(post.created_utc).toLocaleString()}
                </div>
                <div><a href="${escapeHtml(post.permalink)}" target="_blank">View on Reddit</a></div>
                ${post.selftext && post.selftext !== '(no text)' ? `<div class="text">${escapeHtml(post.selftext)}</div>` : ''}
            `;
            feed.prepend(div);

            // Cap displayed posts at 100
            while (feed.children.length > 100) {
                feed.removeChild(feed.lastChild);
            }
        }

        function escapeHtml(str) {
            const div = document.createElement('div');
            div.textContent = str;
            return div.innerHTML;
        }

        document.getElementById('subreddit').addEventListener('keydown', e => {
            if (e.key === 'Enter') startStream();
        });
    </script>
</body>
</html>
```

**Step 3: Commit**

```bash
git add frontend/index.html frontend/nginx.conf
git commit -m "feat(frontend): replace polling with WebSocket live stream UI"
```

---

### Task 8: Update docker-compose.yml

**Files:**
- Modify: `docker-compose.yml`

**Step 1: Replace Redis with Kafka, update environment variables**

Replace `docker-compose.yml` with:

```yaml
services:
  kafka:
    image: bitnami/kafka:latest
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 10s
      timeout: 5s
      retries: 5

  backend:
    build: ./backend
    ports:
      - "8000:8000"
    environment:
      - KAFKA_BOOTSTRAP=kafka:9092
    depends_on:
      kafka:
        condition: service_healthy

  worker:
    build: ./worker
    environment:
      - KAFKA_BOOTSTRAP=kafka:9092
      - POLL_INTERVAL=30
    depends_on:
      kafka:
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
git commit -m "feat(docker): replace redis with kafka in docker-compose"
```

---

### Task 9: Update backend Dockerfile

**Files:**
- Modify: `backend/Dockerfile`

**Step 1: No changes needed**

The existing Dockerfile already copies requirements.txt, installs deps, copies app/, and runs uvicorn. No modifications required.

**Step 2: Verify by building**

Run: `docker compose build backend`
Expected: Build succeeds.

---

### Task 10: Integration test — bring up the stack

**Step 1: Build and start all services**

Run: `docker compose up --build -d`

**Step 2: Wait for services to be healthy**

Run: `docker compose ps` — verify all 4 services are running.

**Step 3: Test health endpoint**

Run: `curl http://localhost:8000/api/health`
Expected: `{"status":"ok"}`

**Step 4: Test WebSocket with wscat**

Run: `npx wscat -c ws://localhost:8080/ws/python`
Expected: Connection opens. Within ~30s, JSON post messages start appearing.

**Step 5: Verify Kafka topics exist**

Run: `docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list`
Expected: `reddit-posts` and `reddit-control` listed.

**Step 6: Commit final state**

```bash
git add -A
git commit -m "feat: complete kafka streaming integration"
```
