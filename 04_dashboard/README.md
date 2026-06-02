# Kafka Reddit Streaming

Real-time Reddit post streaming pipeline using Apache Kafka, FastAPI, and WebSockets.

## Architecture

```
Browser ──WebSocket──▶ Nginx ──proxy──▶ FastAPI Backend ◀──consumes── Kafka ◀──produces── Worker ──HTTP──▶ Reddit API
                                              │                         ▲
                                              └──subscribe/unsubscribe──┘
                                                  (reddit-control topic)
```

**Components:**

| Service | Role | Tech |
|---------|------|------|
| **Worker** | Polls Reddit, produces posts to Kafka | Python 3.12, aiokafka, httpx |
| **Backend** | Bridges Kafka → WebSocket clients | FastAPI, aiokafka, uvicorn |
| **Frontend** | Displays live post feed | Vanilla JS, Nginx |
| **Kafka** | Event bus (KRaft mode, no ZooKeeper) | Apache Kafka |

## Data Flow

1. User enters a subreddit name and clicks **Stream**
2. Frontend opens a WebSocket to `/ws/{subreddit}`
3. Backend sends a `subscribe` control message to the `reddit-control` Kafka topic
4. Worker receives the control message, starts polling `/r/{subreddit}/new.json` every 30 seconds
5. New posts are deduplicated and published to the `reddit-posts` topic (keyed by subreddit)
6. Backend consumes from `reddit-posts`, filters by key, and forwards posts over the WebSocket
7. Frontend renders each post with title, author, score, comments, and a link to Reddit
8. On disconnect, backend sends `unsubscribe` — worker stops polling that subreddit

## Kafka Topics

| Topic | Key | Purpose |
|-------|-----|---------|
| `reddit-posts` | subreddit name | Streaming Reddit posts from worker to backend |
| `reddit-control` | subreddit name | Subscribe/unsubscribe coordination |

## Quick Start

```bash
docker compose up --build -d
```

- **Frontend:** http://localhost:8080
- **API health:** http://localhost:8000/api/health

## Running Tests

```bash
# Backend
cd backend && pip install -r requirements.txt pytest pytest-asyncio
pytest tests/ -v

# Worker
cd worker && pip install -r requirements.txt pytest pytest-asyncio
pytest tests/ -v
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `POLL_INTERVAL` | `30` | Seconds between Reddit polls |

## Project Structure

```
03_kafka/
├── backend/
│   ├── app/main.py          # FastAPI app + WebSocket endpoint
│   ├── tests/test_main.py
│   ├── Dockerfile
│   └── requirements.txt
├── worker/
│   ├── worker.py            # Reddit poller + Kafka producer
│   ├── tests/test_worker.py
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── index.html           # Single-page WebSocket client
│   ├── nginx.conf           # Reverse proxy for /ws/ and /api/
│   └── Dockerfile
├── docker-compose.yml
└── docs/                    # Design & implementation docs
```

## Useful Commands

```bash
# View logs
docker compose logs -f worker
docker compose logs -f backend

# List Kafka topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Test WebSocket directly
npx wscat -c ws://localhost:8080/ws/python

# Tear down
docker compose down -v
```

## Design Decisions

- **Kafka over Redis** — persistent message log, decoupled producers/consumers, horizontal scalability
- **KRaft mode** — no ZooKeeper dependency, simpler single-broker setup
- **Async Python** — efficient handling of concurrent WebSocket connections and I/O-bound operations
- **Control topic pattern** — clean separation between data flow and coordination
- **Worker-side deduplication** — in-memory set of seen post IDs (capped at 500 per subreddit)
- **Vanilla JS frontend** — no build step, minimal footprint, native WebSocket support
