# Reddit Fetcher — Containerized Web App Design

## Overview

A containerized web app where users enter a subreddit name, click "Fetch", and see the latest post. Three containers + Redis, orchestrated via Docker Compose.

## Architecture

```
Frontend (nginx:8080) --> Backend (FastAPI:8000) --> Redis (6379) <-- Worker (fetcher)
```

- **Frontend**: Static HTML/JS served by nginx. Reverse-proxies `/api/*` to backend.
- **Backend**: FastAPI. Creates jobs, enqueues to Redis, exposes job status.
- **Worker**: Polls Redis via `BLPOP`, fetches from Reddit, stores results.
- **Redis**: `redis:7-alpine`. Job queue + result store.

## Data Flow

1. User types subreddit, clicks "Fetch"
2. Frontend POSTs to `POST /api/jobs` with `{"subreddit": "python"}`
3. Backend creates job ID, enqueues to Redis, returns `{"job_id": "xxx", "status": "pending"}`
4. Worker picks up job, fetches from Reddit, stores result in Redis
5. Frontend polls `GET /api/jobs/{job_id}` until `status: "completed"`
6. Result displayed

## Redis Key Structure

- `job:queue` — list, worker consumes via `BLPOP`
- `job:{id}` — JSON string: `{"status": "pending|completed|failed", "result": {...}, "error": "..."}`
- Job TTL: 5 minutes

## Error Handling

- Invalid subreddit (404) → `status: "failed"`, error message
- Reddit API timeout → failure with timeout message
- Worker down → job stays in queue, picked up on restart
- Frontend shows error in red

## API Endpoints

- `POST /api/jobs` — body: `{"subreddit": "..."}` → `{"job_id": "...", "status": "pending"}`
- `GET /api/jobs/{job_id}` → `{"job_id": "...", "status": "...", "result": {...}, "error": "..."}`

## Project Structure

```
streaming/
├── docker-compose.yml
├── frontend/
│   ├── Dockerfile
│   ├── nginx.conf
│   └── index.html
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       ├── main.py
│       ├── models.py
│       └── redis_client.py
└── worker/
    ├── Dockerfile
    ├── requirements.txt
    └── worker.py
```

## Phase 1 Scope

- Single subreddit input + fetch button
- Display latest post (title, author, score, comments, link, text)
- Loading + error states in UI
