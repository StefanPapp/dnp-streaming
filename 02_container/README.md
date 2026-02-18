# 02_container — Reddit Fetcher (Job Queue Pattern)

A containerized Reddit fetcher using the async job queue pattern with four services.

## Architecture

```
┌─────────┐       ┌──────────────┐       ┌───────────┐       ┌──────────────┐
│ Browser │──────▶│   Frontend   │──────▶│  Backend   │──────▶│    Redis     │
│         │       │  (nginx:80)  │       │(FastAPI:8000)      │  (6379)      │
└─────────┘       └──────────────┘       └───────────┘       └──────┬───────┘
                                                                     │
                                                              ┌──────┴───────┐
                                                              │    Worker    │
                                                              │  (Python)   │
                                                              └──────┬───────┘
                                                                     │
                                                              ┌──────┴───────┐
                                                              │  Reddit API  │
                                                              └──────────────┘
```

## Request Flow

```
1. User enters subreddit name, clicks "Fetch"
   Browser ──POST /api/jobs {subreddit}──▶ Nginx ──proxy──▶ Backend

2. Backend creates job in Redis
   Backend ──SET job:{id} {status:pending}──▶ Redis
   Backend ──RPUSH job:queue {id}──────────▶ Redis
   Backend ◀──returns {job_id}──────────────

3. Worker picks up job from queue
   Worker ──BLPOP job:queue──▶ Redis  (blocking pop, waits for jobs)
   Worker ──GET job:{id}─────▶ Redis  (reads subreddit name)

4. Worker fetches from Reddit
   Worker ──GET /r/{sub}/new.json?limit=1──▶ Reddit API
   Worker ◀──JSON response─────────────────

5. Worker writes result back to Redis
   Worker ──SET job:{id} {status:completed, result:...}──▶ Redis

6. Frontend polls until result is ready
   Browser ──GET /api/jobs/{id}──▶ Nginx ──proxy──▶ Backend ──GET job:{id}──▶ Redis
   (repeats every 500ms, up to 30 times = 15s timeout)

7. Frontend renders the post
```

## Services

| Service    | Image             | Port | Role                                         |
|------------|-------------------|------|----------------------------------------------|
| **frontend** | nginx:1.27-alpine | 8080 | Serves HTML, proxies `/api/` to backend     |
| **backend**  | python:3.12-slim  | 8000 | FastAPI API — creates jobs, returns status   |
| **worker**   | python:3.12-slim  | —    | Consumes job queue, fetches from Reddit API  |
| **redis**    | redis:7-alpine    | 6379 | Job storage + message queue (`job:queue` list) |

## How the Worker Connects

Backend and worker have **no direct communication**. They are decoupled through Redis:

- **Backend** pushes job IDs to a Redis list: `RPUSH job:queue <id>`
- **Worker** blocks on that list: `BLPOP job:queue` (pops the next job ID)
- Both read/write job data via `SET/GET job:{id}`

## Running

```bash
docker compose up --build
```

Open http://localhost:8080, enter a subreddit name, and click Fetch.

## Project Structure

```
02_container/
├── docker-compose.yml
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py          # FastAPI endpoints
│       ├── models.py         # Pydantic schemas
│       └── redis_client.py   # Job create/status helpers
├── worker/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── worker.py             # Job consumer + Reddit fetcher
└── frontend/
    ├── Dockerfile
    ├── nginx.conf            # Reverse proxy config
    └── index.html            # UI + polling logic
```
