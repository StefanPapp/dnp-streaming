# Kafka Streaming Design

**Date:** 2026-02-17
**Status:** Approved

## Goal

Replace the Redis job-queue architecture with Kafka-based streaming. The worker continuously polls Reddit and publishes posts to a Kafka topic. The backend consumes from Kafka and pushes posts to the frontend via WebSocket in real-time.

## Architecture

```
Frontend ◄──WebSocket──► Backend ◄──Kafka consumer──► Kafka ◄──Kafka producer── Worker
(nginx)     /ws/{sub}    (FastAPI)   reddit-posts     (KRaft)                   (Python)
                                                                                   │
                                                                         polls every 30s
                                                                                   │
                                                                              Reddit API
```

### Services (4 containers)

| Service | Image | Role |
|---------|-------|------|
| kafka | bitnami/kafka:latest | KRaft mode single broker |
| worker | Python 3.12 | Polls Reddit, deduplicates, produces to Kafka |
| backend | Python 3.12 | WebSocket endpoint, Kafka consumer, bridges to frontend |
| frontend | nginx + vanilla JS | WebSocket client, renders live post feed |

Redis is removed entirely.

## Data Flow

1. User opens frontend, types subreddit, clicks "Stream"
2. Frontend opens WebSocket to `ws://backend/ws/{subreddit}`
3. Backend sends control message to Kafka topic `reddit-control`: `{"action": "subscribe", "subreddit": "python"}`
4. Worker consumes control messages, starts polling that subreddit every 30s
5. Worker fetches `/r/{subreddit}/new.json?limit=10`, deduplicates by post ID (in-memory set), publishes new posts to `reddit-posts` with key = subreddit name
6. Backend consumes from `reddit-posts`, filters by subreddit, pushes to connected WebSocket clients
7. Frontend renders posts in a scrolling feed (newest on top)
8. On WebSocket close, backend sends `{"action": "unsubscribe"}`. Worker stops polling when no subscribers remain.

## Kafka Topics

| Topic | Key | Value | Partitions |
|-------|-----|-------|------------|
| reddit-posts | subreddit name | JSON post object | 1 |
| reddit-control | subreddit name | `{"action": "subscribe"\|"unsubscribe", "subreddit": "..."}` | 1 |

## Post Message Schema

```json
{
  "title": "string",
  "author": "string",
  "score": 0,
  "url": "string",
  "permalink": "string",
  "selftext": "string (max 500 chars)",
  "created_utc": "ISO 8601",
  "num_comments": 0,
  "subreddit": "string",
  "post_id": "string"
}
```

## Technology Choices

- **aiokafka**: Async-native Kafka client, fits FastAPI and asyncio worker
- **bitnami/kafka with KRaft**: No ZooKeeper dependency, single container
- **WebSocket**: Real-time bidirectional, native browser support

## Error Handling

- Reddit API errors: Worker logs, skips cycle, retries next interval
- Kafka unavailable: Retry with backoff on startup; aiokafka handles reconnects
- WebSocket disconnect: Backend cleans up, sends unsubscribe control message
- No posts found: Worker skips, logs at DEBUG level

## Testing

- **Worker**: Mock httpx for Reddit, mock aiokafka producer. Test dedup logic, control message handling.
- **Backend**: Mock aiokafka consumer. Test WebSocket endpoint with FastAPI TestClient.
- **Integration**: Docker Compose up, connect WebSocket, verify posts arrive.
