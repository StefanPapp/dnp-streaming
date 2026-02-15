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
