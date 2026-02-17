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
