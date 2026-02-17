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
