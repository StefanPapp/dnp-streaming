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
