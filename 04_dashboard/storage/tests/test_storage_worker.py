"""Tests for the Kafka→TimescaleDB storage worker."""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from asyncpg.exceptions import InvalidParameterValueError

from storage_worker import (
    INSERT_SQL,
    handle_message,
    post_to_row,
    setup_schema,
    store_post,
)

SAMPLE_POST = {
    "post_id": "t3_abc123",
    "title": "Test Post",
    "author": "user1",
    "score": 42,
    "url": "https://example.com/1",
    "permalink": "https://www.reddit.com/r/BMW/comments/abc123/test/",
    "selftext": "Hello world",
    "created_utc": "2023-11-14T22:13:20+00:00",
    "num_comments": 5,
    "subreddit": "BMW",
}


def test_post_to_row_orders_columns_and_parses_timestamp():
    row = post_to_row(SAMPLE_POST)

    # Column order must match INSERT_SQL: post_id, subreddit, title, author,
    # score, url, permalink, selftext, num_comments, created_utc.
    assert row[0] == "t3_abc123"
    assert row[1] == "BMW"
    assert row[2] == "Test Post"
    assert row[3] == "user1"
    assert row[4] == 42
    assert row[8] == 5

    created = row[9]
    assert isinstance(created, datetime)
    assert created == datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)


@pytest.mark.asyncio
async def test_store_post_issues_upsert_with_row_values():
    conn = AsyncMock()

    await store_post(conn, SAMPLE_POST)

    conn.execute.assert_awaited_once()
    args = conn.execute.await_args.args
    assert args[0] == INSERT_SQL
    assert args[1:] == post_to_row(SAMPLE_POST)


@pytest.mark.asyncio
async def test_handle_message_decodes_json_and_stores():
    conn = AsyncMock()
    msg = MagicMock()
    msg.value = json.dumps(SAMPLE_POST).encode()

    await handle_message(conn, msg)

    conn.execute.assert_awaited_once()
    args = conn.execute.await_args.args
    assert args[0] == INSERT_SQL
    assert args[1] == "t3_abc123"


@pytest.mark.asyncio
async def test_setup_schema_creates_hypertable_and_continuous_aggregate():
    conn = AsyncMock()

    await setup_schema(conn)

    executed = " ".join(call.args[0] for call in conn.execute.await_args_list)
    assert "reddit_posts" in executed
    assert "create_hypertable" in executed
    assert "mentions_hourly" in executed
    assert "timescaledb.continuous" in executed


@pytest.mark.asyncio
async def test_setup_schema_tolerates_existing_refresh_policy():
    """A pre-existing (differently-configured) policy must not crash startup."""
    conn = AsyncMock()

    def execute(statement, *_args):
        if "add_continuous_aggregate_policy" in statement:
            raise InvalidParameterValueError(
                "refresh interval overlaps with an existing continuous "
                'aggregate policy on "mentions_hourly"'
            )

    conn.execute.side_effect = execute

    # Must complete without propagating the policy-overlap error.
    await setup_schema(conn)
