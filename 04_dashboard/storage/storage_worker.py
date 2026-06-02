"""Kafka→TimescaleDB storage worker.

Consumes the ``reddit-posts`` topic and persists each post into a TimescaleDB
hypertable, maintaining an hourly continuous aggregate (``mentions_hourly``)
that backs the Streamlit dashboard. Runs in its own consumer group so its
offset is independent of the WebSocket consumers in the backend.
"""

import asyncio
import json
import logging
import os
from datetime import datetime

import asyncpg
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://reddit:reddit@localhost:5432/reddit"
)
TOPIC_POSTS = "reddit-posts"
CONSUMER_GROUP = "storage-worker"

# Column order is the single source of truth shared by post_to_row + INSERT_SQL.
POST_COLUMNS = (
    "post_id",
    "subreddit",
    "title",
    "author",
    "score",
    "url",
    "permalink",
    "selftext",
    "num_comments",
)

INSERT_SQL = """
    INSERT INTO reddit_posts (
        post_id, subreddit, title, author, score,
        url, permalink, selftext, num_comments, created_utc
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (created_utc, post_id) DO UPDATE
        SET score = EXCLUDED.score,
            num_comments = EXCLUDED.num_comments
"""

SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS reddit_posts (
        post_id      text        NOT NULL,
        subreddit    text        NOT NULL,
        title        text,
        author       text,
        score        integer,
        url          text,
        permalink    text,
        selftext     text,
        num_comments integer,
        created_utc  timestamptz NOT NULL,
        PRIMARY KEY (created_utc, post_id)
    )
    """,
    "SELECT create_hypertable('reddit_posts', 'created_utc', if_not_exists => TRUE)",
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS mentions_hourly
        WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
    SELECT time_bucket('1 hour', created_utc) AS hour,
           subreddit,
           count(*)   AS mention_count,
           avg(score) AS avg_score
    FROM reddit_posts
    GROUP BY hour, subreddit
    WITH NO DATA
    """,
)

# Kept separate from SCHEMA_STATEMENTS: add_continuous_aggregate_policy is only
# idempotent for an *identical* policy, so a pre-existing one is tolerated in
# ensure_refresh_policy() rather than being allowed to crash startup.
ADD_POLICY_SQL = """
    SELECT add_continuous_aggregate_policy('mentions_hourly',
        start_offset      => INTERVAL '3 hours',
        end_offset        => INTERVAL '1 hour',
        schedule_interval => INTERVAL '1 hour',
        if_not_exists     => TRUE)
"""


def post_to_row(post: dict) -> tuple:
    """Map a reddit-posts payload to a positional row for ``INSERT_SQL``.

    Args:
        post: Decoded post payload as produced by the streaming worker.

    Returns:
        A 10-tuple of column values in ``INSERT_SQL`` order, with
        ``created_utc`` parsed from its ISO-8601 string into a
        timezone-aware ``datetime`` for the ``timestamptz`` column.
    """
    values = [post.get(col) for col in POST_COLUMNS]
    values.append(datetime.fromisoformat(post["created_utc"]))
    return tuple(values)


async def store_post(conn, post: dict) -> None:
    """Upsert a single post into the ``reddit_posts`` hypertable."""
    await conn.execute(INSERT_SQL, *post_to_row(post))


async def handle_message(conn, msg) -> None:
    """Decode a Kafka message and persist its post."""
    post = json.loads(msg.value)
    await store_post(conn, post)


async def setup_schema(conn) -> None:
    """Create the hypertable and continuous aggregate if absent (idempotent)."""
    for statement in SCHEMA_STATEMENTS:
        await conn.execute(statement)
    await ensure_refresh_policy(conn)


async def ensure_refresh_policy(conn) -> None:
    """Attach the hourly refresh policy, keeping any pre-existing one.

    ``add_continuous_aggregate_policy(... if_not_exists => TRUE)`` only no-ops
    for an identical policy; a differently-configured policy already on the
    aggregate raises ``InvalidParameterValueError``. Treat that as "already
    provisioned" instead of crashing — the worker must not clobber a policy an
    operator configured by hand.
    """
    try:
        await conn.execute(ADD_POLICY_SQL)
    except (
        asyncpg.exceptions.InvalidParameterValueError,
        asyncpg.exceptions.DuplicateObjectError,
    ) as err:
        logger.info("Keeping existing continuous-aggregate refresh policy (%s)", err)


async def connect_pool(
    database_url: str, retries: int = 10, delay: float = 3.0
) -> asyncpg.Pool:
    """Create an asyncpg pool, retrying while TimescaleDB finishes warming up.

    Raises:
        RuntimeError: If a connection can't be established within ``retries``.
    """
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return await asyncpg.create_pool(database_url, min_size=1, max_size=5)
        except (OSError, asyncpg.PostgresError) as err:
            last_err = err
            logger.warning("DB not ready (attempt %d/%d): %s", attempt, retries, err)
            await asyncio.sleep(delay)
    raise RuntimeError(
        f"Could not connect to TimescaleDB at {database_url}"
    ) from last_err


async def consume(consumer: AIOKafkaConsumer, pool: asyncpg.Pool) -> None:
    """Persist every message from the consumer into the database."""
    async for msg in consumer:
        try:
            async with pool.acquire() as conn:
                await handle_message(conn, msg)
        except (asyncpg.PostgresError, ValueError, KeyError):
            logger.exception("Failed to store message at offset %s", msg.offset)


async def main() -> None:
    """Start the storage worker: bootstrap schema, then drain reddit-posts."""
    logger.info("Storage worker starting (kafka=%s)", KAFKA_BOOTSTRAP)

    pool = await connect_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await setup_schema(conn)
    logger.info("Schema ready (reddit_posts hypertable + mentions_hourly aggregate)")

    consumer = AIOKafkaConsumer(
        TOPIC_POSTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Consuming '%s' as group '%s'", TOPIC_POSTS, CONSUMER_GROUP)

    try:
        await consume(consumer, pool)
    except asyncio.CancelledError:
        logger.info("Storage worker shutting down")
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
