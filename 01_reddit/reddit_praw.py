"""Fetch the latest post from a given subreddit using PRAW."""

import argparse
import logging
import os
import sys
from datetime import UTC, datetime

import praw

logger = logging.getLogger(__name__)

def fetch_latest_post(subreddit: str) -> dict:
    """Fetch the latest post from a subreddit.

    Args:
        subreddit: Name of the subreddit (without r/ prefix).

    Returns:
        Dictionary with post details.

    Raises:
        ValueError: If no posts are found or credentials are missing.
        praw.exceptions.PRAWException: If the PRAW request fails.
    """
    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    user_agent = os.environ.get("REDDIT_USER_AGENT", "python:reddit-praw-fetcher:v1.0")

    if not client_id or not client_secret:
        raise ValueError("REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET environment variables are required")

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )

    sub = reddit.subreddit(subreddit)
    post = next(sub.new(limit=1), None)

    if post is None:
        raise ValueError(f"No posts found in r/{subreddit}")

    created = datetime.fromtimestamp(post.created_utc, tz=UTC)

    return {
        "title": post.title,
        "author": str(post.author),
        "score": post.score,
        "url": post.url,
        "permalink": f"{post.permalink}",
        "selftext": (post.selftext[:500] if post.selftext else "") or "(no text)",
        "created_utc": created.isoformat(),
        "num_comments": post.num_comments,
        "subreddit": str(post.subreddit),
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Fetch the latest post from a subreddit.")
    parser.add_argument("subreddit", help="Subreddit name (e.g. 'python')")
    args = parser.parse_args()

    try:
        post = fetch_latest_post(args.subreddit)
    except praw.exceptions.PRAWException:
        logger.exception("PRAW error fetching r/%s", args.subreddit)
        sys.exit(1)
    except ValueError:
        logger.exception("No data for r/%s", args.subreddit)
        sys.exit(1)

    print(f"\n  Subreddit: r/{post['subreddit']}")
    print(f"     Title: {post['title']}")
    print(f"    Author: u/{post['author']}")
    print(f"     Score: {post['score']}")
    print(f"  Comments: {post['num_comments']}")
    print(f"   Created: {post['created_utc']}")
    print(f"      Link: {post['permalink']}")
    if post["selftext"] != "(no text)":
        print(f"      Text: {post['selftext']}")


if __name__ == "__main__":
    main()
