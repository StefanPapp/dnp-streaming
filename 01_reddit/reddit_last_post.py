"""Fetch the latest post from a given subreddit using Reddit's public JSON API."""

import argparse
import logging
import sys
from datetime import UTC, datetime

import httpx

logger = logging.getLogger(__name__)

REDDIT_BASE_URL = "https://www.reddit.com"


def fetch_latest_post(subreddit: str) -> dict:
    """Fetch the latest post from a subreddit.

    Args:
        subreddit: Name of the subreddit (without r/ prefix).

    Returns:
        Dictionary with post details.

    Raises:
        httpx.HTTPStatusError: If the request fails.
        ValueError: If no posts are found.
    """
    url = f"{REDDIT_BASE_URL}/r/{subreddit}/new.json?limit=1"
    headers = {"User-Agent": "python:reddit-last-post:v1.0 (simple script)"}

    response = httpx.get(url, headers=headers, follow_redirects=True, timeout=10)
    response.raise_for_status()

    data = response.json()
    children = data.get("data", {}).get("children", [])

    if not children:
        raise ValueError(f"No posts found in r/{subreddit}")

    post = children[0]["data"]
    created = datetime.fromtimestamp(post["created_utc"], tz=UTC)

    return {
        "title": post["title"],
        "author": post["author"],
        "score": post["score"],
        "url": post["url"],
        "permalink": f"{REDDIT_BASE_URL}{post['permalink']}",
        "selftext": post.get("selftext", "")[:500] or "(no text)",
        "created_utc": created.isoformat(),
        "num_comments": post["num_comments"],
        "subreddit": post["subreddit"],
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Fetch the latest post from a subreddit.")
    parser.add_argument("subreddit", help="Subreddit name (e.g. 'python')")
    args = parser.parse_args()

    try:
        post = fetch_latest_post(args.subreddit)
    except httpx.HTTPStatusError as e:
        logger.exception("HTTP error fetching r/%s", args.subreddit)
        sys.exit(1)
    except ValueError as e:
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
