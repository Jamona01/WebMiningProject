"""Collect tweets from X (Twitter) API v2 and return structured DataFrames."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import tweepy

from src.scraping.x_client import get_client

logger = logging.getLogger(__name__)

# Fields we request from the API
TWEET_FIELDS = [
    "id",
    "text",
    "author_id",
    "created_at",
    "public_metrics",
    "lang",
    "source",
]


def search_recent_tweets(
    query: str,
    max_results: int = 100,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    client: Optional[tweepy.Client] = None,
) -> pd.DataFrame:
    """Search recent tweets (last 7 days) matching *query*.

    Args:
        query: X search query (supports operators like ``$TSLA`` or ``#AAPL``).
        max_results: Number of tweets to collect (10–100 per page, paginated automatically).
        start_time: Earliest UTC timestamp (inclusive).
        end_time: Latest UTC timestamp (exclusive).
        client: Optional pre-built Tweepy client; created automatically if *None*.

    Returns:
        DataFrame with columns: tweet_id, text, author_id, created_at,
        retweet_count, reply_count, like_count, quote_count, lang, source.
    """
    if client is None:
        client = get_client()

    all_tweets: list[dict] = []
    paginator = tweepy.Paginator(
        client.search_recent_tweets,
        query=query,
        tweet_fields=TWEET_FIELDS,
        max_results=min(max_results, 100),
        start_time=start_time,
        end_time=end_time,
    )

    collected = 0
    for response in paginator:
        if response.data is None:
            break
        for tweet in response.data:
            metrics = tweet.public_metrics or {}
            all_tweets.append(
                {
                    "tweet_id": tweet.id,
                    "text": tweet.text,
                    "author_id": tweet.author_id,
                    "created_at": tweet.created_at,
                    "retweet_count": metrics.get("retweet_count", 0),
                    "reply_count": metrics.get("reply_count", 0),
                    "like_count": metrics.get("like_count", 0),
                    "quote_count": metrics.get("quote_count", 0),
                    "lang": tweet.lang,
                    "source": tweet.source,
                }
            )
            collected += 1
            if collected >= max_results:
                break
        if collected >= max_results:
            break

    logger.info("Collected %d tweets for query '%s'.", len(all_tweets), query)
    return pd.DataFrame(all_tweets)


def save_tweets(df: pd.DataFrame, path: Path | str) -> Path:
    """Persist a tweet DataFrame to CSV.

    Args:
        df: Tweet DataFrame (as returned by :func:`search_recent_tweets`).
        path: Destination file path. Parent directories are created automatically.

    Returns:
        Resolved *Path* to the written file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("Saved %d tweets → %s", len(df), path)
    return path.resolve()
