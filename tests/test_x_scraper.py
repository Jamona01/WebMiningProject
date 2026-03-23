"""Tests for X (Twitter) scraping modules."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.scraping.x_scraper import save_tweets, search_recent_tweets


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_tweet(
    tweet_id: int = 1,
    text: str = "test tweet",
    author_id: int = 42,
) -> SimpleNamespace:
    """Return an object that mimics a tweepy Tweet."""
    return SimpleNamespace(
        id=tweet_id,
        text=text,
        author_id=author_id,
        created_at=datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc),
        public_metrics={
            "retweet_count": 1,
            "reply_count": 2,
            "like_count": 10,
            "quote_count": 0,
        },
        lang="en",
        source="Twitter Web App",
    )


def _fake_paginator_responses(tweets: list) -> list:
    """Wrap tweets in response-like objects that our code iterates over."""
    response = SimpleNamespace(data=tweets)
    return [response]


# ---------------------------------------------------------------------------
# Tests — search_recent_tweets
# ---------------------------------------------------------------------------

class TestSearchRecentTweets:
    @patch("src.scraping.x_scraper.tweepy.Paginator")
    @patch("src.scraping.x_scraper.get_client")
    def test_returns_dataframe_with_expected_columns(
        self, mock_get_client: MagicMock, mock_paginator: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        tweets = [_make_tweet(tweet_id=i, text=f"tweet {i}") for i in range(3)]
        mock_paginator.return_value = _fake_paginator_responses(tweets)

        df = search_recent_tweets(query="$TSLA", max_results=10)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        expected_cols = {
            "tweet_id", "text", "author_id", "created_at",
            "retweet_count", "reply_count", "like_count",
            "quote_count", "lang", "source",
        }
        assert set(df.columns) == expected_cols

    @patch("src.scraping.x_scraper.tweepy.Paginator")
    @patch("src.scraping.x_scraper.get_client")
    def test_respects_max_results(
        self, mock_get_client: MagicMock, mock_paginator: MagicMock
    ) -> None:
        mock_get_client.return_value = MagicMock()
        tweets = [_make_tweet(tweet_id=i) for i in range(20)]
        mock_paginator.return_value = _fake_paginator_responses(tweets)

        df = search_recent_tweets(query="$AAPL", max_results=5)
        assert len(df) == 5

    @patch("src.scraping.x_scraper.tweepy.Paginator")
    @patch("src.scraping.x_scraper.get_client")
    def test_handles_empty_response(
        self, mock_get_client: MagicMock, mock_paginator: MagicMock
    ) -> None:
        mock_get_client.return_value = MagicMock()
        empty_response = SimpleNamespace(data=None)
        mock_paginator.return_value = [empty_response]

        df = search_recent_tweets(query="nonexistent", max_results=10)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @patch("src.scraping.x_scraper.tweepy.Paginator")
    def test_uses_provided_client(self, mock_paginator: MagicMock) -> None:
        custom_client = MagicMock()
        mock_paginator.return_value = _fake_paginator_responses([_make_tweet()])

        df = search_recent_tweets(query="test", max_results=10, client=custom_client)
        assert len(df) == 1


# ---------------------------------------------------------------------------
# Tests — save_tweets
# ---------------------------------------------------------------------------

class TestSaveTweets:
    def test_saves_csv_and_returns_path(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"tweet_id": [1, 2], "text": ["a", "b"]})
        out = tmp_path / "sub" / "tweets.csv"

        result = save_tweets(df, out)

        assert result.exists()
        loaded = pd.read_csv(result)
        assert len(loaded) == 2
        assert list(loaded.columns) == ["tweet_id", "text"]


# ---------------------------------------------------------------------------
# Tests — x_client.get_client
# ---------------------------------------------------------------------------

class TestGetClient:
    @patch.dict("os.environ", {"X_BEARER_TOKEN": "test-token"})
    @patch("src.scraping.x_client.tweepy.Client")
    def test_returns_client_when_token_set(self, mock_tweepy_client: MagicMock) -> None:
        from src.scraping.x_client import get_client

        client = get_client()
        mock_tweepy_client.assert_called_once_with(
            bearer_token="test-token",
            wait_on_rate_limit=True,
        )

    @patch.dict("os.environ", {}, clear=True)
    def test_raises_when_token_missing(self) -> None:
        from src.scraping.x_client import get_client

        with pytest.raises(EnvironmentError, match="X_BEARER_TOKEN"):
            get_client()
