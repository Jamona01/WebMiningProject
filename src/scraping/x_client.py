"""Low-level wrapper around the X (Twitter) API v2 via Tweepy."""

import logging
import os

import tweepy

from src.utils.config import _ENV_PATH  # noqa: F401 – triggers .env loading

logger = logging.getLogger(__name__)


def get_client() -> tweepy.Client:
    """Return an authenticated Tweepy Client using the bearer token.

    Raises:
        EnvironmentError: If X_BEARER_TOKEN is not set.
    """
    bearer_token = os.getenv("X_BEARER_TOKEN")
    if not bearer_token:
        raise EnvironmentError(
            "X_BEARER_TOKEN is not set. "
            "Copy configs/.env.example → configs/.env and add your token."
        )

    client = tweepy.Client(
        bearer_token=bearer_token,
        wait_on_rate_limit=True,
    )
    logger.info("Authenticated X API client created.")
    return client
