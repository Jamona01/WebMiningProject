"""Shared configuration — loads .env and exposes API credentials."""

import logging
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_PATH = _PROJECT_ROOT / "configs" / ".env"

if _ENV_PATH.exists():
    load_dotenv(_ENV_PATH)
    logger.debug("Loaded environment from %s", _ENV_PATH)
else:
    logger.warning(
        "No .env file found at %s — copy configs/.env.example and fill in your keys.",
        _ENV_PATH,
    )
