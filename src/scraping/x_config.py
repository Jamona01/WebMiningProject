"""Configuration for the X/Twitter crawler.

All tuneable knobs live here so the crawler modules stay clean.
Account credentials come from environment variables (never hardcoded).
"""

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_PATH = _PROJECT_ROOT / "configs" / ".env"

if _ENV_PATH.exists():
    load_dotenv(_ENV_PATH)

# ---------------------------------------------------------------------------
# Search queries — cashtags, hashtags, and known accounts
# ---------------------------------------------------------------------------

QUERIES: list[str] = [
    # ---------------------------------------------------------------------------
    # Apple (AAPL)
    # ---------------------------------------------------------------------------
    "$AAPL lang:en",
    "#Apple lang:en",
    "#AAPL lang:en",
    "#AppleStock lang:en",
    "#iPhone lang:en",
    "#AppleEarnings lang:en",

    # ---------------------------------------------------------------------------
    # Microsoft (MSFT)
    # ---------------------------------------------------------------------------
    "$MSFT lang:en",
    "#Microsoft lang:en",
    "#MSFT lang:en",
    "#MicrosoftStock lang:en",
    "#Azure lang:en",
    "#MicrosoftEarnings lang:en",

    # ---------------------------------------------------------------------------
    # Alphabet / Google (GOOGL)
    # ---------------------------------------------------------------------------
    "$GOOGL lang:en",
    "#Google lang:en",
    "#Alphabet lang:en",
    "#GOOGL lang:en",
    "#GoogleStock lang:en",
    "#GoogleEarnings lang:en",

    # ---------------------------------------------------------------------------
    # Amazon (AMZN)
    # ---------------------------------------------------------------------------
    "$AMZN lang:en",
    "#Amazon lang:en",
    "#AMZN lang:en",
    "#AmazonStock lang:en",
    "#AWS lang:en",
    "#AmazonEarnings lang:en",

    # ---------------------------------------------------------------------------
    # Tesla (TSLA)
    # ---------------------------------------------------------------------------
    "$TSLA lang:en",
    "#Tesla lang:en",
    "#TSLA lang:en",
    "#TeslaStock lang:en",
    "#ElonMusk lang:en",
    "#TeslaEarnings lang:en",

    # ---------------------------------------------------------------------------
    # Meta (META)
    # ---------------------------------------------------------------------------
    "$META lang:en",
    "#Meta lang:en",
    "#Facebook lang:en",
    "#META lang:en",
    "#MetaStock lang:en",
    "#MetaEarnings lang:en",

    # ---------------------------------------------------------------------------
    # NVIDIA (NVDA)
    # ---------------------------------------------------------------------------
    "$NVDA lang:en",
    "#NVIDIA lang:en",
    "#NVDA lang:en",
    "#NvidiaStock lang:en",
    "#AI lang:en",
    "#NvidiaEarnings lang:en",

    # ---------------------------------------------------------------------------
    # Übergreifende Tech-Themen / Broad tech sentiment
    # ---------------------------------------------------------------------------
    "#BigTech lang:en",
    "#TechStocks lang:en",
    "#techearnings lang:en",
    "#MAG7 lang:en",
    "#stockmarket lang:en",
]

# Minimum follower count to keep a tweet (filters out low-influence noise)
MIN_FOLLOWERS: int = 100
# Minimum likes a tweet must have to be stored (0 = no filter)
MIN_LIKES: int = 100

# Path to a custom Chromium executable — required on ARM (e.g. Raspberry Pi).
# Leave empty to use Playwright's bundled Chromium (x86/x64 only).
# Example: /usr/bin/chromium-browser
CHROMIUM_EXECUTABLE: str = os.environ.get("CHROMIUM_EXECUTABLE", "")
# ---------------------------------------------------------------------------
# Polling parameters
# ---------------------------------------------------------------------------

POLL_INTERVAL_MINUTES: int = 15       # Re-crawl interval per tweet
POLL_DURATION_MINUTES: int = 120      # Total tracking window per tweet

# ---------------------------------------------------------------------------
# Price window around each tweet
# ---------------------------------------------------------------------------

PRICE_OFFSET_BEFORE_MINUTES: int = 30
PRICE_OFFSET_AFTER_MINUTES: int = 120

# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

REQUEST_DELAY_MIN: float = 1.5        # Seconds between API calls (lower bound)
REQUEST_DELAY_MAX: float = 3.0        # Seconds between API calls (upper bound)

# Maximum tweets to fetch per query in one discovery pass
DISCOVERY_LIMIT: int = 500

# ---------------------------------------------------------------------------
# twscrape account pool
# ---------------------------------------------------------------------------

def get_twscrape_accounts() -> list[dict[str, str]]:
    """Load twscrape account credentials from the TWSCRAPE_ACCOUNTS env var.

    Expected format (JSON array — ``cookies`` is optional but recommended):
        [{"username": "u", "password": "p", "email": "e",
          "email_password": "ep", "cookies": "ct0=…;auth_token=…"}]

    When ``cookies`` contains a valid ``ct0`` token, twscrape marks the
    account as active immediately and skips the login / email-verification
    flow — so ``password`` and ``email_password`` can be dummy values.

    Returns an empty list if the variable is not set.
    """
    raw = os.environ.get("TWSCRAPE_ACCOUNTS", "")
    if not raw:
        logger.warning(
            "TWSCRAPE_ACCOUNTS env var not set. "
            "Add your account(s) as a JSON array in configs/.env"
        )
        return []
    try:
        accounts = json.loads(raw)
        logger.info("Loaded %d twscrape account(s) from env.", len(accounts))
        return accounts
    except json.JSONDecodeError:
        logger.error("TWSCRAPE_ACCOUNTS is not valid JSON — check configs/.env")
        return []
