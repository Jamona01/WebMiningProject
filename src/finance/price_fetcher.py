"""Fetch OHLCV stock-price data from Yahoo Finance via yfinance."""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_PRICES_DIR = _PROJECT_ROOT / "data" / "raw" / "prices"

# Default tickers — extend as needed for your analysis
DEFAULT_TICKERS: list[str] = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "TSLA",
    "META",
    "NVDA",
]

# Delay between individual ticker downloads to avoid throttling
REQUEST_DELAY_SECONDS: float = 1.5


def fetch_prices(
    tickers: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    interval: str = "1d",
) -> pd.DataFrame:
    """Download OHLCV data for the given tickers.

    Args:
        tickers: List of Yahoo Finance ticker symbols.
                 Defaults to ``DEFAULT_TICKERS``.
        start: Start date string (``YYYY-MM-DD``). Defaults to ``"2024-01-01"``.
        end: End date string (``YYYY-MM-DD``). Defaults to today (UTC).
        interval: Bar size — ``"1d"``, ``"1h"``, ``"5m"``, etc.

    Returns:
        A tidy ``DataFrame`` with columns
        ``[Date, Ticker, Open, High, Low, Close, Adj Close, Volume]``.
    """
    tickers = tickers or DEFAULT_TICKERS
    start = start or "2024-01-01"
    end = end or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    frames: list[pd.DataFrame] = []

    for ticker in tickers:
        logger.info("Fetching %s  [%s → %s, interval=%s]", ticker, start, end, interval)
        try:
            data: pd.DataFrame = yf.download(
                ticker,
                start=start,
                end=end,
                interval=interval,
                progress=False,
                auto_adjust=False,
            )
        except Exception:
            logger.warning("Failed to download %s — skipping.", ticker, exc_info=True)
            time.sleep(REQUEST_DELAY_SECONDS)
            continue

        if data.empty:
            logger.warning("No data returned for %s.", ticker)
            time.sleep(REQUEST_DELAY_SECONDS)
            continue

        # yfinance may return MultiIndex columns when downloading a single
        # ticker with auto_adjust=False.  Flatten if necessary.
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        data = data.reset_index()
        data["Ticker"] = ticker

        # Normalise the date column to UTC, date-only
        if "Date" in data.columns:
            data["Date"] = pd.to_datetime(data["Date"], utc=True).dt.strftime("%Y-%m-%d")
        elif "Datetime" in data.columns:
            data.rename(columns={"Datetime": "Date"}, inplace=True)
            data["Date"] = pd.to_datetime(data["Date"], utc=True).dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        frames.append(data)
        time.sleep(REQUEST_DELAY_SECONDS)

    if not frames:
        logger.error("No price data fetched for any ticker.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Consistent column order
    desired_cols = ["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    present_cols = [c for c in desired_cols if c in combined.columns]
    combined = combined[present_cols]

    logger.info("Fetched %d rows for %d tickers.", len(combined), len(frames))
    return combined


def save_prices(df: pd.DataFrame, tag: str = "") -> Path:
    """Persist a price ``DataFrame`` to CSV under ``data/raw/prices/``.

    Args:
        df: The DataFrame returned by :func:`fetch_prices`.
        tag: Optional label appended to the filename (e.g. ``"morning"``).

    Returns:
        The path to the written CSV file.
    """
    RAW_PRICES_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = f"prices_{tag}_{timestamp}" if tag else f"prices_{timestamp}"

    csv_path = RAW_PRICES_DIR / f"{stem}.csv"
    meta_path = RAW_PRICES_DIR / f"{stem}_meta.json"

    df.to_csv(csv_path, index=False)

    metadata = {
        "fetched_at": timestamp,
        "tag": tag,
        "tickers": sorted(df["Ticker"].unique().tolist()) if "Ticker" in df.columns else [],
        "rows": len(df),
        "date_min": str(df["Date"].min()) if "Date" in df.columns else None,
        "date_max": str(df["Date"].max()) if "Date" in df.columns else None,
        "yfinance_version": yf.__version__,
    }
    meta_path.write_text(json.dumps(metadata, indent=2))

    logger.info("Saved %d rows → %s", len(df), csv_path)
    return csv_path


def fetch_and_save(
    tickers: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    interval: str = "1d",
    tag: str = "",
) -> Path | None:
    """Convenience wrapper: fetch prices then persist to disk.

    Returns:
        Path to the CSV file, or ``None`` if nothing was fetched.
    """
    df = fetch_prices(tickers=tickers, start=start, end=end, interval=interval)
    if df.empty:
        return None
    return save_prices(df, tag=tag)
