"""CLI entry-point: collect tweets based on configs/x_scraping.yaml."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import yaml

from src.scraping.x_scraper import save_tweets, search_recent_tweets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CFG = _PROJECT_ROOT / "configs" / "x_scraping.yaml"


def _load_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def main(config_path: Path = _DEFAULT_CFG) -> None:
    """Run tweet collection for every query in the config file."""
    cfg = _load_config(config_path)
    defaults = cfg.get("defaults", {})
    max_results = defaults.get("max_results", 500)
    output_dir = Path(defaults.get("output_dir", "data/raw/x"))

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for entry in cfg.get("queries", []):
        label = entry["label"]
        query = entry["query"]
        logger.info("Collecting tweets for [%s]: %s", label, query)

        df = search_recent_tweets(query=query, max_results=max_results)
        if df.empty:
            logger.warning("No tweets found for [%s].", label)
            continue

        out_path = output_dir / f"{label}_{timestamp}.csv"
        save_tweets(df, out_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect tweets from X API.")
    parser.add_argument(
        "--config",
        type=Path,
        default=_DEFAULT_CFG,
        help="Path to YAML config (default: configs/x_scraping.yaml)",
    )
    args = parser.parse_args()
    main(config_path=args.config)
