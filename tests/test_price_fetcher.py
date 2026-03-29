"""Tests for src.finance.price_fetcher."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.finance.price_fetcher import (
    RAW_PRICES_DIR,
    fetch_and_save,
    fetch_prices,
    save_prices,
)


@pytest.fixture()
def sample_yf_dataframe() -> pd.DataFrame:
    """Mimics the DataFrame returned by ``yf.download`` for a single ticker."""
    dates = pd.date_range("2024-06-01", periods=5, freq="B", tz="UTC")
    return pd.DataFrame(
        {
            "Open": [150.0, 151.0, 152.0, 153.0, 154.0],
            "High": [155.0, 156.0, 157.0, 158.0, 159.0],
            "Low": [149.0, 150.0, 151.0, 152.0, 153.0],
            "Close": [153.0, 154.0, 155.0, 156.0, 157.0],
            "Adj Close": [153.0, 154.0, 155.0, 156.0, 157.0],
            "Volume": [1_000_000] * 5,
        },
        index=dates,
    )


@pytest.fixture()
def sample_yf_dataframe_with_name(sample_yf_dataframe: pd.DataFrame) -> pd.DataFrame:
    df = sample_yf_dataframe.copy()
    df.index.name = "Date"
    return df


class TestFetchPrices:
    @patch("src.finance.price_fetcher.time.sleep")
    @patch("src.finance.price_fetcher.yf.download")
    def test_returns_tidy_dataframe(
        self,
        mock_download: MagicMock,
        _mock_sleep: MagicMock,
        sample_yf_dataframe_with_name: pd.DataFrame,
    ) -> None:
        mock_download.return_value = sample_yf_dataframe_with_name

        df = fetch_prices(tickers=["AAPL"], start="2024-06-01", end="2024-06-08")

        assert not df.empty
        assert "Date" in df.columns
        assert "Ticker" in df.columns
        assert (df["Ticker"] == "AAPL").all()
        assert set(df.columns) >= {"Open", "High", "Low", "Close", "Volume"}

    @patch("src.finance.price_fetcher.time.sleep")
    @patch("src.finance.price_fetcher.yf.download")
    def test_handles_empty_response(
        self, mock_download: MagicMock, _mock_sleep: MagicMock
    ) -> None:
        mock_download.return_value = pd.DataFrame()

        df = fetch_prices(tickers=["INVALID"], start="2024-06-01", end="2024-06-08")

        assert df.empty

    @patch("src.finance.price_fetcher.time.sleep")
    @patch("src.finance.price_fetcher.yf.download")
    def test_handles_download_exception(
        self, mock_download: MagicMock, _mock_sleep: MagicMock
    ) -> None:
        mock_download.side_effect = Exception("network error")

        df = fetch_prices(tickers=["AAPL"], start="2024-06-01", end="2024-06-08")

        assert df.empty

    @patch("src.finance.price_fetcher.time.sleep")
    @patch("src.finance.price_fetcher.yf.download")
    def test_multiple_tickers(
        self,
        mock_download: MagicMock,
        _mock_sleep: MagicMock,
        sample_yf_dataframe_with_name: pd.DataFrame,
    ) -> None:
        mock_download.return_value = sample_yf_dataframe_with_name

        df = fetch_prices(
            tickers=["AAPL", "MSFT"], start="2024-06-01", end="2024-06-08"
        )

        assert set(df["Ticker"].unique()) == {"AAPL", "MSFT"}
        assert len(df) == 10  # 5 rows × 2 tickers


class TestSavePrices:
    def test_writes_csv_and_meta(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            {
                "Date": ["2024-06-03", "2024-06-04"],
                "Ticker": ["AAPL", "AAPL"],
                "Open": [150.0, 151.0],
                "High": [155.0, 156.0],
                "Low": [149.0, 150.0],
                "Close": [153.0, 154.0],
                "Adj Close": [153.0, 154.0],
                "Volume": [1_000_000, 1_100_000],
            }
        )

        with patch("src.finance.price_fetcher.RAW_PRICES_DIR", tmp_path / "prices"):
            path = save_prices(df, tag="test")

        assert path.exists()
        assert path.suffix == ".csv"

        meta_path = path.with_name(path.stem + "_meta.json")
        assert meta_path.exists()

        reloaded = pd.read_csv(path)
        assert len(reloaded) == 2


class TestFetchAndSave:
    @patch("src.finance.price_fetcher.save_prices")
    @patch("src.finance.price_fetcher.fetch_prices")
    def test_returns_none_when_empty(
        self, mock_fetch: MagicMock, mock_save: MagicMock
    ) -> None:
        mock_fetch.return_value = pd.DataFrame()

        result = fetch_and_save(tickers=["AAPL"])

        assert result is None
        mock_save.assert_not_called()
