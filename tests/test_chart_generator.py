"""テスト: chart_generator.py — mplfinance ローソク足チャート生成ロジック"""

import sys
import tempfile
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.article.chart_generator import (
    _safe_sma,
    generate_chart_pair,
    generate_all_charts,
    MPLFINANCE_AVAILABLE,
    MATPLOTLIB_AVAILABLE,  # 後方互換エイリアス
)


# ============================================================
# ヘルパー
# ============================================================

def _make_ohlcv_df(n: int = 260, freq: str = "B") -> pd.DataFrame:
    """mplfinance 対応の OHLCV DataFrame を生成する。"""
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=n, freq=freq)
    close = 100 + np.cumsum(np.random.randn(n) * 0.5)
    high = close + np.abs(np.random.randn(n) * 0.3)
    low = close - np.abs(np.random.randn(n) * 0.3)
    open_ = close + np.random.randn(n) * 0.2
    volume = np.random.randint(1_000_000, 10_000_000, n).astype(float)
    return pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume},
        index=dates,
    )


# ============================================================
# _safe_sma（後方互換シム）
# ============================================================

class TestSafeSma:
    def test_basic_sma(self):
        """基本的な SMA 計算。"""
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = _safe_sma(s, 3)
        assert result is not None
        assert abs(result.iloc[-1] - 4.0) < 0.01  # (3+4+5)/3=4

    def test_insufficient_data_returns_none(self):
        """データ不足は None を返す。"""
        s = pd.Series([1.0, 2.0])
        result = _safe_sma(s, 5)
        assert result is None

    def test_exact_length(self):
        """ちょうど length 個のデータ。"""
        s = pd.Series([1.0, 2.0, 3.0])
        result = _safe_sma(s, 3)
        assert result is not None


# ============================================================
# generate_chart_pair
# ============================================================

@pytest.mark.skipif(not MPLFINANCE_AVAILABLE, reason="mplfinance 未インストール")
class TestGenerateChartPair:
    def test_both_charts_created(self):
        """日次・週次の両ファイルが生成される。"""
        df_daily = _make_ohlcv_df(260, "B")
        df_weekly = _make_ohlcv_df(156, "W")
        with tempfile.TemporaryDirectory() as tmpdir:
            chart_dir = Path(tmpdir) / "charts"
            result = generate_chart_pair(
                df_daily=df_daily,
                df_weekly=df_weekly,
                ticker="^GSPC",
                display_name="S&P 500",
                chart_dir=chart_dir,
                safe_ticker="GSPC",
            )
        assert result["daily"] is not None
        assert result["weekly"] is not None
        assert result["daily"].name == "GSPC_daily.png"
        assert result["weekly"].name == "GSPC_weekly.png"

    def test_daily_file_has_content(self):
        """生成された日次 PNG が空でない。"""
        df_daily = _make_ohlcv_df(260, "B")
        with tempfile.TemporaryDirectory() as tmpdir:
            chart_dir = Path(tmpdir) / "charts"
            result = generate_chart_pair(
                df_daily=df_daily, df_weekly=None,
                ticker="TEST", display_name="テスト",
                chart_dir=chart_dir, safe_ticker="TEST",
            )
            assert result["daily"] is not None
            assert result["daily"].stat().st_size > 0

    def test_weekly_file_has_content(self):
        """生成された週次 PNG が空でない。"""
        df_weekly = _make_ohlcv_df(156, "W")
        with tempfile.TemporaryDirectory() as tmpdir:
            chart_dir = Path(tmpdir) / "charts"
            result = generate_chart_pair(
                df_daily=None, df_weekly=df_weekly,
                ticker="TEST", display_name="テスト",
                chart_dir=chart_dir, safe_ticker="TEST",
            )
            assert result["weekly"] is not None
            assert result["weekly"].stat().st_size > 0

    def test_none_df_returns_none(self):
        """両方 None を渡すと daily/weekly ともに None を返す。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = generate_chart_pair(
                df_daily=None, df_weekly=None,
                ticker="TEST", display_name="テスト",
                chart_dir=Path(tmpdir), safe_ticker="TEST",
            )
        assert result["daily"] is None
        assert result["weekly"] is None

    def test_insufficient_daily_data_skipped(self):
        """日次データが MIN_DAILY_ROWS 未満ならスキップ（None を返す）。"""
        df_short = _make_ohlcv_df(10, "B")  # 10行 < 80
        with tempfile.TemporaryDirectory() as tmpdir:
            result = generate_chart_pair(
                df_daily=df_short, df_weekly=None,
                ticker="TEST", display_name="テスト",
                chart_dir=Path(tmpdir), safe_ticker="TEST",
            )
        assert result["daily"] is None

    def test_insufficient_weekly_data_skipped(self):
        """週次データが MIN_WEEKLY_ROWS 未満ならスキップ（None を返す）。"""
        df_short = _make_ohlcv_df(10, "W")  # 10行 < 55
        with tempfile.TemporaryDirectory() as tmpdir:
            result = generate_chart_pair(
                df_daily=None, df_weekly=df_short,
                ticker="TEST", display_name="テスト",
                chart_dir=Path(tmpdir), safe_ticker="TEST",
            )
        assert result["weekly"] is None

    def test_no_volume_chart_generated(self):
        """Volume なしのデータでもチャートが生成される。"""
        df = _make_ohlcv_df(260, "B").drop(columns=["Volume"])
        with tempfile.TemporaryDirectory() as tmpdir:
            chart_dir = Path(tmpdir) / "charts"
            result = generate_chart_pair(
                df_daily=df, df_weekly=None,
                ticker="TEST", display_name="テスト",
                chart_dir=chart_dir, safe_ticker="TEST",
            )
        assert result["daily"] is not None

    def test_japanese_ticker_safe_name(self):
        """日本ティッカーの特殊文字（.）がファイル名に使われない。"""
        df_daily = _make_ohlcv_df(260, "B")
        with tempfile.TemporaryDirectory() as tmpdir:
            chart_dir = Path(tmpdir) / "charts"
            result = generate_chart_pair(
                df_daily=df_daily, df_weekly=None,
                ticker="1306.T", display_name="TOPIX",
                chart_dir=chart_dir, safe_ticker="1306_T",
            )
        assert result["daily"] is not None
        assert result["daily"].name == "1306_T_daily.png"


# ============================================================
# generate_all_charts
# ============================================================

@pytest.mark.skipif(not MPLFINANCE_AVAILABLE, reason="mplfinance 未インストール")
class TestGenerateAllCharts:
    def _make_market_data(self) -> dict:
        """テスト用 market_data（_df なしで OK：chart_data を別途渡す）。"""
        return {
            "us_indices": [
                {"ticker": "^GSPC", "display_name": "S&P 500", "error": False},
                {"ticker": "^IXIC", "display_name": "NASDAQ",  "error": True},  # error
            ],
            "jp_indices": [
                {"ticker": "^N225", "display_name": "日経平均", "error": False},
            ],
        }

    def _make_chart_data(self, n_daily: int = 260, n_weekly: int = 156) -> dict:
        """テスト用 chart_data。"""
        df_d = _make_ohlcv_df(n_daily, "B")
        df_w = _make_ohlcv_df(n_weekly, "W")
        return {
            "^GSPC": {"daily": df_d, "weekly": df_w},
            "^IXIC": {"daily": None, "weekly": None},  # error ticker
            "^N225": {"daily": df_d, "weekly": df_w},
        }

    def test_returns_url_pairs_for_valid_tickers(self):
        """有効なティッカーの URL ペアが返される。"""
        market_data = self._make_market_data()
        chart_data = self._make_chart_data()
        with tempfile.TemporaryDirectory() as tmpdir:
            urls = generate_all_charts(
                market_data, chart_data, date(2026, 4, 7), Path(tmpdir)
            )
        assert "^GSPC" in urls
        assert "^N225" in urls
        # error=True のティッカーは含まれない
        assert "^IXIC" not in urls

    def test_url_structure(self):
        """戻り値が {ticker: {"daily": url, "weekly": url}} 形式。"""
        market_data = self._make_market_data()
        chart_data = self._make_chart_data()
        with tempfile.TemporaryDirectory() as tmpdir:
            urls = generate_all_charts(
                market_data, chart_data, date(2026, 4, 7), Path(tmpdir)
            )
        for ticker, pair in urls.items():
            assert isinstance(pair, dict)
            if "daily" in pair:
                assert pair["daily"].endswith("_daily.png")
            if "weekly" in pair:
                assert pair["weekly"].endswith("_weekly.png")

    def test_url_contains_date(self):
        """URL にターゲット日付が含まれる。"""
        market_data = self._make_market_data()
        chart_data = self._make_chart_data()
        with tempfile.TemporaryDirectory() as tmpdir:
            urls = generate_all_charts(
                market_data, chart_data, date(2026, 4, 7), Path(tmpdir)
            )
        for pair in urls.values():
            for url in pair.values():
                assert "2026-04-07" in url

    def test_url_without_base_path(self):
        """base_url_path 省略時は /charts/... で始まる。"""
        market_data = self._make_market_data()
        chart_data = self._make_chart_data()
        with tempfile.TemporaryDirectory() as tmpdir:
            urls = generate_all_charts(
                market_data, chart_data, date(2026, 4, 7), Path(tmpdir)
            )
        for pair in urls.values():
            for url in pair.values():
                assert url.startswith("/charts/2026-04-07/")

    def test_url_with_base_url_path(self):
        """base_url_path が先頭に付与される（GitHub Pages サブパス対応）。"""
        market_data = self._make_market_data()
        chart_data = self._make_chart_data()
        with tempfile.TemporaryDirectory() as tmpdir:
            urls = generate_all_charts(
                market_data, chart_data, date(2026, 4, 7), Path(tmpdir),
                base_url_path="/market-briefing",
            )
        assert len(urls) > 0
        for pair in urls.values():
            for url in pair.values():
                assert url.startswith("/market-briefing/charts/2026-04-07/")

    def test_empty_chart_data_yields_no_urls(self):
        """chart_data が空の場合はチャート URL が生成されない。"""
        market_data = self._make_market_data()
        chart_data: dict = {}
        with tempfile.TemporaryDirectory() as tmpdir:
            urls = generate_all_charts(
                market_data, chart_data, date(2026, 4, 7), Path(tmpdir)
            )
        assert urls == {}
