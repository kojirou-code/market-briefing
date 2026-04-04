"""テスト: chart_generator.py のチャート生成ロジック"""

import sys
import tempfile
from datetime import date
from pathlib import Path

import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.article.chart_generator import (
    _safe_sma,
    generate_chart,
    generate_all_charts,
    MATPLOTLIB_AVAILABLE,
)


class TestSafeSma:
    def test_basic_sma(self):
        """基本的なSMA計算。"""
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = _safe_sma(s, 3)
        assert result is not None
        assert abs(result.iloc[-1] - 4.0) < 0.01  # (3+4+5)/3=4

    def test_insufficient_data_returns_none(self):
        """データ不足はNoneを返す。"""
        s = pd.Series([1.0, 2.0])
        result = _safe_sma(s, 5)
        assert result is None

    def test_exact_length(self):
        """ちょうどlength個のデータ。"""
        s = pd.Series([1.0, 2.0, 3.0])
        result = _safe_sma(s, 3)
        assert result is not None


@pytest.mark.skipif(not MATPLOTLIB_AVAILABLE, reason="matplotlib未インストール")
class TestGenerateChart:
    def _make_df(self, n=80):
        """テスト用OHLCVデータを生成する。"""
        dates = pd.date_range("2025-10-01", periods=n, freq="B")
        close = 100 + np.cumsum(np.random.randn(n) * 0.5)
        volume = np.random.randint(1_000_000, 10_000_000, n).astype(float)
        return pd.DataFrame({"Close": close, "Volume": volume}, index=dates)

    def test_chart_created(self):
        """チャートファイルが生成される。"""
        df = self._make_df()
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "test_chart.png"
            result = generate_chart(df, "TEST", "テスト指数", out)
            assert result is True
            assert out.exists()
            assert out.stat().st_size > 0

    def test_insufficient_data_returns_false(self):
        """データ不足でFalseを返す。"""
        df = self._make_df(n=5)
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "short.png"
            result = generate_chart(df, "TEST", "テスト", out)
            assert result is False

    def test_no_volume_chart(self):
        """出来高なしでも正常に生成される。"""
        df = self._make_df()
        df = df.drop(columns=["Volume"])
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "no_vol.png"
            result = generate_chart(df, "TEST", "テスト", out)
            assert result is True


@pytest.mark.skipif(not MATPLOTLIB_AVAILABLE, reason="matplotlib未インストール")
class TestGenerateAllCharts:
    def _make_market_data(self, n=80):
        dates = pd.date_range("2025-10-01", periods=n, freq="B")
        close = 100 + np.cumsum(np.random.randn(n) * 0.5)
        volume = np.random.randint(1_000_000, 10_000_000, n).astype(float)
        df = pd.DataFrame({"Close": close, "Volume": volume}, index=dates)
        return {
            "us_indices": [
                {"ticker": "^GSPC", "display_name": "S&P 500", "error": False, "_df": df},
                {"ticker": "^IXIC", "display_name": "NASDAQ", "error": True, "_df": None},
            ],
            "jp_indices": [
                {"ticker": "^N225", "display_name": "日経平均", "error": False, "_df": df},
            ],
        }

    def test_returns_urls_for_valid_tickers(self):
        """有効なティッカーのURLが返される。"""
        market_data = self._make_market_data()
        with tempfile.TemporaryDirectory() as tmpdir:
            static_dir = Path(tmpdir)
            urls = generate_all_charts(market_data, date(2026, 4, 7), static_dir)

        assert "^GSPC" in urls
        assert "^N225" in urls
        # error=True のティッカーは含まれない
        assert "^IXIC" not in urls

    def test_url_format(self):
        """URLが /charts/YYYY-MM-DD/TICKER.png 形式（base_url_path省略時）。"""
        market_data = self._make_market_data()
        with tempfile.TemporaryDirectory() as tmpdir:
            urls = generate_all_charts(market_data, date(2026, 4, 7), Path(tmpdir))

        for url in urls.values():
            assert url.startswith("/charts/2026-04-07/")
            assert url.endswith(".png")

    def test_url_with_base_url_path(self):
        """base_url_path が先頭に付与される（GitHub Pages サブパス対応）。"""
        market_data = self._make_market_data()
        with tempfile.TemporaryDirectory() as tmpdir:
            urls = generate_all_charts(
                market_data, date(2026, 4, 7), Path(tmpdir),
                base_url_path="/market-briefing",
            )

        assert len(urls) > 0
        for url in urls.values():
            assert url.startswith("/market-briefing/charts/2026-04-07/")
            assert url.endswith(".png")
