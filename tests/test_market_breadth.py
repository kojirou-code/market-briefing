"""テスト: market_breadth.py の Fear & Greed スコアロジック"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.collectors.market_breadth import (
    _vix_to_score,
    _momentum_to_score,
    _rsi_to_score,
    _macd_to_score,
    _highlow_to_score,
    _score_to_label,
    fetch_market_breadth,
)


class TestVixToScore:
    def test_very_low_vix_greed(self):
        """VIX 12以下 → Extreme Greed相当スコア。"""
        assert _vix_to_score(10) == 90.0
        assert _vix_to_score(12) == 90.0

    def test_low_vix_greed(self):
        """VIX 15以下 → Greedスコア。"""
        assert _vix_to_score(13) == 75.0
        assert _vix_to_score(15) == 75.0

    def test_moderate_vix_neutral(self):
        """VIX 18-20 → Neutralスコア。"""
        score = _vix_to_score(18)
        assert 40 <= score <= 60

    def test_high_vix_fear(self):
        """VIX 30超 → Fearスコア。"""
        score = _vix_to_score(35)
        assert score <= 20

    def test_extreme_vix(self):
        """VIX 40超 → Extreme Fearスコア。"""
        assert _vix_to_score(50) <= 10

    def test_none(self):
        """NoneはNoneを返す。"""
        assert _vix_to_score(None) is None


class TestMomentumToScore:
    def test_strongly_above_sma(self):
        """価格がSMA125を10%上回る → Greed。"""
        score = _momentum_to_score(110, 100)
        assert score >= 80

    def test_slightly_above_sma(self):
        """価格がSMA125をわずかに上回る → Neutral寄り。"""
        score = _momentum_to_score(101, 100)
        assert 45 <= score <= 60

    def test_below_sma(self):
        """価格がSMA125を下回る → Fear。"""
        score = _momentum_to_score(90, 100)
        assert score <= 40

    def test_none_inputs(self):
        """NoneはNoneを返す。"""
        assert _momentum_to_score(None, 100) is None
        assert _momentum_to_score(100, None) is None
        assert _momentum_to_score(100, 0) is None


class TestRsiToScore:
    def test_overbought(self):
        """RSI 70+ → Greedスコア。"""
        assert _rsi_to_score(75) >= 75

    def test_oversold(self):
        """RSI 30- → Fearスコア。"""
        assert _rsi_to_score(25) <= 20

    def test_neutral(self):
        """RSI 50 → Neutralスコア。"""
        score = _rsi_to_score(50)
        assert 45 <= score <= 60

    def test_none(self):
        assert _rsi_to_score(None) is None


class TestHighlowToScore:
    def test_near_52w_high(self):
        """52週高値圏 → Greed。"""
        score = _highlow_to_score(98, 100, 50)
        assert score >= 80

    def test_near_52w_low(self):
        """52週安値圏 → Fear。"""
        score = _highlow_to_score(52, 100, 50)
        assert score <= 20

    def test_midrange(self):
        """中間値 → 50前後。"""
        score = _highlow_to_score(75, 100, 50)
        assert abs(score - 50) <= 5

    def test_zero_range(self):
        """高値=安値の場合は50を返す。"""
        assert _highlow_to_score(100, 100, 100) == 50.0

    def test_none(self):
        assert _highlow_to_score(None, 100, 50) is None


class TestScoreToLabel:
    def test_extreme_fear(self):
        label, emoji = _score_to_label(10)
        assert label == "Extreme Fear"
        assert emoji == "🔴"

    def test_fear(self):
        label, emoji = _score_to_label(35)
        assert label == "Fear"
        assert emoji == "🟠"

    def test_neutral(self):
        label, emoji = _score_to_label(50)
        assert label == "Neutral"
        assert emoji == "🟡"

    def test_greed(self):
        label, emoji = _score_to_label(65)
        assert label == "Greed"
        assert emoji == "🟢"

    def test_extreme_greed(self):
        label, emoji = _score_to_label(85)
        assert label == "Extreme Greed"
        assert emoji == "💚"


class TestFetchMarketBreadth:
    """fetch_market_breadth の統合テスト（モックデータ使用）。"""

    def _make_market_data(self, df=None):
        return {
            "us_indices": [
                {
                    "ticker": "^GSPC",
                    "name": "S&P 500",
                    "error": False,
                    "_df": df,
                }
            ],
            "jp_indices": [],
        }

    def _make_indicators_data(self, vix=18.0):
        return {
            "indicators": [{"name": "VIX", "value": vix, "error": False}],
            "indicators_by_name": {"VIX": {"name": "VIX", "value": vix, "error": False}},
        }

    def test_returns_fear_greed_structure(self):
        """戻り値に fear_greed キーが含まれる。"""
        market = self._make_market_data()
        indicators = self._make_indicators_data(vix=18.0)
        result = fetch_market_breadth(market, indicators)

        assert "fear_greed" in result
        assert "error" in result

    def test_vix_only_score_computed(self):
        """S&P 500データなしでもVIXだけでスコアを返す。"""
        market = self._make_market_data(df=None)
        indicators = self._make_indicators_data(vix=15.0)
        result = fetch_market_breadth(market, indicators)

        fg = result["fear_greed"]
        assert fg["score"] is not None
        # VIX=15 → Greed相当スコア
        assert fg["score"] >= 70

    def test_high_vix_fear_score(self):
        """VIX=35 → Fear相当スコア。"""
        market = self._make_market_data(df=None)
        indicators = self._make_indicators_data(vix=35.0)
        result = fetch_market_breadth(market, indicators)

        fg = result["fear_greed"]
        assert fg["score"] is not None
        assert fg["score"] <= 25
