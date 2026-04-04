"""テスト: direction_scorer.py の方向性推定ロジック"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.analyzers.direction_scorer import (
    _signal_to_score,
    _fear_greed_to_score,
    _score_to_label,
    _score_technical,
    calculate_direction_score,
)


class TestSignalToScore:
    def test_green_positive(self):
        assert _signal_to_score("🟢") == 2
        assert _signal_to_score("🟢", positive=1) == 1

    def test_red_negative(self):
        assert _signal_to_score("🔴") == -2
        assert _signal_to_score("🔴", negative=-1) == -1

    def test_yellow_zero(self):
        assert _signal_to_score("🟡") == 0

    def test_white_zero(self):
        assert _signal_to_score("⚪") == 0


class TestFearGreedToScore:
    def test_extreme_greed(self):
        assert _fear_greed_to_score(80) == 4

    def test_greed(self):
        assert _fear_greed_to_score(60) == 2

    def test_neutral(self):
        assert _fear_greed_to_score(50) == 0

    def test_fear(self):
        assert _fear_greed_to_score(35) == -2

    def test_extreme_fear(self):
        assert _fear_greed_to_score(10) == -4

    def test_none(self):
        assert _fear_greed_to_score(None) == 0


class TestScoreToLabel:
    def test_strong_bull(self):
        label, emoji = _score_to_label(8)
        assert label == "強気"
        assert "🟢" in emoji

    def test_mild_bull(self):
        label, emoji = _score_to_label(4)
        assert label == "やや強気"
        assert emoji == "🟢"

    def test_neutral(self):
        label, emoji = _score_to_label(0)
        assert label == "中立"
        assert emoji == "🟡"

    def test_mild_bear(self):
        label, emoji = _score_to_label(-4)
        assert label == "やや弱気"
        assert "🔴" in emoji

    def test_strong_bear(self):
        label, emoji = _score_to_label(-8)
        assert label == "弱気"
        assert "🔴" in emoji


class TestScoreTechnical:
    def test_all_green_max_score(self):
        """全信号🟢 → 最大スコア (+6)。"""
        tech = {
            "name": "S&P 500",
            "error": False,
            "sma": {"signal": "🟢"},
            "rsi": {"signal": "🟢"},
            "macd": {"signal": "🟢"},
            "bb": {"signal": "🟢"},
        }
        result = _score_technical(tech)
        assert result["total"] == 6

    def test_all_red_min_score(self):
        """全信号🔴 → 最小スコア (-6)。"""
        tech = {
            "name": "S&P 500",
            "error": False,
            "sma": {"signal": "🔴"},
            "rsi": {"signal": "🔴"},
            "macd": {"signal": "🔴"},
            "bb": {"signal": "🔴"},
        }
        result = _score_technical(tech)
        assert result["total"] == -6

    def test_error_returns_zero(self):
        """error=True → スコア全0。"""
        result = _score_technical({"name": "X", "error": True})
        assert result["total"] == 0

    def test_mixed_signals(self):
        """混在シグナル → 合計が正しい。"""
        tech = {
            "name": "X",
            "error": False,
            "sma": {"signal": "🟢"},   # +2
            "rsi": {"signal": "🔴"},   # -1
            "macd": {"signal": "🟡"},  # 0
            "bb": {"signal": "🟢"},    # +1
        }
        result = _score_technical(tech)
        assert result["total"] == 2


class TestCalculateDirectionScore:
    def _make_technical_data(self, us_signal="🟢", jp_signal="🟡"):
        def make_tech(name, sig):
            return {
                "name": name,
                "error": False,
                "sma": {"signal": sig},
                "rsi": {"signal": sig},
                "macd": {"signal": sig},
                "bb": {"signal": sig},
            }
        return {
            "us_technical": [make_tech("S&P 500", us_signal)],
            "jp_technical": [make_tech("日経平均", jp_signal)],
        }

    def _make_breadth_data(self, fg_score=50):
        return {
            "fear_greed": {"score": fg_score, "label": "Neutral", "error": False}
        }

    def test_bullish_scenario(self):
        """全🟢 + Extreme Greed → 強気判定。"""
        tech = self._make_technical_data("🟢", "🟢")
        breadth = self._make_breadth_data(fg_score=80)
        result = calculate_direction_score(tech, breadth)

        assert result["us"]["score"] > 0
        assert result["us"]["label"] in ["強気", "やや強気"]

    def test_bearish_scenario(self):
        """全🔴 + Extreme Fear → 弱気判定。"""
        tech = self._make_technical_data("🔴", "🔴")
        breadth = self._make_breadth_data(fg_score=10)
        result = calculate_direction_score(tech, breadth)

        assert result["us"]["score"] < 0
        assert result["us"]["label"] in ["弱気", "やや弱気"]

    def test_result_structure(self):
        """戻り値の構造が正しい。"""
        result = calculate_direction_score(
            self._make_technical_data(),
            self._make_breadth_data(),
        )
        for key in ("us", "jp", "overall", "fg_label", "fg_raw"):
            assert key in result
        for region in ("us", "jp", "overall"):
            for field in ("score", "label", "emoji"):
                assert field in result[region]

    def test_score_clamped(self):
        """スコアは-10〜+10にクランプされる。"""
        tech = self._make_technical_data("🟢", "🟢")
        breadth = self._make_breadth_data(fg_score=100)
        result = calculate_direction_score(tech, breadth)
        assert -10 <= result["us"]["score"] <= 10
        assert -10 <= result["jp"]["score"] <= 10
