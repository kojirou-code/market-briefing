"""テスト: alert_checker.py"""

import sys
from pathlib import Path

import pytest

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.article.alert_checker import check_alerts

# alert_thresholds.yaml のパス
THRESHOLDS_PATH = str(Path(__file__).parent.parent / "generators" / "config" / "alert_thresholds.yaml")


def make_market_data(us_changes=None, jp_changes=None):
    """テスト用ダミー市場データを生成する。"""
    us_changes = us_changes or []
    jp_changes = jp_changes or []
    return {
        "us_indices": [
            {"display_name": f"US{i}", "change_pct": c, "error": False}
            for i, c in enumerate(us_changes)
        ],
        "jp_indices": [
            {"display_name": f"JP{i}", "change_pct": c, "error": False}
            for i, c in enumerate(jp_changes)
        ],
    }


def make_indicators(vix_change=None, vix_value=18.0, usdjpy_change=None):
    """テスト用ダミー指標データを生成する。"""
    return {
        "indicators_by_name": {
            "VIX": {"value": vix_value, "change_pct": vix_change, "error": False},
            "USDJPY": {"value": 150.0, "change_pct": usdjpy_change, "error": False},
        }
    }


class TestNoAlerts:
    def test_normal_market(self):
        """通常時はアラートなし。"""
        market = make_market_data(us_changes=[0.5, 0.3], jp_changes=[-0.2])
        indicators = make_indicators(vix_change=2.0, usdjpy_change=0.3)
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert alerts == []

    def test_no_data(self):
        """データなしでもクラッシュしない。"""
        market = {"us_indices": [], "jp_indices": []}
        indicators = {"indicators_by_name": {}}
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert isinstance(alerts, list)


class TestVixAlert:
    def test_vix_surge_triggers_alert(self):
        """VIX +20% 以上でアラート発生。"""
        market = make_market_data()
        indicators = make_indicators(vix_change=25.0, vix_value=32.0)
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert any("VIX" in a["message"] for a in alerts)
        assert any(a["level"] == "warning" for a in alerts)

    def test_vix_just_below_threshold_no_alert(self):
        """VIX +19.9% はアラートなし。"""
        market = make_market_data()
        indicators = make_indicators(vix_change=19.9, vix_value=24.0)
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert not any("VIX" in a["message"] for a in alerts)


class TestIndexCrashAlert:
    def test_us_crash_triggers_alert(self):
        """S&P 500 -2.5% でアラート発生。"""
        market = make_market_data(us_changes=[-2.5])
        indicators = make_indicators()
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert any("急落" in a["message"] for a in alerts)
        assert any(a["level"] == "warning" for a in alerts)

    def test_us_surge_triggers_info(self):
        """NASDAQ +3.0% でアラート発生（info）。"""
        market = make_market_data(us_changes=[3.0])
        indicators = make_indicators()
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert any("急騰" in a["message"] for a in alerts)
        assert any(a["level"] == "info" for a in alerts)

    def test_jp_crash_triggers_alert(self):
        """日経平均 -2.1% でアラート発生。"""
        market = make_market_data(jp_changes=[-2.1])
        indicators = make_indicators()
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert any("急落" in a["message"] for a in alerts)


class TestUsdJpyAlert:
    def test_yen_surge_triggers_alert(self):
        """円急騰（-1.8%）でアラート発生。"""
        market = make_market_data()
        indicators = make_indicators(usdjpy_change=-1.8)
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert any("USD/JPY" in a["message"] for a in alerts)

    def test_yen_drop_triggers_alert(self):
        """円急落（+2.0%）でアラート発生。"""
        market = make_market_data()
        indicators = make_indicators(usdjpy_change=2.0)
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert any("USD/JPY" in a["message"] for a in alerts)

    def test_small_usdjpy_move_no_alert(self):
        """USD/JPY 0.5% 変動はアラートなし。"""
        market = make_market_data()
        indicators = make_indicators(usdjpy_change=0.5)
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert not any("USD/JPY" in a["message"] for a in alerts)


class TestNoneValues:
    def test_none_change_pct_no_crash(self):
        """change_pct が None でもクラッシュしない。"""
        market = make_market_data(us_changes=[None, 1.0], jp_changes=[None])
        indicators = make_indicators(vix_change=None, usdjpy_change=None)
        alerts = check_alerts(market, indicators, THRESHOLDS_PATH)
        assert isinstance(alerts, list)
