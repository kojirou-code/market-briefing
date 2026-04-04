"""テスト: sector_etf.py のロジック"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.collectors.sector_etf import (
    _change_signal,
    fetch_sector_etfs,
    SECTOR_ETF_MAP,
)


class TestChangeSignal:
    def test_strong_up(self):
        """1%以上の上昇は🟢。"""
        assert _change_signal(1.0) == "🟢"
        assert _change_signal(2.5) == "🟢"

    def test_strong_down(self):
        """1%以上の下落は🔴。"""
        assert _change_signal(-1.0) == "🔴"
        assert _change_signal(-3.0) == "🔴"

    def test_neutral(self):
        """±1%未満は🟡。"""
        assert _change_signal(0.5) == "🟡"
        assert _change_signal(-0.9) == "🟡"
        assert _change_signal(0.0) == "🟡"

    def test_none(self):
        """Noneは⚪。"""
        assert _change_signal(None) == "⚪"


class TestSectorEtfMap:
    def test_all_11_sectors_defined(self):
        """11セクターすべて定義済み。"""
        assert len(SECTOR_ETF_MAP) == 11

    def test_expected_tickers(self):
        """主要ティッカーが含まれている。"""
        for ticker in ["XLK", "XLF", "XLV", "XLP", "XLE"]:
            assert ticker in SECTOR_ETF_MAP


class TestFetchSectorEtfsReturnShape:
    """fetch_sector_etfs の戻り値の形状テスト（ネットワーク不要）。"""

    def test_all_error_returns_valid_structure(self, monkeypatch):
        """全ティッカー失敗時も正しい構造を返す。"""
        import generators.collectors.sector_etf as mod

        def mock_fetch(tickers):
            return {t: {"value": None, "change_pct": None, "error": True} for t in tickers}

        monkeypatch.setattr(mod, "_fetch_sector_etfs", mock_fetch)
        result = fetch_sector_etfs(["XLK", "XLF"])

        assert "sectors" in result
        assert "top3" in result
        assert "bottom3" in result
        assert result["error"] is True
        assert len(result["sectors"]) == 2

    def test_partial_success_rank_order(self, monkeypatch):
        """一部成功時、変化率でランキングされる。"""
        import generators.collectors.sector_etf as mod

        def mock_fetch(tickers):
            return {
                "XLK": {"value": 200.0, "change_pct": 2.0, "error": False},
                "XLF": {"value": 50.0, "change_pct": -1.5, "error": False},
                "XLV": {"value": 120.0, "change_pct": 0.5, "error": False},
            }

        monkeypatch.setattr(mod, "_fetch_sector_etfs", mock_fetch)
        result = fetch_sector_etfs(["XLK", "XLF", "XLV"])

        sectors = result["sectors"]
        # rank 1 が最高騰落率
        rank1 = next(s for s in sectors if s["rank"] == 1)
        assert rank1["ticker"] == "XLK"
        assert rank1["change_pct"] == 2.0

    def test_signals_assigned(self, monkeypatch):
        """各セクターにsignalが付与される。"""
        import generators.collectors.sector_etf as mod

        def mock_fetch(tickers):
            return {"XLK": {"value": 200.0, "change_pct": 1.5, "error": False}}

        monkeypatch.setattr(mod, "_fetch_sector_etfs", mock_fetch)
        result = fetch_sector_etfs(["XLK"])
        assert result["sectors"][0]["signal"] == "🟢"
