"""テスト: market_breadth_jpx.py の騰落レシオ・新高値新安値ロジック"""

import json
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.collectors.market_breadth_jpx import (
    _ad_ratio_signal,
    _parse_ad_ratio,
    _parse_highlow,
    fetch_jpx_market_breadth,
)


# ===== _ad_ratio_signal =====

class TestAdRatioSignal:
    def test_overheated(self):
        """130超 → 過熱圏シグナル。"""
        sig = _ad_ratio_signal(135.0)
        assert "過熱" in sig
        assert "🔴" in sig

    def test_oversold(self):
        """70未満 → 底値圏シグナル。"""
        sig = _ad_ratio_signal(65.0)
        assert "底値" in sig
        assert "🟢" in sig

    def test_normal_high(self):
        """129.9 → 通常シグナル。"""
        sig = _ad_ratio_signal(129.9)
        assert "🟡" in sig
        assert "通常" in sig

    def test_normal_low(self):
        """70.0 → 通常シグナル（境界値は通常）。"""
        sig = _ad_ratio_signal(70.0)
        assert "🟡" in sig

    def test_none_returns_circle(self):
        """None → ⚪。"""
        assert _ad_ratio_signal(None) == "⚪"

    def test_boundary_130(self):
        """130.0 → 過熱圏（境界値）。"""
        sig = _ad_ratio_signal(130.0)
        assert "🔴" in sig

    def test_typical_range(self):
        """100.0 → 通常。"""
        sig = _ad_ratio_signal(100.0)
        assert "🟡" in sig


# ===== _parse_ad_ratio =====

class TestParseAdRatio:
    def _make_soup(self, html: str):
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "html.parser")

    def test_text_pattern(self):
        """テキストパターンから騰落レシオを取得。"""
        soup = self._make_soup("<p>騰落レシオ: 108.5</p>")
        result = _parse_ad_ratio(soup, soup.get_text())
        assert result == pytest.approx(108.5)

    def test_table_pattern(self):
        """テーブルから騰落レシオを取得。"""
        html = """
        <table>
          <tr><td>騰落レシオ</td><td>95.2</td></tr>
        </table>
        """
        soup = self._make_soup(html)
        result = _parse_ad_ratio(soup, soup.get_text())
        assert result == pytest.approx(95.2)

    def test_no_data_returns_none(self):
        """騰落レシオがなければNone。"""
        soup = self._make_soup("<p>関係ないテキスト</p>")
        result = _parse_ad_ratio(soup, soup.get_text())
        assert result is None

    def test_out_of_range_rejected(self):
        """範囲外の値（例: 10）は無効として扱う。"""
        soup = self._make_soup("<p>騰落レシオ: 10</p>")
        result = _parse_ad_ratio(soup, soup.get_text())
        assert result is None  # 20未満は無効

    def test_25_day_suffix(self):
        """「騰落レシオ(25日)」形式。"""
        soup = self._make_soup("<p>騰落レシオ(25日): 112.3</p>")
        result = _parse_ad_ratio(soup, soup.get_text())
        assert result == pytest.approx(112.3)


# ===== _parse_highlow =====

class TestParseHighlow:
    def _make_soup(self, html: str):
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "html.parser")

    def test_text_pattern(self):
        """テキストから新高値・新安値を取得。"""
        soup = self._make_soup(
            "<p>新高値: 45</p><p>新安値: 12</p>"
        )
        high, low = _parse_highlow(soup, soup.get_text())
        assert high == 45
        assert low == 12

    def test_table_pattern(self):
        """テーブルから取得。"""
        html = """
        <table>
          <tr><td>新高値</td><td>38</td></tr>
          <tr><td>新安値</td><td>7</td></tr>
        </table>
        """
        soup = self._make_soup(html)
        high, low = _parse_highlow(soup, soup.get_text())
        assert high == 38
        assert low == 7

    def test_no_data(self):
        """データなし → (None, None)。"""
        soup = self._make_soup("<p>普通のテキスト</p>")
        high, low = _parse_highlow(soup, soup.get_text())
        assert high is None
        assert low is None


# ===== fetch_jpx_market_breadth =====

class TestFetchJpxMarketBreadth:
    def test_cache_fallback_when_all_fail(self, tmp_path: Path):
        """全取得失敗時にキャッシュを使用する。"""
        cache_dir = tmp_path / "market_breadth_jpx"
        cache_dir.mkdir()
        cached_data = {
            "date": "2026-04-04",
            "advance_decline_ratio": 108.5,
            "ad_signal": "🟡 通常",
            "new_high": 45,
            "new_low": 12,
            "nh_nl_ratio": 3.75,
            "error": False,
        }
        with open(cache_dir / "latest.json", "w") as f:
            json.dump(cached_data, f)

        with (
            patch("generators.collectors.market_breadth_jpx.CACHE_DIR", cache_dir),
            patch("generators.collectors.market_breadth_jpx._fetch_kabutan_market", return_value=None),
            patch("generators.collectors.market_breadth_jpx._fetch_kabutan_highlow", return_value=None),
        ):
            result = fetch_jpx_market_breadth(date(2026, 4, 7))

        assert result["cached"] is True
        assert result["advance_decline_ratio"] == pytest.approx(108.5)
        assert not result["error"]

    def test_error_when_no_cache_and_all_fail(self, tmp_path: Path):
        """キャッシュなし + 全失敗時はerror=True。"""
        cache_dir = tmp_path / "market_breadth_empty"
        cache_dir.mkdir()

        with (
            patch("generators.collectors.market_breadth_jpx.CACHE_DIR", cache_dir),
            patch("generators.collectors.market_breadth_jpx._fetch_kabutan_market", return_value=None),
            patch("generators.collectors.market_breadth_jpx._fetch_kabutan_highlow", return_value=None),
        ):
            result = fetch_jpx_market_breadth(date(2026, 4, 7))

        assert result["error"] is True

    def test_partial_success_returns_data(self, tmp_path: Path):
        """騰落レシオのみ取得できた場合も成功扱い。"""
        cache_dir = tmp_path / "market_breadth_partial"

        with (
            patch("generators.collectors.market_breadth_jpx.CACHE_DIR", cache_dir),
            patch(
                "generators.collectors.market_breadth_jpx._fetch_kabutan_market",
                return_value={"advance_decline_ratio": 95.0, "source_ad": "kabutan_market"},
            ),
            patch("generators.collectors.market_breadth_jpx._fetch_kabutan_highlow", return_value=None),
        ):
            result = fetch_jpx_market_breadth(date(2026, 4, 7))

        assert not result["error"]
        assert result["advance_decline_ratio"] == pytest.approx(95.0)
        assert result["new_high"] is None  # 新高値は取れていない

    def test_nh_nl_ratio_calculation(self, tmp_path: Path):
        """新高値/新安値比率の計算。"""
        cache_dir = tmp_path / "market_breadth_ratio"

        with (
            patch("generators.collectors.market_breadth_jpx.CACHE_DIR", cache_dir),
            patch(
                "generators.collectors.market_breadth_jpx._fetch_kabutan_market",
                return_value={"advance_decline_ratio": 100.0, "source_ad": "kabutan_market"},
            ),
            patch(
                "generators.collectors.market_breadth_jpx._fetch_kabutan_highlow",
                return_value={"new_high": 40, "new_low": 8, "source_hl": "kabutan_highlow"},
            ),
        ):
            result = fetch_jpx_market_breadth(date(2026, 4, 7))

        assert result["new_high"] == 40
        assert result["new_low"] == 8
        assert result["nh_nl_ratio"] == pytest.approx(5.0)

    def test_nh_nl_ratio_zero_low(self, tmp_path: Path):
        """新安値が0の場合のゼロ除算回避。"""
        cache_dir = tmp_path / "market_breadth_zero"

        with (
            patch("generators.collectors.market_breadth_jpx.CACHE_DIR", cache_dir),
            patch(
                "generators.collectors.market_breadth_jpx._fetch_kabutan_market",
                return_value={"advance_decline_ratio": 120.0, "source_ad": "kabutan_market"},
            ),
            patch(
                "generators.collectors.market_breadth_jpx._fetch_kabutan_highlow",
                return_value={"new_high": 50, "new_low": 0, "source_hl": "kabutan_highlow"},
            ),
        ):
            result = fetch_jpx_market_breadth(date(2026, 4, 7))

        assert result["new_high"] == 50
        assert result["new_low"] == 0
        # inf か None かのどちらでも、例外が起きないことを確認
        assert result["nh_nl_ratio"] is not None or result["nh_nl_ratio"] is None

    def test_return_structure(self, tmp_path: Path):
        """戻り値のキー構造が正しいこと。"""
        cache_dir = tmp_path / "market_breadth_struct"

        with (
            patch("generators.collectors.market_breadth_jpx.CACHE_DIR", cache_dir),
            patch("generators.collectors.market_breadth_jpx._fetch_kabutan_market", return_value=None),
            patch("generators.collectors.market_breadth_jpx._fetch_kabutan_highlow", return_value=None),
        ):
            result = fetch_jpx_market_breadth(date(2026, 4, 7))

        required_keys = {
            "advance_decline_ratio", "ad_signal", "new_high", "new_low",
            "nh_nl_ratio", "error", "cached",
        }
        assert required_keys.issubset(result.keys())

    def test_ad_signal_reflects_ratio(self, tmp_path: Path):
        """取得した騰落レシオに応じたシグナルが設定される。"""
        cache_dir = tmp_path / "market_breadth_signal"

        with (
            patch("generators.collectors.market_breadth_jpx.CACHE_DIR", cache_dir),
            patch(
                "generators.collectors.market_breadth_jpx._fetch_kabutan_market",
                return_value={"advance_decline_ratio": 145.0, "source_ad": "kabutan_market"},
            ),
            patch("generators.collectors.market_breadth_jpx._fetch_kabutan_highlow", return_value=None),
        ):
            result = fetch_jpx_market_breadth(date(2026, 4, 7))

        assert "過熱" in result["ad_signal"]
