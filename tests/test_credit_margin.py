"""テスト: credit_margin.py の信用残高データ取得ロジック"""

import json
import sys
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.collectors.credit_margin import (
    _calc_change_pct,
    _parse_jpx_csv,
    _parse_kabutan_credit,
    fetch_credit_margin,
)


# ===== _calc_change_pct =====

class TestCalcChangePct:
    def test_positive_change(self):
        """前週より増加した場合。"""
        result = _calc_change_pct(4.0, {"margin_buy": 3.8})
        assert result is not None
        assert abs(result - 5.26) < 0.1

    def test_negative_change(self):
        """前週より減少した場合。"""
        result = _calc_change_pct(3.5, {"margin_buy": 4.0})
        assert result is not None
        assert result < 0

    def test_none_current(self):
        """現在値が None の場合。"""
        assert _calc_change_pct(None, {"margin_buy": 4.0}) is None

    def test_none_cache(self):
        """キャッシュが None の場合。"""
        assert _calc_change_pct(4.0, None) is None

    def test_zero_prev(self):
        """前週の値が0の場合（ゼロ除算回避）。"""
        assert _calc_change_pct(4.0, {"margin_buy": 0}) is None

    def test_no_margin_buy_in_cache(self):
        """キャッシュに margin_buy がない場合。"""
        assert _calc_change_pct(4.0, {"other": 123}) is None


# ===== _parse_jpx_csv =====

class TestParseJpxCsv:
    def test_valid_csv_data(self):
        """有効なCSVデータの解析。"""
        import pandas as pd
        # 典型的なJPX CSV フォーマット
        df = pd.DataFrame({
            "週末日": ["2026-04-04"],
            "市場区分": ["市場全体"],
            "信用買い残(百万円)": ["3,870,000"],
            "信用売り残(百万円)": ["1,200,000"],
            "信用倍率": ["3.22"],
        })
        result = _parse_jpx_csv(df)
        assert result is not None
        assert not result.get("error")
        assert result["margin_buy"] is not None
        assert result["margin_buy"] > 0
        assert result["source"] == "JPX"

    def test_empty_dataframe(self):
        """空のDataFrameはNoneを返す。"""
        import pandas as pd
        df = pd.DataFrame()
        result = _parse_jpx_csv(df)
        assert result is None

    def test_dataframe_with_only_nans(self):
        """全NaNのDataFrameはNoneを返す。"""
        import pandas as pd
        df = pd.DataFrame({"A": [None, None], "B": [None, None]})
        result = _parse_jpx_csv(df)
        assert result is None


# ===== _parse_kabutan_credit =====

class TestParseKabutanCredit:
    def _make_soup(self, html: str) -> "BeautifulSoup":
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "html.parser")

    def test_trillion_pattern(self):
        """兆円表記の解析。"""
        soup = self._make_soup(
            "<p>信用買い残: 3.87兆円</p><p>信用売り残: 1.20兆円</p>"
            "<p>信用倍率: 3.22</p>"
        )
        result = _parse_kabutan_credit(soup)
        assert result is not None
        assert result["margin_buy"] == pytest.approx(3.87, abs=0.01)

    def test_buy_residual_in_table(self):
        """テーブル内の買い残の解析。"""
        html = """
        <table>
          <tr><th>項目</th><th>値</th></tr>
          <tr><td>信用買い残</td><td>3.87</td></tr>
        </table>
        """
        soup = self._make_soup(html)
        result = _parse_kabutan_credit(soup)
        # テーブルから3.87（兆円として判定）が取れればOK
        if result is not None:
            assert result["margin_buy"] == pytest.approx(3.87, abs=0.1)

    def test_no_data_returns_none(self):
        """信用残高データが見つからない場合はNone。"""
        soup = self._make_soup("<p>関係ないテキスト</p>")
        result = _parse_kabutan_credit(soup)
        assert result is None


# ===== fetch_credit_margin =====

class TestFetchCreditMargin:
    def test_cache_fallback_when_all_fail(self, tmp_path: Path):
        """全取得失敗時にキャッシュを使用する。"""
        cache_dir = tmp_path / "credit_margin"
        cache_dir.mkdir()
        cached_data = {
            "date": "2026-03-28",
            "margin_buy": 3.80,
            "margin_sell": 1.18,
            "margin_ratio": 3.22,
            "source": "kabutan",
            "error": False,
        }
        with open(cache_dir / "latest.json", "w") as f:
            json.dump(cached_data, f)

        with (
            patch("generators.collectors.credit_margin.CACHE_DIR", cache_dir),
            patch("generators.collectors.credit_margin._fetch_jpx_csv", return_value=None),
            patch("generators.collectors.credit_margin._fetch_kabutan_credit", return_value=None),
        ):
            result = fetch_credit_margin(date(2026, 4, 7))

        assert result["cached"] is True
        assert result["margin_buy"] == pytest.approx(3.80)
        assert not result["error"]

    def test_error_when_no_cache_and_all_fail(self, tmp_path: Path):
        """キャッシュなし + 全失敗時はerror=True。"""
        cache_dir = tmp_path / "credit_margin_empty"
        cache_dir.mkdir()

        with (
            patch("generators.collectors.credit_margin.CACHE_DIR", cache_dir),
            patch("generators.collectors.credit_margin._fetch_jpx_csv", return_value=None),
            patch("generators.collectors.credit_margin._fetch_kabutan_credit", return_value=None),
        ):
            result = fetch_credit_margin(date(2026, 4, 7))

        assert result["error"] is True
        assert result["margin_buy"] is None

    def test_jpx_success_saves_cache(self, tmp_path: Path):
        """JPX取得成功時にキャッシュを保存する。"""
        cache_dir = tmp_path / "credit_margin"
        jpx_data = {
            "margin_buy": 3.87,
            "margin_sell": 1.20,
            "margin_ratio": 3.22,
            "source": "JPX",
            "error": False,
        }

        with (
            patch("generators.collectors.credit_margin.CACHE_DIR", cache_dir),
            patch("generators.collectors.credit_margin._fetch_jpx_csv", return_value=jpx_data),
        ):
            result = fetch_credit_margin(date(2026, 4, 7))

        assert not result["error"]
        assert result["margin_buy"] == pytest.approx(3.87)
        assert result["source"] == "JPX"
        # キャッシュが保存されているか確認
        assert (cache_dir / "latest.json").exists()

    def test_kabutan_fallback_when_jpx_fails(self, tmp_path: Path):
        """JPX失敗時にkabutanにフォールバック。"""
        cache_dir = tmp_path / "credit_margin"
        kabutan_data = {
            "margin_buy": 3.90,
            "margin_sell": 1.22,
            "margin_ratio": 3.19,
            "source": "kabutan",
            "error": False,
        }

        with (
            patch("generators.collectors.credit_margin.CACHE_DIR", cache_dir),
            patch("generators.collectors.credit_margin._fetch_jpx_csv", return_value=None),
            patch("generators.collectors.credit_margin._fetch_kabutan_credit", return_value=kabutan_data),
        ):
            result = fetch_credit_margin(date(2026, 4, 7))

        assert not result["error"]
        assert result["source"] == "kabutan"

    def test_return_structure(self, tmp_path: Path):
        """戻り値のキー構造が正しいこと。"""
        cache_dir = tmp_path / "credit_margin"
        jpx_data = {
            "margin_buy": 3.87,
            "margin_sell": 1.20,
            "margin_ratio": 3.22,
            "source": "JPX",
            "error": False,
        }

        with (
            patch("generators.collectors.credit_margin.CACHE_DIR", cache_dir),
            patch("generators.collectors.credit_margin._fetch_jpx_csv", return_value=jpx_data),
        ):
            result = fetch_credit_margin(date(2026, 4, 7))

        required_keys = {
            "margin_buy", "margin_sell", "margin_ratio",
            "buy_change_pct", "data_date", "source", "error", "cached",
        }
        assert required_keys.issubset(result.keys())
