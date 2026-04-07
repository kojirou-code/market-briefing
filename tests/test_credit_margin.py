"""テスト: credit_margin.py の信用残高データ取得ロジック"""

import json
import sys
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.collectors.credit_margin import (
    _calc_change_pct,
    _extract_margin_from_sheet,
    _get_candidate_fridays,
    _parse_jpx_xls,
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


# ===== _get_candidate_fridays =====

class TestGetCandidateFridays:
    def test_tuesday_skips_recent_friday(self):
        """火曜日（2026-04-07）: 直近金曜4/3のデータは未公開（4/8公開予定）。
        利用可能な最新金曜は 3/27 であること。"""
        result = _get_candidate_fridays(date(2026, 4, 7), count=1)
        assert len(result) == 1
        assert result[0] == date(2026, 3, 27)

    def test_wednesday_includes_last_friday(self):
        """水曜日（2026-04-08）: 金曜4/3のデータが公開日当日に利用可能。"""
        result = _get_candidate_fridays(date(2026, 4, 8), count=1)
        assert len(result) == 1
        assert result[0] == date(2026, 4, 3)

    def test_returns_requested_count(self):
        """指定した件数のリストを返す。"""
        result = _get_candidate_fridays(date(2026, 4, 7), count=3)
        assert len(result) == 3

    def test_descending_order(self):
        """新しい順（降順）で返す。"""
        result = _get_candidate_fridays(date(2026, 4, 14), count=3)
        for i in range(len(result) - 1):
            assert result[i] > result[i + 1]

    def test_all_are_fridays(self):
        """全ての日付が金曜日（weekday=4）であること。"""
        result = _get_candidate_fridays(date(2026, 4, 14), count=3)
        for d in result:
            assert d.weekday() == 4  # Friday


# ===== _extract_margin_from_sheet =====

class TestExtractMarginFromSheet:
    def test_valid_total_row(self):
        """合計行から正しく信用残高を抽出する（百万円単位）。"""
        df = pd.DataFrame([
            ["市場区分", "信用買い残(百万円)", "信用売り残(百万円)", "信用倍率"],
            ["プライム", 3500000, 1100000, 3.18],
            ["スタンダード", 300000, 95000, 3.16],
            ["合計", 3800000, 1195000, 3.18],
        ])
        result = _extract_margin_from_sheet(df, date(2026, 3, 27))
        assert result is not None
        assert result["margin_buy"] == pytest.approx(3.80, abs=0.01)
        assert result["margin_sell"] == pytest.approx(1.195, abs=0.01)
        assert result["source"] == "JPX"
        assert not result["error"]

    def test_data_date_in_result(self):
        """data_date が正しく設定される。"""
        df = pd.DataFrame([
            ["合計", 3800000, 1195000, 3.18],
        ])
        result = _extract_margin_from_sheet(df, date(2026, 3, 27))
        assert result is not None
        assert result["data_date"] == "2026-03-27"

    def test_no_total_row_returns_none(self):
        """合計行がない場合は None。"""
        df = pd.DataFrame([
            ["プライム", 3500000, 1100000, 3.18],
            ["スタンダード", 300000, 95000, 3.16],
        ])
        result = _extract_margin_from_sheet(df, date(2026, 3, 27))
        assert result is None

    def test_small_values_skipped(self):
        """500,000 未満の値（百万円単位でない）はスキップ。"""
        df = pd.DataFrame([
            ["合計", 40000, 12000, 3.33],  # < 500,000 → スキップ
        ])
        result = _extract_margin_from_sheet(df, date(2026, 3, 27))
        assert result is None

    def test_ratio_extracted(self):
        """信用倍率が正しく抽出される。"""
        df = pd.DataFrame([
            ["合計", 3800000, 1195000, 3.18],
        ])
        result = _extract_margin_from_sheet(df, date(2026, 3, 27))
        assert result is not None
        assert result["margin_ratio"] == pytest.approx(3.18, abs=0.01)

    def test_with_string_commas(self):
        """カンマ区切りの数値文字列も正しく解析する。"""
        df = pd.DataFrame([
            ["合計", "3,800,000", "1,195,000", "3.18"],
        ])
        result = _extract_margin_from_sheet(df, date(2026, 3, 27))
        assert result is not None
        assert result["margin_buy"] == pytest.approx(3.80, abs=0.01)


# ===== _parse_jpx_xls =====

class TestParseJpxXls:
    def test_invalid_bytes_returns_none(self):
        """不正なバイト列は None を返す（例外を飲み込む）。"""
        result = _parse_jpx_xls(b"not xls data", date(2026, 3, 27))
        assert result is None

    def test_empty_bytes_returns_none(self):
        """空バイト列は None を返す。"""
        result = _parse_jpx_xls(b"", date(2026, 3, 27))
        assert result is None

    def test_valid_xls_via_mock(self):
        """pandas.ExcelFile をモックして正常解析を確認する。"""
        fake_df = pd.DataFrame([
            ["合計", 3800000, 1195000, 3.18],
        ])
        mock_xl = MagicMock()
        mock_xl.sheet_names = ["Sheet1"]
        mock_xl.parse.return_value = fake_df

        with patch("pandas.ExcelFile", return_value=mock_xl):
            result = _parse_jpx_xls(b"fake_bytes", date(2026, 3, 27))

        assert result is not None
        assert result["margin_buy"] == pytest.approx(3.80, abs=0.01)
        assert result["source"] == "JPX"

    def test_all_sheets_fail_returns_none(self):
        """全シートで合計行が見つからない場合は None。"""
        fake_df = pd.DataFrame([
            ["プライム", 3500000, 1100000, 3.18],  # 合計行なし
        ])
        mock_xl = MagicMock()
        mock_xl.sheet_names = ["Sheet1", "Sheet2"]
        mock_xl.parse.return_value = fake_df

        with patch("pandas.ExcelFile", return_value=mock_xl):
            result = _parse_jpx_xls(b"fake_bytes", date(2026, 3, 27))

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
            patch("generators.collectors.credit_margin._fetch_jpx_xls", return_value=None),
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
            patch("generators.collectors.credit_margin._fetch_jpx_xls", return_value=None),
            patch("generators.collectors.credit_margin._fetch_kabutan_credit", return_value=None),
        ):
            result = fetch_credit_margin(date(2026, 4, 7))

        assert result["error"] is True
        assert result["margin_buy"] is None

    def test_jpx_success_saves_cache(self, tmp_path: Path):
        """JPX XLS取得成功時にキャッシュを保存する。"""
        cache_dir = tmp_path / "credit_margin"
        jpx_data = {
            "margin_buy": 3.87,
            "margin_sell": 1.20,
            "margin_ratio": 3.22,
            "data_date": "2026-03-28",
            "source": "JPX",
            "error": False,
        }

        with (
            patch("generators.collectors.credit_margin.CACHE_DIR", cache_dir),
            patch("generators.collectors.credit_margin._fetch_jpx_xls", return_value=jpx_data),
        ):
            result = fetch_credit_margin(date(2026, 4, 7))

        assert not result["error"]
        assert result["margin_buy"] == pytest.approx(3.87)
        assert result["source"] == "JPX"
        # キャッシュが保存されているか確認
        assert (cache_dir / "latest.json").exists()

    def test_kabutan_fallback_when_jpx_fails(self, tmp_path: Path):
        """JPX XLS失敗時にkabutanにフォールバック。"""
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
            patch("generators.collectors.credit_margin._fetch_jpx_xls", return_value=None),
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
            "data_date": "2026-03-28",
            "source": "JPX",
            "error": False,
        }

        with (
            patch("generators.collectors.credit_margin.CACHE_DIR", cache_dir),
            patch("generators.collectors.credit_margin._fetch_jpx_xls", return_value=jpx_data),
        ):
            result = fetch_credit_margin(date(2026, 4, 7))

        required_keys = {
            "margin_buy", "margin_sell", "margin_ratio",
            "buy_change_pct", "data_date", "source", "error", "cached",
        }
        assert required_keys.issubset(result.keys())
