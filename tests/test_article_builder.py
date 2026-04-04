"""テスト: article_builder.py"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.article.article_builder import (
    build_article,
    get_article_filename,
    _format_date_title,
)


def make_minimal_context():
    """テスト用の最小コンテキストを生成する。"""
    return {
        "market_data": {
            "us_indices": [
                {
                    "ticker": "^GSPC", "name": "SP500", "display_name": "S&P 500",
                    "close": 5800.0, "change_pct": 0.72, "signal": "🟢", "error": False
                }
            ],
            "jp_indices": [
                {
                    "ticker": "^N225", "name": "Nikkei225", "display_name": "日経平均",
                    "close": 38500.0, "change_pct": -0.31, "signal": "🟡", "error": False
                }
            ],
        },
        "indicators_data": {
            "indicators": [
                {"name": "VIX", "display_name": "VIX", "value": 18.2, "change_pct": 1.5, "signal": "🟡", "error": False},
                {"name": "US10Y", "display_name": "米10年債", "value": 4.32, "change_pct": 0.1, "error": False},
                {"name": "USDJPY", "display_name": "USD/JPY", "value": 151.20, "change_pct": 0.3, "error": False},
            ],
            "futures_commodities": [
                {"name": "Oil", "display_name": "原油（WTI）", "value": 78.3, "change_pct": -0.5, "error": False},
            ],
            "indicators_by_name": {},
            "futures_by_name": {},
        },
        "technical_data": {
            "us_technical": [
                {
                    "name": "S&P 500", "error": False,
                    "sma": {"signal": "🟢", "sma5": 5800.0, "sma25": 5750.0, "sma75": 5600.0},
                    "rsi": {"signal": "🟡", "value": 55.0},
                    "macd": {"signal": "🟢", "macd": 12.5, "macd_signal": 10.0, "histogram": 2.5},
                    "bb": {"signal": "🟡", "upper": 5900.0, "mid": 5780.0, "lower": 5660.0},
                    "volume": {"signal": "🟡", "ratio": 1.1, "current": 3.5e9, "ma20": 3.2e9},
                }
            ],
            "jp_technical": [],
        },
        "alerts": [],
        "news_data": {
            "top5": [
                {"title": "テストニュース", "url": "https://example.com", "source": "Reuters", "category": "米国金融"},
            ],
            "all_items": [],
            "error": False,
        },
        "calendar_events": [
            {
                "date": date(2026, 4, 10), "date_str": "4/10", "date_full": "2026-04-10",
                "weekday": "金", "event": "CPI発表", "country": "US",
                "importance": "high", "days_until": 6,
            }
        ],
    }


class TestBuildArticle:
    def test_returns_string(self):
        """build_article が文字列を返すこと。"""
        ctx = make_minimal_context()
        result = build_article(**ctx, target_date=date(2026, 4, 4))
        assert isinstance(result, str)
        assert len(result) > 100

    def test_contains_frontmatter(self):
        """Hugo フロントマターが含まれること。"""
        ctx = make_minimal_context()
        result = build_article(**ctx, target_date=date(2026, 4, 4))
        assert result.startswith("---")
        assert "title:" in result
        assert "date:" in result

    def test_contains_us_index(self):
        """米国指数が含まれること。"""
        ctx = make_minimal_context()
        result = build_article(**ctx, target_date=date(2026, 4, 4))
        assert "S&P 500" in result

    def test_contains_jp_index(self):
        """日本指数が含まれること。"""
        ctx = make_minimal_context()
        result = build_article(**ctx, target_date=date(2026, 4, 4))
        assert "日経平均" in result

    def test_contains_news(self):
        """ニュースが含まれること。"""
        ctx = make_minimal_context()
        result = build_article(**ctx, target_date=date(2026, 4, 4))
        assert "テストニュース" in result

    def test_contains_calendar(self):
        """経済カレンダーが含まれること。"""
        ctx = make_minimal_context()
        result = build_article(**ctx, target_date=date(2026, 4, 4))
        assert "CPI" in result

    def test_alert_displayed_when_present(self):
        """アラートがある場合に表示されること。"""
        ctx = make_minimal_context()
        ctx["alerts"] = [{"level": "warning", "emoji": "⚠️", "message": "テストアラート"}]
        result = build_article(**ctx, target_date=date(2026, 4, 4))
        assert "テストアラート" in result

    def test_news_error_fallback(self):
        """ニュース取得失敗時にエラー表示されること。"""
        ctx = make_minimal_context()
        ctx["news_data"] = {"top5": [], "all_items": [], "error": True}
        result = build_article(**ctx, target_date=date(2026, 4, 4))
        assert "取得失敗" in result


class TestHelpers:
    def test_get_article_filename(self):
        """ファイル名が正しい形式であること。"""
        d = date(2026, 4, 7)
        assert get_article_filename(d) == "2026-04-07.md"

    def test_format_date_title_weekday(self):
        """曜日が正しく含まれること。"""
        d = date(2026, 4, 7)  # 火曜日
        title = _format_date_title(d)
        assert "火" in title
        assert "4月" in title
        assert "7日" in title
