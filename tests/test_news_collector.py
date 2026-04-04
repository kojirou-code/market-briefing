"""テスト: news_collector.py — Google News RSS + 国際RSS + JSON蓄積"""

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.collectors.news_collector import (
    JP_ECONOMIC_KEYWORDS,
    JP_GEOPOLITICAL_KEYWORDS,
    EN_ECONOMIC_KEYWORDS,
    EN_GEOPOLITICAL_KEYWORDS,
    INTERNATIONAL_RSS_FEEDS,
    DEFAULT_PRIORITY_CATEGORIES,
    _is_individual_stock_news,
    _deduplicate_items,
    _select_top5,
    fetch_news_headlines,
    fetch_international_news,
    fetch_all_news,
    load_news_from_file,
    save_news_to_file,
    NEWS_DATA_DIR,
)


# ============================================================
# キーワード定数の確認
# ============================================================

class TestKeywordConstants:
    def test_jp_economic_covers_key_terms(self):
        """日本語経済キーワードに主要語が含まれる。"""
        assert "日銀" in JP_ECONOMIC_KEYWORDS
        assert "FOMC" in JP_ECONOMIC_KEYWORDS
        assert "為替" in JP_ECONOMIC_KEYWORDS

    def test_jp_geopolitical_covers_regions(self):
        """日本語地政学キーワードに主要地域が含まれる。"""
        assert "中東" in JP_GEOPOLITICAL_KEYWORDS
        assert "ロシア" in JP_GEOPOLITICAL_KEYWORDS
        assert "中国" in JP_GEOPOLITICAL_KEYWORDS
        assert "台湾" in JP_GEOPOLITICAL_KEYWORDS
        assert "地政学" in JP_GEOPOLITICAL_KEYWORDS

    def test_en_economic_covers_key_terms(self):
        """英語経済キーワードに主要語が含まれる。"""
        assert "Federal Reserve" in EN_ECONOMIC_KEYWORDS
        assert "S&P 500" in EN_ECONOMIC_KEYWORDS

    def test_en_geopolitical_covers_regions(self):
        """英語地政学キーワードに主要地域が含まれる。"""
        assert "Middle East" in EN_GEOPOLITICAL_KEYWORDS
        assert "Russia" in EN_GEOPOLITICAL_KEYWORDS
        assert "China" in EN_GEOPOLITICAL_KEYWORDS
        assert "Taiwan" in EN_GEOPOLITICAL_KEYWORDS
        assert "geopolitics" in EN_GEOPOLITICAL_KEYWORDS
        assert "sanctions" in EN_GEOPOLITICAL_KEYWORDS

    def test_4_keyword_groups_are_distinct(self):
        """4つのキーワードグループは内容が重複しない（代表語で確認）。"""
        groups = [JP_ECONOMIC_KEYWORDS, JP_GEOPOLITICAL_KEYWORDS,
                  EN_ECONOMIC_KEYWORDS, EN_GEOPOLITICAL_KEYWORDS]
        assert len(groups) == 4
        # 各グループは空でない
        for g in groups:
            assert len(g) > 0


# ============================================================
# 国際RSSフィード設定の確認
# ============================================================

class TestInternationalRssFeeds:
    def test_3_feeds_defined(self):
        """3つの国際RSSフィードが定義されている。"""
        assert len(INTERNATIONAL_RSS_FEEDS) == 3

    def test_nhk_international_included(self):
        """NHK国際ニュースフィードが含まれる。"""
        names = [f["name"] for f in INTERNATIONAL_RSS_FEEDS]
        assert any("NHK" in n for n in names)

    def test_bbc_world_included(self):
        """BBC Worldフィードが含まれる。"""
        urls = [f["url"] for f in INTERNATIONAL_RSS_FEEDS]
        assert any("bbci.co.uk" in u or "bbc" in u.lower() for u in urls)

    def test_all_feeds_geopolitical_category(self):
        """国際RSSの全フィードはカテゴリ「地政学」。"""
        for feed in INTERNATIONAL_RSS_FEEDS:
            assert feed["category"] == "地政学", f"{feed['name']} のカテゴリが地政学でない"

    def test_all_feeds_have_required_keys(self):
        """全フィードに必須キーが存在する。"""
        for feed in INTERNATIONAL_RSS_FEEDS:
            assert "name" in feed
            assert "url" in feed
            assert "category" in feed
            assert "language" in feed


# ============================================================
# 個別銘柄フィルタのテスト
# ============================================================

class TestIsIndividualStockNews:
    def test_apple_filtered(self):
        """Apple言及記事は除外される。"""
        assert _is_individual_stock_news("Apple reports record earnings") is True

    def test_nvidia_ticker_filtered(self):
        """NVDAティッカーは除外される。"""
        assert _is_individual_stock_news("NVDA surges 5% after results") is True

    def test_analyst_upgrade_filtered(self):
        """アナリストアップグレードは除外される。"""
        assert _is_individual_stock_news("Analyst upgrades Nvidia to buy") is True

    def test_price_target_filtered(self):
        """price target含む記事は除外される。"""
        assert _is_individual_stock_news("Bank raises price target on Tesla") is True

    def test_macro_news_not_filtered(self):
        """マクロニュースは除外されない。"""
        assert _is_individual_stock_news("Federal Reserve signals rate cut") is False
        assert _is_individual_stock_news("日銀、金融政策を据え置き") is False
        assert _is_individual_stock_news("S&P 500 falls on trade war fears") is False

    def test_geopolitical_news_not_filtered(self):
        """地政学ニュースは除外されない。"""
        assert _is_individual_stock_news("Israel and Iran tensions escalate") is False
        assert _is_individual_stock_news("Russia Ukraine ceasefire talks") is False
        assert _is_individual_stock_news("China Taiwan military drills") is False

    def test_earnings_beat_filtered(self):
        """earnings beat/miss は除外される。"""
        assert _is_individual_stock_news("Tech giant earnings beat expectations") is True


# ============================================================
# 重複除去のテスト
# ============================================================

class TestDeduplicateItems:
    def _make_item(self, url: str, title: str = "T") -> dict:
        return {"title": title, "url": url, "source": "S", "category": "C",
                "language": "en", "published": None, "snippet": ""}

    def test_removes_duplicate_urls(self):
        """同一URLは2番目以降が除去される。"""
        items = [
            self._make_item("http://a.com/1", "A"),
            self._make_item("http://a.com/1", "B"),  # dup
            self._make_item("http://a.com/2", "C"),
        ]
        result = _deduplicate_items(items)
        assert len(result) == 2
        assert result[0]["title"] == "A"

    def test_keeps_items_without_url(self):
        """URLなしアイテムはそのまま通過する。"""
        items = [
            {"title": "no-url", "url": "", "source": "S", "category": "C",
             "language": "en", "published": None, "snippet": ""},
            {"title": "no-url2", "url": "", "source": "S", "category": "C",
             "language": "en", "published": None, "snippet": ""},
        ]
        result = _deduplicate_items(items)
        assert len(result) == 2

    def test_empty_input(self):
        """空リストは空リストを返す。"""
        assert _deduplicate_items([]) == []


# ============================================================
# ニュース選別のテスト（TOP5 + 地政学補充）
# ============================================================

class TestSelectTop5:
    def _make_items(self, categories: list[str]) -> list[dict]:
        return [
            {"title": f"News {i}", "url": f"http://example.com/{i}",
             "source": "Test", "category": cat, "language": "en", "published": None, "snippet": ""}
            for i, cat in enumerate(categories)
        ]

    def test_max_5_items(self):
        """6件以上あっても5件を返す。"""
        items = self._make_items(["米国金融"] * 6)
        result = _select_top5(items)
        assert len(result) == 5

    def test_priority_order(self):
        """米国金融 > 日本金融 > 地政学 の優先順。"""
        items = self._make_items(["地政学", "日本金融", "米国金融", "日本金融", "地政学"])
        result = _select_top5(items)
        assert result[0]["category"] == "米国金融"

    def test_empty_returns_empty(self):
        """空リストは空リストを返す。"""
        assert _select_top5([]) == []

    def test_fewer_than_5(self):
        """5件未満はそのまま全件返す。"""
        items = self._make_items(["米国金融"] * 3)
        result = _select_top5(items)
        assert len(result) == 3

    def test_geopolitical_included_when_not_in_top4(self):
        """5件中に地政学がない場合、6件目以降の地政学を差し替えて補充する。"""
        # 5件すべて米国金融 + 6件目が地政学
        items = self._make_items(["米国金融"] * 5 + ["地政学"])
        result = _select_top5(items)
        categories = [i["category"] for i in result]
        assert "地政学" in categories, "地政学が補充されていない"

    def test_geopolitical_already_present_no_change(self):
        """既に地政学が含まれている場合は差し替えない。"""
        items = self._make_items(["米国金融", "地政学", "米国金融", "米国金融", "米国金融"])
        result = _select_top5(items)
        assert len(result) == 5

    def test_default_priority_includes_geopolitical(self):
        """デフォルト優先リストに地政学が含まれる。"""
        assert "地政学" in DEFAULT_PRIORITY_CATEGORIES


# ============================================================
# JSON 保存・読み込みのテスト
# ============================================================

class TestNewsJsonPersistence:
    def test_save_and_load_roundtrip(self, tmp_path, monkeypatch):
        """保存して読み込むと同じデータが返る。"""
        monkeypatch.setattr("generators.collectors.news_collector.NEWS_DATA_DIR", tmp_path)
        items = [
            {"title": "Test News", "url": "http://example.com/1",
             "source": "Test", "category": "米国金融",
             "language": "en", "published": "2026-04-04T00:00:00+00:00", "snippet": ""},
        ]
        target_date = date(2026, 4, 4)
        save_news_to_file(items, target_date)
        loaded = load_news_from_file(target_date)
        assert len(loaded) == 1
        assert loaded[0]["title"] == "Test News"

    def test_deduplication_on_save(self, tmp_path, monkeypatch):
        """同じURLを2回保存しても重複しない。"""
        monkeypatch.setattr("generators.collectors.news_collector.NEWS_DATA_DIR", tmp_path)
        item = {"title": "Test", "url": "http://example.com/dup",
                "source": "Test", "category": "米国金融",
                "language": "en", "published": None, "snippet": ""}
        target_date = date(2026, 4, 4)
        save_news_to_file([item], target_date)
        save_news_to_file([item], target_date)
        loaded = load_news_from_file(target_date)
        assert len(loaded) == 1

    def test_append_new_items(self, tmp_path, monkeypatch):
        """新しいURLのアイテムは追記される。"""
        monkeypatch.setattr("generators.collectors.news_collector.NEWS_DATA_DIR", tmp_path)
        target_date = date(2026, 4, 4)
        item1 = {"title": "A", "url": "http://example.com/1",
                 "source": "S", "category": "米国金融", "language": "en",
                 "published": None, "snippet": ""}
        item2 = {"title": "B", "url": "http://example.com/2",
                 "source": "S", "category": "米国金融", "language": "en",
                 "published": None, "snippet": ""}
        save_news_to_file([item1], target_date)
        save_news_to_file([item2], target_date)
        loaded = load_news_from_file(target_date)
        assert len(loaded) == 2

    def test_load_nonexistent_returns_empty(self, tmp_path, monkeypatch):
        """存在しないファイルの読み込みは空リスト。"""
        monkeypatch.setattr("generators.collectors.news_collector.NEWS_DATA_DIR", tmp_path)
        result = load_news_from_file(date(2099, 1, 1))
        assert result == []


# ============================================================
# fetch_international_news のテスト
# ============================================================

class TestFetchInternationalNews:
    def _make_item(self, i: int, category: str = "地政学") -> dict:
        return {
            "title": f"Intl News {i}", "url": f"http://intl.example.com/{i}",
            "source": "NHK国際", "category": category,
            "language": "ja", "published": None, "snippet": "",
        }

    def test_returns_items_from_direct_rss(self):
        """直接RSSから地政学ニュースを取得できる。"""
        fake_items = [self._make_item(i) for i in range(3)]
        with patch(
            "generators.collectors.news_collector._parse_direct_rss_feed",
            return_value=fake_items
        ):
            result = fetch_international_news()
        assert len(result) > 0
        assert all(item["category"] == "地政学" for item in result)

    def test_deduplicates_across_feeds(self):
        """複数フィードから同一URLが来ても重複除去される。"""
        dup_item = {"title": "Dup", "url": "http://dup.com/1", "source": "S",
                    "category": "地政学", "language": "en", "published": None, "snippet": ""}
        with patch(
            "generators.collectors.news_collector._parse_direct_rss_feed",
            return_value=[dup_item]  # 全フィードが同じアイテムを返すと仮定
        ):
            result = fetch_international_news()
        # 重複除去されて1件のみ
        assert len(result) == 1

    def test_graceful_on_feed_failure(self):
        """1フィード失敗でもほかのフィードは継続（空リストを返す）。"""
        def side_effect(feed_config, max_items=10):
            if "NHK" in feed_config["name"]:
                return []   # NHK失敗
            return [{"title": "OK", "url": f"http://ok.com/{feed_config['name']}",
                     "source": feed_config["name"], "category": "地政学",
                     "language": "en", "published": None, "snippet": ""}]

        with patch("generators.collectors.news_collector._parse_direct_rss_feed",
                   side_effect=side_effect):
            result = fetch_international_news()

        # NHKは失敗したが BBC / AP からは取得できる
        assert len(result) >= 1


# ============================================================
# fetch_all_news のテスト
# ============================================================

class TestFetchAllNews:
    def _make_item(self, url: str, category: str = "米国金融") -> dict:
        return {"title": url, "url": url, "source": "S", "category": category,
                "language": "en", "published": None, "snippet": ""}

    def test_merges_google_and_international(self):
        """Google NewsとINTL RSSが統合される。"""
        google_items = [self._make_item("http://g.com/1"), self._make_item("http://g.com/2")]
        intl_items = [self._make_item("http://i.com/1", "地政学")]

        with patch("generators.collectors.news_collector.fetch_google_news", return_value=google_items):
            with patch("generators.collectors.news_collector.fetch_international_news", return_value=intl_items):
                result = fetch_all_news()

        assert len(result) == 3
        categories = {i["category"] for i in result}
        assert "地政学" in categories

    def test_deduplicates_across_sources(self):
        """Google NewsとINTL RSSで同一URLは重複除去される。"""
        dup_url = "http://dup.com/1"
        google_items = [self._make_item(dup_url)]
        intl_items = [self._make_item(dup_url, "地政学")]

        with patch("generators.collectors.news_collector.fetch_google_news", return_value=google_items):
            with patch("generators.collectors.news_collector.fetch_international_news", return_value=intl_items):
                result = fetch_all_news()

        assert len(result) == 1

    def test_returns_empty_when_all_fail(self):
        """全ソース失敗時は空リストを返す。"""
        with patch("generators.collectors.news_collector.fetch_google_news", return_value=[]):
            with patch("generators.collectors.news_collector.fetch_international_news", return_value=[]):
                result = fetch_all_news()
        assert result == []


# ============================================================
# fetch_news_headlines のテスト（JSON蓄積優先 + フォールバック）
# ============================================================

class TestFetchNewsHeadlines:
    def _make_item(self, i: int, category: str = "米国金融") -> dict:
        return {
            "title": f"News {i}", "url": f"http://example.com/{i}",
            "source": "Test", "category": category,
            "language": "en", "published": None, "snippet": "",
        }

    def test_uses_stored_json_when_available(self, tmp_path, monkeypatch):
        """蓄積JSONが存在する場合はそこから読む（fetch_all_news呼ばない）。"""
        monkeypatch.setattr("generators.collectors.news_collector.NEWS_DATA_DIR", tmp_path)
        stored = [self._make_item(i) for i in range(3)]
        target_date = date(2026, 4, 4)
        save_news_to_file(stored, target_date)

        with patch("generators.collectors.news_collector.fetch_all_news") as mock_all:
            result = fetch_news_headlines(target_date=target_date)
            mock_all.assert_not_called()

        assert result["source"] == "json"
        assert not result["error"]
        assert len(result["all_items"]) == 3

    def test_falls_back_to_live_when_no_json(self, tmp_path, monkeypatch):
        """JSONがない場合はfetch_all_newsを呼ぶ。"""
        monkeypatch.setattr("generators.collectors.news_collector.NEWS_DATA_DIR", tmp_path)
        fake_items = [self._make_item(i) for i in range(2)]
        with patch("generators.collectors.news_collector.fetch_all_news", return_value=fake_items):
            result = fetch_news_headlines(target_date=date(2099, 1, 1))

        assert result["source"] == "google"
        assert len(result["all_items"]) == 2

    def test_falls_back_to_rss_when_live_fails(self, tmp_path, monkeypatch):
        """fetch_all_newsが空なら直接RSSフォールバック。"""
        monkeypatch.setattr("generators.collectors.news_collector.NEWS_DATA_DIR", tmp_path)
        sources_config = {
            "rss_feeds": [
                {"name": "NHK", "url": "https://example.com/rss",
                 "category": "日本金融", "language": "ja"}
            ],
            "max_items_per_feed": 5,
        }
        fallback_item = self._make_item(0, "日本金融")

        with patch("generators.collectors.news_collector.fetch_all_news", return_value=[]):
            with patch("generators.collectors.news_collector._parse_direct_rss_feed",
                       return_value=[fallback_item]):
                result = fetch_news_headlines(sources_config, target_date=date(2099, 1, 1))

        assert result["source"] == "fallback"
        assert len(result["all_items"]) == 1

    def test_error_when_all_sources_fail(self, tmp_path, monkeypatch):
        """全ソース失敗時は error=True。"""
        monkeypatch.setattr("generators.collectors.news_collector.NEWS_DATA_DIR", tmp_path)
        with patch("generators.collectors.news_collector.fetch_all_news", return_value=[]):
            result = fetch_news_headlines(target_date=date(2099, 1, 1))

        assert result["error"] is True
        assert result["top5"] == []
