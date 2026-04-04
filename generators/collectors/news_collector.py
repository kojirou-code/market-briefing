"""
news_collector.py — ニュース収集（Google News RSS + 国際RSS直接取得 + JSON蓄積）

Phase 3強化版:
- Google News RSSを「経済・金融」と「地政学・安全保障」の2クエリ×日英 = 計4クエリに分割
- NHK国際 / BBC World / AP国際ニュースの直接RSSも取得（地政学カバレッジ強化）
- 1日4回取得（5:00/12:00/18:00/23:00）した結果を data/news/YYYY-MM-DD.json に蓄積
- URLベース重複除去・個別銘柄フィルタ
- Gemini API失敗時は既存RSSヘッドライン方式にフォールバック
"""

import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import feedparser

logger = logging.getLogger(__name__)

# プロジェクトルート（このファイルから3階層上）
_PROJECT_ROOT = Path(__file__).parent.parent.parent
NEWS_DATA_DIR = _PROJECT_ROOT / "data" / "news"

# Google News RSS ベースURL
GOOGLE_NEWS_RSS_BASE = "https://news.google.com/rss/search?q={query}&hl={hl}&gl={gl}&ceid={ceid}"

# ===== Google News キーワード（4クエリに分割） =====

# 日本語 ① 経済・金融
JP_ECONOMIC_KEYWORDS = (
    "日銀 OR 金融政策 OR 日経平均 OR 為替 OR 円安 OR 関税 OR FOMC OR 利上げ OR 利下げ OR 物価"
)

# 日本語 ② 地政学・安全保障
JP_GEOPOLITICAL_KEYWORDS = (
    "中東 OR イラン OR イスラエル OR ホルムズ OR 原油 OR ロシア OR ウクライナ"
    " OR 中国 OR 台湾 OR 有事 OR 安全保障 OR 地政学 OR 制裁"
)

# 英語 ① 経済・金融
EN_ECONOMIC_KEYWORDS = (
    "Federal Reserve OR interest rate OR S&P 500 OR trade war OR oil price"
    " OR inflation OR recession OR tariff OR bond yield"
)

# 英語 ② 地政学・安全保障
EN_GEOPOLITICAL_KEYWORDS = (
    "Middle East OR Iran OR Israel OR Hormuz OR Russia OR Ukraine"
    " OR China OR Taiwan OR NATO OR military OR geopolitics OR crude oil OR sanctions"
)

# ===== 国際ニュース直接RSSフィード設定 =====
# graceful degradation: 取得失敗は警告のみでスキップ

INTERNATIONAL_RSS_FEEDS: list[dict[str, str]] = [
    {
        "name": "NHK国際",
        "url": "https://www.nhk.or.jp/rss/news/cat6.xml",
        "category": "地政学",
        "language": "ja",
    },
    {
        "name": "BBC World",
        "url": "https://feeds.bbci.co.uk/news/world/rss.xml",
        "category": "地政学",
        "language": "en",
    },
    {
        "name": "AP International",
        "url": "https://rsshub.app/apnews/topics/world-news",
        "category": "地政学",
        "language": "en",
    },
]

# ===== 個別銘柄・アナリスト名フィルタ =====
INDIVIDUAL_STOCK_PATTERNS = [
    r"\b(Apple|Microsoft|Google|Amazon|Tesla|Nvidia|Meta|Netflix|AMD)\b",
    r"\b(AAPL|MSFT|GOOGL|AMZN|TSLA|NVDA|META|NFLX)\b",
    r"\banalyst\s+(?:upgrades?|downgrades?|raises?|cuts?)\b",
    r"\bprice\s+target\b",
    r"\bearnings\s+(?:beat|miss)\b",
]

# ニュース収集ウィンドウ（時間）
NEWS_CUTOFF_HOURS = 36

# TOP5選択のデフォルト優先順（地政学を追加）
DEFAULT_PRIORITY_CATEGORIES = ["米国金融", "日本金融", "地政学"]


# ===== 内部ユーティリティ =====

def _build_google_news_url(query: str, hl: str = "ja", gl: str = "JP", ceid: str = "JP:ja") -> str:
    """Google News RSS のURLを構築する。"""
    return GOOGLE_NEWS_RSS_BASE.format(
        query=quote(query),
        hl=hl,
        gl=gl,
        ceid=ceid,
    )


def _is_individual_stock_news(title: str) -> bool:
    """個別銘柄・アナリスト関連ニュースを除外する。"""
    for pattern in INDIVIDUAL_STOCK_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return True
    return False


def _deduplicate_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """URLベースで重複を除去する。URLなし記事は全件通過させる。"""
    seen_urls: set[str] = set()
    deduped: list[dict] = []
    for item in items:
        url = item.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            deduped.append(item)
        elif not url:
            deduped.append(item)
    return deduped


def _parse_google_news_feed(
    url: str, source_name: str, category: str, language: str
) -> list[dict[str, Any]]:
    """Google News RSSフィードをパースする。失敗時は空リストを返す。"""
    try:
        feed = feedparser.parse(url)
        if feed.bozo and not feed.entries:
            logger.warning(f"Google News RSS parse error for {source_name}: {feed.bozo_exception}")
            return []

        items = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=NEWS_CUTOFF_HOURS)

        for entry in feed.entries[:20]:
            published = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                except Exception:
                    published = None

            if published and published < cutoff:
                continue

            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()

            if not title:
                continue

            # 個別銘柄フィルタ（経済・金融クエリのみ適用、地政学は除外しない）
            if category != "地政学" and _is_individual_stock_news(title):
                logger.debug(f"個別銘柄記事をスキップ: {title[:50]}")
                continue

            snippet = ""
            if hasattr(entry, "summary"):
                snippet = re.sub(r"<[^>]+>", "", entry.get("summary", "")).strip()[:200]

            items.append({
                "title": title,
                "url": link,
                "source": source_name,
                "category": category,
                "language": language,
                "published": published.isoformat() if published else None,
                "snippet": snippet,
            })

        logger.info(f"{source_name}: {len(items)}件取得")
        return items

    except Exception as e:
        logger.error(f"{source_name}: Google News RSS取得エラー: {e}")
        return []


def _parse_direct_rss_feed(feed_config: dict, max_items: int = 10) -> list[dict[str, Any]]:
    """単一RSSフィードを直接取得してパースする（国際RSS・フォールバック共通）。"""
    try:
        feed = feedparser.parse(feed_config["url"])
        if feed.bozo and not feed.entries:
            logger.warning(f"RSS parse error for {feed_config['name']}: {feed.bozo_exception}")
            return []

        items = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=NEWS_CUTOFF_HOURS)

        for entry in feed.entries[:max_items]:
            published = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                except Exception:
                    published = None

            if published and published < cutoff:
                continue

            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            if not title:
                continue

            snippet = ""
            if hasattr(entry, "summary"):
                snippet = re.sub(r"<[^>]+>", "", entry.get("summary", "")).strip()[:200]

            items.append({
                "title": title,
                "url": link,
                "source": feed_config["name"],
                "category": feed_config["category"],
                "language": feed_config.get("language", "en"),
                "published": published.isoformat() if published else None,
                "snippet": snippet,
            })

        logger.info(f"{feed_config['name']}: {len(items)}件取得")
        return items

    except Exception as e:
        logger.error(f"{feed_config['name']}: RSS取得エラー: {e}")
        return []


# ===== 公開 API =====

def fetch_google_news() -> list[dict[str, Any]]:
    """Google News RSSキーワード検索で日本語・英語×経済/地政学の4クエリを取得する。

    Returns:
        重複除去済みのニュースアイテムリスト
    """
    all_items: list[dict] = []

    # ① 日本語 経済・金融
    url = _build_google_news_url(JP_ECONOMIC_KEYWORDS, hl="ja", gl="JP", ceid="JP:ja")
    all_items.extend(_parse_google_news_feed(url, "Google News JP 経済", "日本金融", "ja"))

    # ② 日本語 地政学
    url = _build_google_news_url(JP_GEOPOLITICAL_KEYWORDS, hl="ja", gl="JP", ceid="JP:ja")
    all_items.extend(_parse_google_news_feed(url, "Google News JP 地政学", "地政学", "ja"))

    # ③ 英語 経済・金融
    url = _build_google_news_url(EN_ECONOMIC_KEYWORDS, hl="en-US", gl="US", ceid="US:en")
    all_items.extend(_parse_google_news_feed(url, "Google News EN 経済", "米国金融", "en"))

    # ④ 英語 地政学
    url = _build_google_news_url(EN_GEOPOLITICAL_KEYWORDS, hl="en-US", gl="US", ceid="US:en")
    all_items.extend(_parse_google_news_feed(url, "Google News EN 地政学", "地政学", "en"))

    deduped = _deduplicate_items(all_items)
    logger.info(f"Google News取得完了: {len(all_items)}件 → 重複除去後{len(deduped)}件")
    return deduped


def fetch_international_news() -> list[dict[str, Any]]:
    """NHK国際 / BBC World / AP International の直接RSSを取得する。

    各フィードは独立して失敗可能（graceful degradation）。

    Returns:
        重複除去済みのニュースアイテムリスト（地政学カテゴリ）
    """
    all_items: list[dict] = []
    for feed_config in INTERNATIONAL_RSS_FEEDS:
        items = _parse_direct_rss_feed(feed_config, max_items=15)
        all_items.extend(items)

    deduped = _deduplicate_items(all_items)
    logger.info(f"国際RSS取得完了: {len(all_items)}件 → 重複除去後{len(deduped)}件")
    return deduped


def fetch_all_news() -> list[dict[str, Any]]:
    """Google News（4クエリ）＋国際RSS を統合して取得する。

    Returns:
        全ソースをURLベース重複除去した統合リスト
    """
    google_items = fetch_google_news()
    intl_items = fetch_international_news()
    combined = _deduplicate_items(google_items + intl_items)
    logger.info(f"全ニュース統合: {len(combined)}件")
    return combined


def load_news_from_file(target_date: date | None = None) -> list[dict[str, Any]]:
    """data/news/YYYY-MM-DD.json からニュースを読み込む。

    Args:
        target_date: 読み込む日付（Noneなら今日）

    Returns:
        ニュースアイテムリスト（ファイルがなければ空リスト）
    """
    if target_date is None:
        target_date = date.today()

    news_file = NEWS_DATA_DIR / f"{target_date.isoformat()}.json"
    if not news_file.exists():
        return []

    try:
        with open(news_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.error(f"ニュースファイル読み込みエラー ({news_file}): {e}")
        return []


def save_news_to_file(items: list[dict[str, Any]], target_date: date | None = None) -> None:
    """ニュースを data/news/YYYY-MM-DD.json に追記保存する（URL重複除去）。

    Args:
        items: 新規取得したニュースアイテムリスト
        target_date: 保存する日付（Noneなら今日）
    """
    if target_date is None:
        target_date = date.today()

    NEWS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    news_file = NEWS_DATA_DIR / f"{target_date.isoformat()}.json"

    existing = load_news_from_file(target_date)
    existing_urls = {item.get("url") for item in existing if item.get("url")}

    new_items = [
        item for item in items
        if item.get("url") not in existing_urls or not item.get("url")
    ]
    merged = existing + new_items

    try:
        with open(news_file, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        logger.info(
            f"ニュース保存: {news_file} "
            f"({len(existing)}件既存 + {len(new_items)}件追加 = {len(merged)}件)"
        )
    except Exception as e:
        logger.error(f"ニュースファイル保存エラー ({news_file}): {e}")


def fetch_and_save_news(target_date: date | None = None) -> list[dict[str, Any]]:
    """全ソース（Google News + 国際RSS）からニュースを取得してJSONに保存する。

    --news-only モードから呼び出す。

    Returns:
        保存後の全ニュースアイテムリスト
    """
    if target_date is None:
        target_date = date.today()

    items = fetch_all_news()
    save_news_to_file(items, target_date)
    return load_news_from_file(target_date)


def fetch_news_headlines(
    sources_config: dict | None = None,
    target_date: date | None = None,
) -> dict[str, Any]:
    """ニュースヘッドラインを取得する。

    優先順: data/news/YYYY-MM-DD.json → fetch_all_news() → 直接RSS（フォールバック）

    Args:
        sources_config: trusted_sources.yaml の内容（フォールバック用）
        target_date: 対象日付（Noneなら今日）

    Returns:
        {
            "top5": [...],      # 表示用TOP5
            "all_items": [...], # 全件
            "error": bool,
            "source": str,      # "json" | "google" | "fallback" | "none"
        }
    """
    if target_date is None:
        target_date = date.today()

    # [1] 蓄積JSONから読み込み
    stored = load_news_from_file(target_date)
    if stored:
        logger.info(f"蓄積JSONからニュース読み込み: {len(stored)}件")
        return {"top5": _select_top5(stored), "all_items": stored, "error": False, "source": "json"}

    # [2] Google News + 国際RSSをリアルタイム取得
    logger.info("蓄積JSONなし → Google News + 国際RSSをリアルタイム取得")
    live_items = fetch_all_news()
    if live_items:
        logger.info(f"リアルタイム取得成功: {len(live_items)}件")
        return {"top5": _select_top5(live_items), "all_items": live_items, "error": False, "source": "google"}

    # [3] フォールバック: trusted_sources.yaml の直接RSSフィード
    if sources_config:
        logger.warning("リアルタイム取得失敗 → 直接RSS フォールバック")
        max_items = sources_config.get("max_items_per_feed", 10)
        fallback_items: list[dict] = []
        for feed_config in sources_config.get("rss_feeds", []):
            fallback_items.extend(_parse_direct_rss_feed(feed_config, max_items))

        if fallback_items:
            return {
                "top5": _select_top5(fallback_items, sources_config),
                "all_items": fallback_items,
                "error": False,
                "source": "fallback",
            }

    logger.error("全ニュース取得方法が失敗")
    return {"top5": [], "all_items": [], "error": True, "source": "none"}


def _select_top5(items: list[dict], sources_config: dict | None = None) -> list[dict]:
    """ニュースリストからTOP5を選別する。

    優先順: 米国金融 > 日本金融 > 地政学。
    地政学ニュースを確保するため、最低1件は地政学を含めるよう調整する。
    """
    if not items:
        return []

    priority_categories = DEFAULT_PRIORITY_CATEGORIES.copy()
    if sources_config:
        cfg_prio = sources_config.get("priority_categories", [])
        # 地政学が設定になければ末尾に追加
        if "地政学" not in cfg_prio:
            cfg_prio = cfg_prio + ["地政学"]
        priority_categories = cfg_prio

    priority_map = {cat: i for i, cat in enumerate(priority_categories)}

    def sort_key(item: dict) -> tuple:
        return (priority_map.get(item.get("category", ""), 999),)

    sorted_items = sorted(items, key=sort_key)
    top5 = sorted_items[:5]

    # 地政学が1件も入っていない場合、6位以降から地政学を1件補充（差し替え）
    has_geopolitical = any(i.get("category") == "地政学" for i in top5)
    if not has_geopolitical and len(sorted_items) > 5:
        for candidate in sorted_items[5:]:
            if candidate.get("category") == "地政学":
                top5[-1] = candidate  # 最後の1件を地政学に差し替え
                break

    return top5
