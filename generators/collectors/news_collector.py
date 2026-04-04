"""
news_collector.py — ニュースRSS取得（Phase 1: ヘッドライン + URL）

ホワイトリストのみ取得。ファクトチェックはPhase 2以降。
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import feedparser

logger = logging.getLogger(__name__)


def _parse_feed(feed_config: dict, max_items: int = 10) -> list[dict[str, Any]]:
    """単一RSSフィードを取得してパースする。失敗時は空リストを返す。"""
    try:
        feed = feedparser.parse(feed_config["url"])
        if feed.bozo and not feed.entries:
            logger.warning(f"RSS parse error for {feed_config['name']}: {feed.bozo_exception}")
            return []

        items = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        for entry in feed.entries[:max_items]:
            # 公開日時の取得（なければ現在時刻）
            published = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                except Exception:
                    published = None

            # 24時間以内の記事のみ（取得できない場合は含める）
            if published and published < cutoff:
                continue

            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            if not title:
                continue

            items.append({
                "title": title,
                "url": link,
                "source": feed_config["name"],
                "category": feed_config["category"],
                "language": feed_config.get("language", "en"),
                "published": published.isoformat() if published else None,
            })

        logger.info(f"{feed_config['name']}: {len(items)}件取得")
        return items

    except Exception as e:
        logger.error(f"{feed_config['name']}: RSS取得エラー: {e}")
        return []


def fetch_news_headlines(sources_config: dict) -> dict[str, Any]:
    """全RSSフィードからヘッドラインを取得し、TOP5を選別する。

    Args:
        sources_config: trusted_sources.yamlの内容

    Returns:
        {
            "top5": [...],      # 表示用TOP5
            "all_items": [...], # 全件
            "error": bool,      # 全滅フラグ
        }
    """
    max_items = sources_config.get("max_items_per_feed", 10)
    all_items: list[dict] = []

    for feed_config in sources_config.get("rss_feeds", []):
        items = _parse_feed(feed_config, max_items)
        all_items.extend(items)

    if not all_items:
        logger.error("全RSSフィード取得失敗")
        return {"top5": [], "all_items": [], "error": True}

    # 優先カテゴリ順にソート（同カテゴリ内は取得順）
    priority = sources_config.get("priority_categories", [])
    priority_map = {cat: i for i, cat in enumerate(priority)}

    def sort_key(item: dict) -> tuple:
        cat_priority = priority_map.get(item["category"], 999)
        return (cat_priority,)

    sorted_items = sorted(all_items, key=sort_key)
    top5 = sorted_items[:5]

    logger.info(f"ニュース取得完了: 全{len(all_items)}件 → TOP5選別")
    return {"top5": top5, "all_items": all_items, "error": False}
