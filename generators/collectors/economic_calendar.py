"""
economic_calendar.py — 経済指標カレンダー読み込み（Phase 1: YAML手動管理）

Phase 2以降で自動取得に移行予定。
"""

import logging
from datetime import date, datetime
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def load_upcoming_events(config_path: str, today: date | None = None) -> list[dict[str, Any]]:
    """YAMLから直近イベントを読み込む。

    Args:
        config_path: economic_events.yamlのパス
        today: 基準日（Noneなら今日）

    Returns:
        直近N日以内のイベントリスト（display_max_events件まで）
    """
    if today is None:
        today = date.today()

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"経済カレンダー読み込みエラー: {e}")
        return []

    events = config.get("events", [])
    days_ahead = config.get("display_days_ahead", 14)
    max_events = config.get("display_max_events", 5)

    upcoming = []
    for event in events:
        try:
            event_date = datetime.strptime(str(event["date"]), "%Y-%m-%d").date()
            delta = (event_date - today).days
            if 0 <= delta <= days_ahead:
                upcoming.append({
                    "date": event_date,
                    "date_str": event_date.strftime("%-m/%-d"),
                    "date_full": event_date.strftime("%Y-%m-%d"),
                    "weekday": ["月", "火", "水", "木", "金", "土", "日"][event_date.weekday()],
                    "event": event["event"],
                    "country": event.get("country", ""),
                    "importance": event.get("importance", "medium"),
                    "days_until": delta,
                })
        except Exception as e:
            logger.warning(f"イベント解析エラー ({event}): {e}")
            continue

    # 日付順ソート
    upcoming.sort(key=lambda x: x["date"])
    result = upcoming[:max_events]

    logger.info(f"経済カレンダー: {len(result)}件のイベントを返す（今日から{days_ahead}日以内）")
    return result
