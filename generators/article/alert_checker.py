"""
alert_checker.py — 異常値アラート判定（Phase 1）

SPEC.md Section 5 の閾値に基づくアラート生成。
"""

import logging
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def _load_thresholds(config_path: str) -> dict:
    """alert_thresholds.yamlを読み込む。"""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"アラート閾値設定読み込みエラー: {e}")
        return {
            "vix_surge_pct": 20.0,
            "index_crash_pct": -2.0,
            "index_surge_pct": 2.0,
            "usdjpy_move_pct": 1.5,
        }


def check_alerts(
    market_data: dict,
    indicators_data: dict,
    thresholds_path: str,
    jpx_breadth_data: dict | None = None,
) -> list[dict[str, Any]]:
    """全データをチェックしてアラートリストを返す。

    Args:
        market_data:      fetch_all_market_data() の戻り値
        indicators_data:  fetch_all_indicators_and_futures() の戻り値
        thresholds_path:  alert_thresholds.yaml のパス
        jpx_breadth_data: fetch_jpx_market_breadth() の戻り値（Phase 2、省略可）

    Returns:
        [
            {"level": "warning"|"info", "emoji": str, "message": str},
            ...
        ]
    """
    thresholds = _load_thresholds(thresholds_path)
    alerts: list[dict[str, Any]] = []

    vix_surge = thresholds.get("vix_surge_pct", 20.0)
    index_crash = thresholds.get("index_crash_pct", -2.0)
    index_surge = thresholds.get("index_surge_pct", 2.0)
    usdjpy_move = thresholds.get("usdjpy_move_pct", 1.5)

    # VIX アラート
    vix_data = indicators_data.get("indicators_by_name", {}).get("VIX", {})
    vix_val = vix_data.get("value")
    vix_change = vix_data.get("change_pct")
    if vix_val is not None and vix_change is not None:
        if vix_change >= vix_surge:
            alerts.append({
                "level": "warning",
                "emoji": "⚠️",
                "message": f"VIX {vix_change:+.1f}% ({vix_val:.1f}) — 警戒水準",
            })
            logger.warning(f"VIXアラート: {vix_change:+.1f}%")

    # 米国指数アラート
    for idx in market_data.get("us_indices", []):
        change = idx.get("change_pct")
        name = idx.get("display_name", idx.get("name", ""))
        if change is None:
            continue
        if change <= index_crash:
            alerts.append({
                "level": "warning",
                "emoji": "⚠️",
                "message": f"{name} {change:+.2f}% — 急落",
            })
        elif change >= index_surge:
            alerts.append({
                "level": "info",
                "emoji": "📈",
                "message": f"{name} {change:+.2f}% — 急騰",
            })

    # 日本指数アラート
    for idx in market_data.get("jp_indices", []):
        change = idx.get("change_pct")
        name = idx.get("display_name", idx.get("name", ""))
        if change is None:
            continue
        if change <= index_crash:
            alerts.append({
                "level": "warning",
                "emoji": "⚠️",
                "message": f"{name} {change:+.2f}% — 急落",
            })
        elif change >= index_surge:
            alerts.append({
                "level": "info",
                "emoji": "📈",
                "message": f"{name} {change:+.2f}% — 急騰",
            })

    # USD/JPY アラート
    usdjpy_data = indicators_data.get("indicators_by_name", {}).get("USDJPY", {})
    usdjpy_change = usdjpy_data.get("change_pct")
    if usdjpy_change is not None:
        if abs(usdjpy_change) >= usdjpy_move:
            direction = "円急騰" if usdjpy_change < 0 else "円急落"
            alerts.append({
                "level": "warning",
                "emoji": "⚠️",
                "message": f"USD/JPY {usdjpy_change:+.2f}% — {direction}",
            })

    # 騰落レシオアラート（Phase 2: jpx_breadth_data が渡された場合のみ）
    if jpx_breadth_data and not jpx_breadth_data.get("error"):
        ad_ratio = jpx_breadth_data.get("advance_decline_ratio")
        ad_high = thresholds.get("advance_decline_ratio_high", 130.0)
        ad_low = thresholds.get("advance_decline_ratio_low", 70.0)
        if ad_ratio is not None:
            if ad_ratio >= ad_high:
                alerts.append({
                    "level": "warning",
                    "emoji": "⚠️",
                    "message": f"騰落レシオ {ad_ratio:.1f} — 過熱圏（{ad_high:.0f}超）",
                })
                logger.warning(f"騰落レシオアラート(過熱): {ad_ratio:.1f}")
            elif ad_ratio <= ad_low:
                alerts.append({
                    "level": "info",
                    "emoji": "📉",
                    "message": f"騰落レシオ {ad_ratio:.1f} — 底値圏（{ad_low:.0f}未満）",
                })
                logger.info(f"騰落レシオアラート(底値): {ad_ratio:.1f}")

    logger.info(f"アラートチェック完了: {len(alerts)}件")
    return alerts
