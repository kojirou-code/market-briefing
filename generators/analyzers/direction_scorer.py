"""
direction_scorer.py — 方向性推定スコア（Phase 2）

テクニカル + 需給（Fear & Greed）を統合して市場の方向性スコアを算出する。

スコア体系（-10 〜 +10）:
  テクニカル（最大 ±6 点）:
    SMA パーフェクトオーダー: +2/-2
    RSI 水準:                 +1/-1
    MACD クロス:              +2/-2
    BB 位置:                  +1/-1
  Fear & Greed（最大 ±4 点）:
    Extreme Greed (75+):     +4
    Greed (55-74):           +2
    Neutral (45-54):          0
    Fear (25-44):            -2
    Extreme Fear (<25):      -4

最終判定:
  +7 〜 +10: 強気 🟢🟢
  +3 〜 +6:  やや強気 🟢
  -2 〜 +2:  中立 🟡
  -6 〜 -3:  やや弱気 🔴
  -10 〜 -7: 弱気 🔴🔴
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _signal_to_score(signal: str, positive: int = 2, negative: int = -2) -> int:
    """テクニカル信号機をスコアに変換する。"""
    if signal == "🟢":
        return positive
    elif signal == "🔴":
        return negative
    return 0  # 🟡 / ⚪


def _fear_greed_to_score(fg_score: float | None) -> int:
    """Fear & Greed スコアを方向性スコアに変換する。"""
    if fg_score is None:
        return 0
    if fg_score >= 75:
        return 4
    elif fg_score >= 55:
        return 2
    elif fg_score >= 45:
        return 0
    elif fg_score >= 25:
        return -2
    return -4


def _score_to_label(score: int) -> tuple[str, str]:
    """スコアをラベルと絵文字に変換する。"""
    if score >= 7:
        return "強気", "🟢🟢"
    elif score >= 3:
        return "やや強気", "🟢"
    elif score >= -2:
        return "中立", "🟡"
    elif score >= -6:
        return "やや弱気", "🔴"
    return "弱気", "🔴🔴"


def _score_technical(tech: dict[str, Any]) -> dict[str, int]:
    """単一指数のテクニカル指標からスコアを計算する。"""
    if tech.get("error"):
        return {"total": 0, "sma": 0, "rsi": 0, "macd": 0, "bb": 0}

    sma_score = _signal_to_score(tech.get("sma", {}).get("signal", "🟡"), positive=2, negative=-2)
    rsi_score = _signal_to_score(tech.get("rsi", {}).get("signal", "🟡"), positive=1, negative=-1)
    macd_score = _signal_to_score(tech.get("macd", {}).get("signal", "🟡"), positive=2, negative=-2)
    bb_score = _signal_to_score(tech.get("bb", {}).get("signal", "🟡"), positive=1, negative=-1)

    return {
        "sma": sma_score,
        "rsi": rsi_score,
        "macd": macd_score,
        "bb": bb_score,
        "total": sma_score + rsi_score + macd_score + bb_score,
    }


def calculate_direction_score(
    technical_data: dict[str, Any],
    breadth_data: dict[str, Any],
) -> dict[str, Any]:
    """市場の方向性スコアを算出する。

    Args:
        technical_data: analyze_all_indices()の戻り値
        breadth_data: fetch_market_breadth()の戻り値

    Returns:
        {
            "us": {
                "score": int,           # 合計スコア (-10〜+10)
                "label": str,           # "強気" 等
                "emoji": str,           # "🟢" 等
                "tech_score": int,      # テクニカル部分 (-6〜+6)
                "fg_score": int,        # F&G部分 (-4〜+4)
                "breakdown": {...},     # 各指数のスコア
            },
            "jp": {...},
            "overall": {...},           # 米国+日本の平均
        }
    """
    fg_score_raw = breadth_data.get("fear_greed", {}).get("score")
    fg_contribution = _fear_greed_to_score(fg_score_raw)
    fg_label = breadth_data.get("fear_greed", {}).get("label", "不明")

    # 米国テクニカルスコア（S&P 500 + NASDAQ + SOX の平均）
    us_techs = technical_data.get("us_technical", [])
    us_tech_scores = [_score_technical(t) for t in us_techs]
    if us_tech_scores:
        us_tech_total = round(sum(s["total"] for s in us_tech_scores) / len(us_tech_scores))
    else:
        us_tech_total = 0

    us_total = us_tech_total + fg_contribution
    us_total = max(-10, min(10, us_total))  # クランプ
    us_label, us_emoji = _score_to_label(us_total)

    # 日本テクニカルスコア
    jp_techs = technical_data.get("jp_technical", [])
    jp_tech_scores = [_score_technical(t) for t in jp_techs]
    if jp_tech_scores:
        jp_tech_total = round(sum(s["total"] for s in jp_tech_scores) / len(jp_tech_scores))
    else:
        jp_tech_total = 0

    jp_total = jp_tech_total + fg_contribution
    jp_total = max(-10, min(10, jp_total))
    jp_label, jp_emoji = _score_to_label(jp_total)

    # 総合（米国+日本の平均）
    overall_total = round((us_total + jp_total) / 2)
    overall_label, overall_emoji = _score_to_label(overall_total)

    # 各指数の内訳
    us_breakdown = {
        t.get("name", f"US{i}"): s
        for i, (t, s) in enumerate(zip(us_techs, us_tech_scores))
    }
    jp_breakdown = {
        t.get("name", f"JP{i}"): s
        for i, (t, s) in enumerate(zip(jp_techs, jp_tech_scores))
    }

    logger.info(
        f"方向性スコア: 米国={us_total}({us_label}) "
        f"日本={jp_total}({jp_label}) "
        f"総合={overall_total}({overall_label}) "
        f"[F&G={fg_score_raw} → {fg_contribution}点]"
    )

    return {
        "us": {
            "score": us_total,
            "label": us_label,
            "emoji": us_emoji,
            "tech_score": us_tech_total,
            "fg_score": fg_contribution,
            "breakdown": us_breakdown,
        },
        "jp": {
            "score": jp_total,
            "label": jp_label,
            "emoji": jp_emoji,
            "tech_score": jp_tech_total,
            "fg_score": fg_contribution,
            "breakdown": jp_breakdown,
        },
        "overall": {
            "score": overall_total,
            "label": overall_label,
            "emoji": overall_emoji,
        },
        "fg_label": fg_label,
        "fg_raw": fg_score_raw,
    }
