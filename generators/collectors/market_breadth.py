"""
market_breadth.py — 市場ブレデス / Fear & Greed 自作スコア（Phase 2）

CNNのFear & Greed Indexに相当する自作複合スコア（0〜100）を算出する。
使用指標（すべてyfinance取得可能）:
  1. VIX 水準           (0-100: VIX高→Fear、低→Greed)
  2. S&P 500 モメンタム  (直近値 vs SMA125比較)
  3. S&P 500 RSI(14)    (RSI値を0-100にマッピング)
  4. MACD シグナル       (MACD vs Signal lineの乖離)
  5. 52週高値/安値比率   (高値圏にいるほどGreed)

スコア解釈:
  0-24:  Extreme Fear  🔴
  25-44: Fear          🟠
  45-55: Neutral       🟡
  56-74: Greed         🟢
  75-100: Extreme Greed 💚
"""

import logging
import warnings
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# pandas-ta のインポート（NumPy互換性警告を抑制）
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    try:
        import pandas_ta as ta
        PANDAS_TA_AVAILABLE = True
    except ImportError:
        PANDAS_TA_AVAILABLE = False


def _vix_to_score(vix: float | None) -> float | None:
    """VIX水準をGreedスコア(0-100)に変換する。VIX高 → Fear（スコア低）。"""
    if vix is None:
        return None
    if vix <= 12:
        return 90.0
    elif vix <= 15:
        return 75.0
    elif vix <= 18:
        return 60.0
    elif vix <= 20:
        return 50.0
    elif vix <= 25:
        return 35.0
    elif vix <= 30:
        return 20.0
    elif vix <= 40:
        return 10.0
    return 5.0


def _momentum_to_score(close: float | None, sma125: float | None) -> float | None:
    """価格 vs SMA125の乖離率をスコアに変換する。"""
    if close is None or sma125 is None or sma125 == 0:
        return None
    ratio = (close - sma125) / sma125 * 100  # 乖離率(%)
    if ratio >= 10:
        return 90.0
    elif ratio >= 5:
        return 75.0
    elif ratio >= 2:
        return 60.0
    elif ratio >= 0:
        return 52.0
    elif ratio >= -3:
        return 40.0
    elif ratio >= -7:
        return 25.0
    elif ratio >= -12:
        return 15.0
    return 5.0


def _rsi_to_score(rsi: float | None) -> float | None:
    """RSI値をGreedスコアに変換する（RSI高→Greed）。"""
    if rsi is None:
        return None
    # RSI → スコアのシンプルなマッピング
    # 70+(買われすぎ) → 80(Greed), 30-(売られすぎ) → 20(Fear)
    if rsi >= 70:
        return 80.0
    elif rsi >= 60:
        return 65.0
    elif rsi >= 50:
        return 55.0
    elif rsi >= 40:
        return 45.0
    elif rsi >= 30:
        return 30.0
    return 15.0


def _macd_to_score(macd: float | None, signal: float | None, close: float | None) -> float | None:
    """MACD vs シグナルラインの乖離をスコアに変換する。"""
    if macd is None or signal is None or close is None or close == 0:
        return None
    # close で正規化した乖離率
    normalized = (macd - signal) / close * 100
    if normalized >= 0.5:
        return 80.0
    elif normalized >= 0.2:
        return 65.0
    elif normalized >= 0.05:
        return 55.0
    elif normalized >= -0.05:
        return 50.0
    elif normalized >= -0.2:
        return 40.0
    elif normalized >= -0.5:
        return 30.0
    return 15.0


def _highlow_to_score(close: float | None, high52w: float | None, low52w: float | None) -> float | None:
    """52週高値/安値レンジにおける現在値の位置をスコアに変換する。"""
    if close is None or high52w is None or low52w is None:
        return None
    rng = high52w - low52w
    if rng <= 0:
        return 50.0
    position = (close - low52w) / rng  # 0.0(安値圏) 〜 1.0(高値圏)
    return round(position * 100, 1)


def _safe_last(series: pd.Series | None) -> float | None:
    """Seriesの最終値を安全に取得する。"""
    if series is None:
        return None
    try:
        val = series.iloc[-1]
        return None if pd.isna(val) else float(val)
    except Exception:
        return None


def _calc_fear_greed_score(
    sp500_df: pd.DataFrame | None,
    vix_value: float | None,
) -> dict[str, Any]:
    """Fear & Greed スコアを計算する。

    Args:
        sp500_df: S&P 500の長期データ（^GSPC、120日以上推奨）
        vix_value: 最新VIX値

    Returns:
        {
            "score": float,          # 0-100
            "label": str,            # "Extreme Fear" 等
            "emoji": str,            # 絵文字
            "components": {...},     # 各コンポーネントのスコア
            "error": bool,
        }
    """
    scores: dict[str, float | None] = {}

    # 1. VIXスコア
    scores["vix"] = _vix_to_score(vix_value)

    if sp500_df is not None and len(sp500_df) >= 30 and PANDAS_TA_AVAILABLE:
        close = sp500_df["Close"]
        current_close = _safe_last(close)

        # 2. モメンタム（SMA125比較）
        if len(sp500_df) >= 125:
            sma125 = _safe_last(ta.sma(close, length=125))
            scores["momentum"] = _momentum_to_score(current_close, sma125)
        else:
            sma_len = min(len(sp500_df) - 1, 60)
            sma = _safe_last(ta.sma(close, length=sma_len))
            scores["momentum"] = _momentum_to_score(current_close, sma)

        # 3. RSI(14)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rsi = _safe_last(ta.rsi(close, length=14))
        scores["rsi"] = _rsi_to_score(rsi)

        # 4. MACD(12,26,9)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            macd_df = ta.macd(close, fast=12, slow=26, signal=9)
        if macd_df is not None and not macd_df.empty:
            cols = macd_df.columns.tolist()
            macd_val = _safe_last(macd_df[cols[0]])
            macd_sig = _safe_last(macd_df[cols[2]] if len(cols) >= 3 else None)
            scores["macd"] = _macd_to_score(macd_val, macd_sig, current_close)
        else:
            scores["macd"] = None

        # 5. 52週高値/安値
        if len(sp500_df) >= 252:
            window_252 = close.iloc[-252:]
        else:
            window_252 = close
        high52 = float(window_252.max()) if not window_252.empty else None
        low52 = float(window_252.min()) if not window_252.empty else None
        scores["highlow"] = _highlow_to_score(current_close, high52, low52)
    else:
        scores["momentum"] = None
        scores["rsi"] = None
        scores["macd"] = None
        scores["highlow"] = None

    # 有効なスコアの平均
    valid = [v for v in scores.values() if v is not None]
    if not valid:
        return {
            "score": None,
            "label": "データ不足",
            "emoji": "⚪",
            "components": scores,
            "error": True,
        }

    total_score = round(sum(valid) / len(valid), 1)
    label, emoji = _score_to_label(total_score)

    logger.info(
        f"Fear & Greed スコア: {total_score} ({label}) "
        f"[VIX={scores.get('vix')}, mom={scores.get('momentum')}, "
        f"rsi={scores.get('rsi')}, macd={scores.get('macd')}, "
        f"hl={scores.get('highlow')}]"
    )

    return {
        "score": total_score,
        "label": label,
        "emoji": emoji,
        "components": scores,
        "error": False,
    }


def _score_to_label(score: float) -> tuple[str, str]:
    """スコアをラベルと絵文字に変換する。"""
    if score < 25:
        return "Extreme Fear", "🔴"
    elif score < 45:
        return "Fear", "🟠"
    elif score <= 55:
        return "Neutral", "🟡"
    elif score <= 74:
        return "Greed", "🟢"
    return "Extreme Greed", "💚"


def fetch_market_breadth(
    market_data: dict[str, Any],
    indicators_data: dict[str, Any],
) -> dict[str, Any]:
    """市場ブレデス指標を計算して返す。

    Args:
        market_data: fetch_all_market_data()の戻り値
        indicators_data: fetch_all_indicators_and_futures()の戻り値

    Returns:
        {
            "fear_greed": {...},   # Fear & Greed スコア
            "error": bool,
        }
    """
    # S&P 500 の長期データを取得
    sp500_df = None
    for idx in market_data.get("us_indices", []):
        if idx.get("name") == "S&P 500" or idx.get("ticker") == "^GSPC":
            sp500_df = idx.get("_df")
            break

    # VIX値
    vix_value = None
    ind_by_name = indicators_data.get("indicators_by_name", {})
    vix_data = ind_by_name.get("VIX", {})
    if not vix_data.get("error"):
        vix_value = vix_data.get("value")

    fear_greed = _calc_fear_greed_score(sp500_df, vix_value)

    return {
        "fear_greed": fear_greed,
        "error": fear_greed.get("error", False),
    }
