"""
technical.py — テクニカル分析（pandas-ta使用）

Phase 1: 信号機🟢🟡🔴のみ。チャート生成はPhase 2。
計算指標: SMA(5/25/75), RSI(14), MACD(12,26,9), BB(20,2σ), Volume(20MA比)
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
        logger.warning("pandas-ta が利用できません。テクニカル分析をスキップします。")


def _sma_signal(close: float, sma5: float | None, sma25: float | None, sma75: float | None) -> str:
    """移動平均線パーフェクトオーダーで信号機を返す。"""
    if sma5 is None or sma25 is None or sma75 is None:
        return "⚪"
    if close > sma5 > sma25 > sma75:
        return "🟢"
    elif close < sma5 < sma25 < sma75:
        return "🔴"
    return "🟡"


def _rsi_signal(rsi: float | None) -> str:
    """RSI水準から信号機を返す。"""
    if rsi is None:
        return "⚪"
    if rsi >= 70:
        return "🔴"  # 買われすぎ
    elif rsi <= 30:
        return "🟢"  # 売られすぎ（逆張り視点では買い）
    return "🟡"


def _macd_signal(macd: float | None, macd_signal: float | None) -> str:
    """MACDとシグナルラインの関係から信号機を返す。"""
    if macd is None or macd_signal is None:
        return "⚪"
    if macd > macd_signal:
        return "🟢"
    elif macd < macd_signal:
        return "🔴"
    return "🟡"


def _bb_signal(close: float, bb_upper: float | None, bb_lower: float | None, bb_mid: float | None) -> str:
    """ボリンジャーバンド位置から信号機を返す。"""
    if bb_upper is None or bb_lower is None or bb_mid is None:
        return "⚪"
    if close > bb_upper:
        return "🔴"  # 上バンド突破（過熱）
    elif close < bb_lower:
        return "🟢"  # 下バンド割れ（売られすぎ）
    return "🟡"


def _volume_signal(volume: float | None, vol_ma20: float | None) -> str:
    """出来高の20日移動平均比から信号機を返す。"""
    if volume is None or vol_ma20 is None or vol_ma20 == 0:
        return "⚪"
    ratio = volume / vol_ma20
    if ratio >= 1.5:
        return "🟢"  # 平均の1.5倍以上: 注目
    elif ratio <= 0.5:
        return "🟡"  # 平均の0.5倍以下: 低調
    return "🟡"


def _safe_last(series: pd.Series | None) -> float | None:
    """Seriesの最終値を安全に取得する。"""
    if series is None:
        return None
    try:
        val = series.iloc[-1]
        if pd.isna(val):
            return None
        return float(val)
    except Exception:
        return None


def analyze_index(df: pd.DataFrame, name: str) -> dict[str, Any]:
    """単一指数のテクニカル分析を実行する。

    Args:
        df: yfinanceで取得したOHLCVデータ（100日分推奨）
        name: 表示名

    Returns:
        テクニカル指標と信号機の辞書
    """
    result: dict[str, Any] = {
        "name": name,
        "sma": {"signal": "⚪", "sma5": None, "sma25": None, "sma75": None},
        "rsi": {"signal": "⚪", "value": None},
        "macd": {"signal": "⚪", "macd": None, "macd_signal": None, "histogram": None},
        "bb": {"signal": "⚪", "upper": None, "mid": None, "lower": None},
        "volume": {"signal": "⚪", "ratio": None},
        "error": False,
    }

    if not PANDAS_TA_AVAILABLE:
        result["error"] = True
        return result

    try:
        if df is None or len(df) < 30:
            logger.warning(f"{name}: データ不足（{len(df) if df is not None else 0}行）")
            result["error"] = True
            return result

        close = df["Close"]
        volume = df.get("Volume")

        # SMA
        sma5 = _safe_last(ta.sma(close, length=5))
        sma25 = _safe_last(ta.sma(close, length=25))
        sma75 = _safe_last(ta.sma(close, length=75))
        current_close = float(close.iloc[-1])
        result["sma"] = {
            "signal": _sma_signal(current_close, sma5, sma25, sma75),
            "sma5": sma5,
            "sma25": sma25,
            "sma75": sma75,
        }

        # RSI(14)
        rsi_series = ta.rsi(close, length=14)
        rsi_val = _safe_last(rsi_series)
        result["rsi"] = {
            "signal": _rsi_signal(rsi_val),
            "value": rsi_val,
        }

        # MACD(12,26,9)
        macd_df = ta.macd(close, fast=12, slow=26, signal=9)
        if macd_df is not None and not macd_df.empty:
            macd_cols = macd_df.columns.tolist()
            macd_val = _safe_last(macd_df[macd_cols[0]])
            macd_sig = _safe_last(macd_df[macd_cols[2]] if len(macd_cols) >= 3 else None)
            macd_hist = _safe_last(macd_df[macd_cols[1]] if len(macd_cols) >= 2 else None)
        else:
            macd_val = macd_sig = macd_hist = None
        result["macd"] = {
            "signal": _macd_signal(macd_val, macd_sig),
            "macd": macd_val,
            "macd_signal": macd_sig,
            "histogram": macd_hist,
        }

        # Bollinger Bands(20, 2σ)
        bb_df = ta.bbands(close, length=20, std=2)
        if bb_df is not None and not bb_df.empty:
            bb_cols = bb_df.columns.tolist()
            bb_lower = _safe_last(bb_df[bb_cols[0]])
            bb_mid = _safe_last(bb_df[bb_cols[1]])
            bb_upper = _safe_last(bb_df[bb_cols[2]])
        else:
            bb_lower = bb_mid = bb_upper = None
        result["bb"] = {
            "signal": _bb_signal(current_close, bb_upper, bb_lower, bb_mid),
            "upper": bb_upper,
            "mid": bb_mid,
            "lower": bb_lower,
        }

        # Volume vs 20MA
        if volume is not None and not volume.empty:
            vol_ma20 = _safe_last(ta.sma(volume, length=20))
            current_vol = _safe_last(volume)
            ratio = current_vol / vol_ma20 if (vol_ma20 and vol_ma20 != 0 and current_vol) else None
            result["volume"] = {
                "signal": _volume_signal(current_vol, vol_ma20),
                "ratio": ratio,
                "current": current_vol,
                "ma20": vol_ma20,
            }

        logger.info(f"{name}: テクニカル分析完了")

    except Exception as e:
        logger.error(f"{name}: テクニカル分析エラー: {e}")
        result["error"] = True

    return result


def analyze_all_indices(market_data: dict) -> dict[str, list[dict]]:
    """全指数のテクニカル分析を実行する。

    Args:
        market_data: fetch_all_market_data()の戻り値

    Returns:
        {"us_technical": [...], "jp_technical": [...]}
    """
    us_results = []
    for idx in market_data.get("us_indices", []):
        df = idx.get("_df")
        if df is not None:
            tech = analyze_index(df, idx["display_name"])
        else:
            tech = {"name": idx["display_name"], "error": True}
        us_results.append(tech)

    jp_results = []
    for idx in market_data.get("jp_indices", []):
        df = idx.get("_df")
        if df is not None:
            tech = analyze_index(df, idx["display_name"])
        else:
            tech = {"name": idx["display_name"], "error": True}
        jp_results.append(tech)

    return {"us_technical": us_results, "jp_technical": jp_results}
