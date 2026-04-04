"""
market_data.py — 株価・指数データ取得（yfinance）

Phase 1: US/JP主要指数6本のOHLCV + 前日比変化率取得
"""

import logging
import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# 最大リトライ回数
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance 1.x の MultiIndex カラムをフラット化する。"""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.droplevel(level=1, axis=1)
    return df


def _fetch_with_retry(ticker: str, period: str = "5d") -> pd.DataFrame | None:
    """yfinanceでデータ取得（リトライ付き）。失敗時はNoneを返す。"""
    for attempt in range(MAX_RETRIES):
        try:
            data = yf.download(ticker, period=period, progress=False, auto_adjust=True)
            if data is not None and not data.empty:
                return _normalize_df(data)
            logger.warning(f"{ticker}: empty data (attempt {attempt+1})")
        except Exception as e:
            logger.warning(f"{ticker}: fetch error (attempt {attempt+1}): {e}")
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)
    logger.error(f"{ticker}: 全{MAX_RETRIES}回のリトライに失敗")
    return None


def _calc_change(df: pd.DataFrame) -> dict[str, float | None]:
    """終値と前日比変化率を計算する。"""
    try:
        close = df["Close"].iloc[-1]
        prev_close = df["Close"].iloc[-2] if len(df) >= 2 else None
        if close is None or (isinstance(close, float) and pd.isna(close)):
            return {"close": None, "change_pct": None}
        if prev_close is not None and not pd.isna(prev_close) and prev_close != 0:
            change_pct = (float(close) - float(prev_close)) / float(prev_close) * 100
        else:
            change_pct = None
        return {"close": float(close), "change_pct": change_pct}
    except Exception as e:
        logger.error(f"変化率計算エラー: {e}")
        return {"close": None, "change_pct": None}


def _signal(change_pct: float | None, positive_threshold: float = 0.0) -> str:
    """変化率から信号機絵文字を返す。"""
    if change_pct is None:
        return "⚪"
    if change_pct > positive_threshold:
        return "🟢"
    elif change_pct < -positive_threshold:
        return "🔴"
    return "🟡"


def fetch_us_indices(tickers: list[dict]) -> list[dict[str, Any]]:
    """米国主要指数データを取得する。

    Args:
        tickers: settings.yamlのtickers.us_indices

    Returns:
        各指数の辞書リスト。取得失敗時はerror=Trueを含む。
    """
    results = []
    for t in tickers:
        ticker = t["ticker"]
        # 直近データ（前日比計算用）
        df_short = _fetch_with_retry(ticker, period="5d")
        # 長期データ（テクニカル分析用・100日分）
        df_long = _fetch_with_retry(ticker, period="120d")
        if df_short is None:
            results.append({
                "ticker": ticker,
                "name": t["name"],
                "display_name": t["display_name"],
                "close": None,
                "change_pct": None,
                "signal": "⚪",
                "error": True,
            })
            continue
        vals = _calc_change(df_short)
        results.append({
            "ticker": ticker,
            "name": t["name"],
            "display_name": t["display_name"],
            "close": vals["close"],
            "change_pct": vals["change_pct"],
            "signal": _signal(vals["change_pct"]),
            "error": False,
            "_df": df_long if df_long is not None else df_short,  # テクニカル分析用
        })
        logger.info(f"{ticker}: {vals['close']:.2f} ({vals['change_pct']:+.2f}%)" if vals["close"] else f"{ticker}: 取得失敗")
    return results


def fetch_jp_indices(tickers: list[dict]) -> list[dict[str, Any]]:
    """日本主要指数データを取得する。"""
    return fetch_us_indices(tickers)  # 同一ロジック


def fetch_all_market_data(settings: dict) -> dict[str, Any]:
    """全市場データを取得して返す。

    Returns:
        {
            "us_indices": [...],
            "jp_indices": [...],
            "fetch_date": date,
        }
    """
    logger.info("市場データ取得開始")
    us = fetch_us_indices(settings["tickers"]["us_indices"])
    jp = fetch_jp_indices(settings["tickers"]["jp_indices"])
    return {
        "us_indices": us,
        "jp_indices": jp,
        "fetch_date": date.today(),
    }
