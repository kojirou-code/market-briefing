"""
futures_commodities.py — 先物・コモディティ・為替・債券指標取得

Phase 1: VIX, 米10年債, USD/JPY, 日経先物, S&P先物, 原油, 金
"""

import logging
import time
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance 1.x の MultiIndex カラムをフラット化する。"""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.droplevel(level=1, axis=1)
    return df


def _fetch_latest(ticker: str) -> dict[str, float | None]:
    """ティッカーの最新値と前日比を取得する。"""
    for attempt in range(MAX_RETRIES):
        try:
            data = yf.download(ticker, period="5d", progress=False, auto_adjust=True)
            if data is not None and not data.empty and len(data) >= 1:
                data = _normalize_df(data)
                close = float(data["Close"].iloc[-1])
                if len(data) >= 2:
                    prev = float(data["Close"].iloc[-2])
                    change_pct = (close - prev) / prev * 100 if prev != 0 else None
                else:
                    change_pct = None
                return {"value": close, "change_pct": change_pct, "error": False}
        except Exception as e:
            logger.warning(f"{ticker}: attempt {attempt+1} failed: {e}")
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)
    logger.error(f"{ticker}: 全リトライ失敗")
    return {"value": None, "change_pct": None, "error": True}


def _vix_signal(vix_value: float | None) -> str:
    """VIX水準に応じた信号機を返す。"""
    if vix_value is None:
        return "⚪"
    if vix_value < 15:
        return "🟢"
    elif vix_value < 20:
        return "🟡"
    elif vix_value < 30:
        return "🟠"
    return "🔴"


def fetch_indicators(tickers: list[dict]) -> list[dict[str, Any]]:
    """VIX, 米10年債, USD/JPY を取得する。"""
    results = []
    for t in tickers:
        data = _fetch_latest(t["ticker"])
        entry = {
            "ticker": t["ticker"],
            "name": t["name"],
            "display_name": t["display_name"],
            **data,
        }
        if t["name"] == "VIX":
            entry["signal"] = _vix_signal(data["value"])
        results.append(entry)
        if data["value"] is not None:
            logger.info(f"{t['ticker']}: {data['value']:.4f}")
    return results


def fetch_futures_commodities(tickers: list[dict]) -> list[dict[str, Any]]:
    """先物・コモディティデータを取得する。"""
    results = []
    for t in tickers:
        data = _fetch_latest(t["ticker"])
        results.append({
            "ticker": t["ticker"],
            "name": t["name"],
            "display_name": t["display_name"],
            **data,
        })
        if data["value"] is not None:
            logger.info(f"{t['ticker']}: {data['value']:.2f}")
    return results


def fetch_all_indicators_and_futures(settings: dict) -> dict[str, Any]:
    """指標・先物・コモディティを一括取得する。"""
    logger.info("指標・先物データ取得開始")
    indicators = fetch_indicators(settings["tickers"]["indicators"])
    futures = fetch_futures_commodities(settings["tickers"]["futures_commodities"])

    # 個別アクセス用の辞書も作成
    indicators_by_name = {d["name"]: d for d in indicators}
    futures_by_name = {d["name"]: d for d in futures}

    return {
        "indicators": indicators,
        "futures_commodities": futures,
        "indicators_by_name": indicators_by_name,
        "futures_by_name": futures_by_name,
    }
