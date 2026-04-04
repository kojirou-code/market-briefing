"""
sector_etf.py — 米国セクターETF騰落率取得（Phase 2）

11セクターETFの前日比変化率を取得し、ランキング形式で返す。
ティッカー: XLK, XLF, XLV, XLY, XLP, XLE, XLU, XLI, XLB, XLRE, XLC
"""

import logging
import time
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5

# セクターETF定義（ticker → 日本語名）
SECTOR_ETF_MAP: dict[str, str] = {
    "XLK": "情報技術",
    "XLF": "金融",
    "XLV": "ヘルスケア",
    "XLY": "一般消費財",
    "XLP": "生活必需品",
    "XLE": "エネルギー",
    "XLU": "公益事業",
    "XLI": "資本財",
    "XLB": "素材",
    "XLRE": "不動産",
    "XLC": "通信サービス",
}


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance 1.x の MultiIndex カラムをフラット化する。"""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.droplevel(level=1, axis=1)
    return df


def _fetch_sector_etfs(tickers: list[str]) -> dict[str, dict[str, Any]]:
    """複数ティッカーを一括取得して各銘柄の変化率を返す。"""
    results: dict[str, dict[str, Any]] = {}

    for attempt in range(MAX_RETRIES):
        try:
            tickers_str = " ".join(tickers)
            data = yf.download(tickers_str, period="5d", progress=False, auto_adjust=True)
            if data is None or data.empty:
                logger.warning(f"sector ETF: empty data (attempt {attempt + 1})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                continue

            # MultiIndexの場合 (複数ティッカー)
            if isinstance(data.columns, pd.MultiIndex):
                close_df = data["Close"]
            else:
                # 単一ティッカーのフォールバック
                close_df = data[["Close"]]
                close_df.columns = tickers

            for ticker in tickers:
                if ticker not in close_df.columns:
                    results[ticker] = {"value": None, "change_pct": None, "error": True}
                    continue
                series = close_df[ticker].dropna()
                if len(series) < 2:
                    results[ticker] = {"value": None, "change_pct": None, "error": True}
                    continue
                close = float(series.iloc[-1])
                prev = float(series.iloc[-2])
                change_pct = (close - prev) / prev * 100 if prev != 0 else None
                results[ticker] = {"value": close, "change_pct": change_pct, "error": False}

            logger.info(f"sector ETF: {len(results)}/{len(tickers)} 取得成功")
            return results

        except Exception as e:
            logger.warning(f"sector ETF: attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    # 全失敗時
    for ticker in tickers:
        if ticker not in results:
            results[ticker] = {"value": None, "change_pct": None, "error": True}
    return results


def fetch_sector_etfs(tickers: list[str] | None = None) -> dict[str, Any]:
    """セクターETF騰落率を取得し、ランキング順で返す。

    Args:
        tickers: 対象ティッカーリスト（Noneの場合は全11セクター）

    Returns:
        {
            "sectors": [
                {
                    "ticker": "XLK",
                    "name": "情報技術",
                    "value": 195.23,
                    "change_pct": 1.23,
                    "rank": 1,
                    "signal": "🟢",
                    "error": False,
                },
                ...
            ],
            "top3": [...],      # 上位3セクター
            "bottom3": [...],   # 下位3セクター
            "error": False,
        }
    """
    if tickers is None:
        tickers = list(SECTOR_ETF_MAP.keys())

    raw = _fetch_sector_etfs(tickers)

    sectors = []
    for ticker in tickers:
        r = raw.get(ticker, {"value": None, "change_pct": None, "error": True})
        sectors.append({
            "ticker": ticker,
            "name": SECTOR_ETF_MAP.get(ticker, ticker),
            "value": r["value"],
            "change_pct": r["change_pct"],
            "signal": _change_signal(r["change_pct"]),
            "error": r["error"],
        })

    # 変化率でソート（取得失敗は末尾）
    sorted_sectors = sorted(
        sectors,
        key=lambda x: (x["change_pct"] is None, -(x["change_pct"] or 0)),
    )
    for i, s in enumerate(sorted_sectors):
        s["rank"] = i + 1

    # 元の順序（11セクター順）でも持っておく
    valid = [s for s in sorted_sectors if not s["error"]]
    error_count = sum(1 for s in sectors if s["error"])

    result = {
        "sectors": sorted_sectors,
        "top3": sorted_sectors[:3] if sorted_sectors else [],
        "bottom3": sorted_sectors[-3:] if len(sorted_sectors) >= 3 else sorted_sectors,
        "error": error_count == len(tickers),
    }

    logger.info(
        f"セクターETF取得完了: {len(valid)}/{len(tickers)}件成功"
    )
    return result


def _change_signal(change_pct: float | None) -> str:
    """騰落率から信号機絵文字を返す。"""
    if change_pct is None:
        return "⚪"
    if change_pct >= 1.0:
        return "🟢"
    elif change_pct <= -1.0:
        return "🔴"
    return "🟡"
