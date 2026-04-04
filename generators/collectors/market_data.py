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


def _trim_price_discontinuity(df: pd.DataFrame, threshold: float = 0.50) -> pd.DataFrame:
    """価格の不連続点を検出し、連続した価格系列に正規化する。

    yfinanceの誤データやETF再編・株式分割等による急激な価格不連続（±50%超の単日変化）
    を検出し、以下の戦略で対処する:

    1. 不連続点以降に十分なデータ（10行以上）がある場合:
       → 不連続点以降のデータのみを返す（古い誤スケールのデータを切り捨て）

    2. 不連続点以降が少ない場合（スプリット直後等、10行未満）:
       → 不連続点より前のデータを新スケールに比率換算して正規化した上で
         全データを返す（Close/Open/High/Lowを調整）

    例: 1306.T (TOPIX ETF) で直近5日のみが ~386 円、それ以前が ~3700 円と
    なる yfinance のスプリット未反映データを正規化できる。

    Args:
        df: OHLCVデータ（Closeカラム必須）
        threshold: 不連続と判定する単日変化率の絶対値（デフォルト: 0.50 = 50%超）

    Returns:
        正規化済みデータ（不連続なければ元のDataFrameをそのまま返す）
    """
    if len(df) < 10:
        return df

    pct_change = df["Close"].pct_change().abs()
    large_jumps = pct_change[pct_change > threshold]

    if large_jumps.empty:
        return df

    last_jump_idx = large_jumps.index[-1]
    loc = df.index.get_loc(last_jump_idx)
    trimmed = df.iloc[loc:]

    if len(trimmed) >= 10:
        # ケース1: 不連続点以降のデータが十分 → そのまま切り捨て
        logger.info(
            f"価格不連続点を検出: {last_jump_idx.date()} に{pct_change[last_jump_idx]:.1%}の変化。"
            f"直近{len(trimmed)}行のデータを使用（全{len(df)}行）。"
        )
        return trimmed

    # ケース2: 不連続点後のデータが不足（スプリット直後等）
    # → 不連続点より前のデータを新スケールにスケール調整して返す
    old_price = float(df["Close"].iloc[loc - 1]) if loc > 0 else 0.0
    new_price = float(df["Close"].iloc[loc])

    if old_price <= 0:
        logger.warning(
            f"価格不連続点（{pct_change[last_jump_idx]:.1%}）のスケール調整不可"
            f"（旧価格={old_price}）。元のデータを使用。"
        )
        return df

    ratio = new_price / old_price
    logger.info(
        f"価格不連続点を検出（直近{len(trimmed)}行のみ正常）: {last_jump_idx.date()} に"
        f"{pct_change[last_jump_idx]:.1%}の変化（比率={ratio:.4f}）。"
        f"全{len(df)}行を新スケールに調整。"
    )

    df_scaled = df.copy()
    pre_jump_mask = df_scaled.index < last_jump_idx
    # Close・OHLC をスケール調整（Volumeは調整しない）
    for col in ["Close", "Open", "High", "Low"]:
        if col in df_scaled.columns:
            df_scaled.loc[pre_jump_mask, col] = (
                df_scaled.loc[pre_jump_mask, col] * ratio
            )

    return df_scaled


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
        # 長期データ（テクニカル分析 + 前日比計算を同一DataFrameで行い価格スケールを統一する）
        # ※ 短期(5d)と長期(120d)を別途取得すると auto_adjust の配当調整基準が異なり、
        #   SMAと close が乖離する問題（ETF代替指数で顕著）が生じるため廃止
        df_long = _fetch_with_retry(ticker, period="120d")
        if df_long is None:
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
        # 価格不連続点（yfinance誤データ・ETF再編等）を除去
        df_long = _trim_price_discontinuity(df_long)
        vals = _calc_change(df_long)
        results.append({
            "ticker": ticker,
            "name": t["name"],
            "display_name": t["display_name"],
            "close": vals["close"],
            "change_pct": vals["change_pct"],
            "signal": _signal(vals["change_pct"]),
            "error": False,
            "_df": df_long,  # テクニカル分析用（close と同一スケール）
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
