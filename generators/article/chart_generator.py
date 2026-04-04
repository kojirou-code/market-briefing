"""
chart_generator.py — mplfinance ローソク足チャート生成（Phase 2 改修）

各指数について日次（1年）・週次（3年）の2枚を横並び表示用に生成する。
出力先: hugo-site/static/charts/YYYY-MM-DD/{ticker}_daily.png
         hugo-site/static/charts/YYYY-MM-DD/{ticker}_weekly.png
"""

import logging
import warnings
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ===== ライブラリ可用性フラグ =====
MPLFINANCE_AVAILABLE = False
MATPLOTLIB_AVAILABLE = False  # 後方互換エイリアス（テストの skipif で使用）
JP_FONT_NAME = "DejaVu Sans"

try:
    import matplotlib
    matplotlib.use("Agg")  # GUI なし（headless）
    import matplotlib.pyplot as plt
    import mplfinance as mpf

    MPLFINANCE_AVAILABLE = True
    MATPLOTLIB_AVAILABLE = True

    # 日本語フォント検出
    def _detect_jp_font() -> str:
        """利用可能な日本語フォント名を返す。見つからなければ DejaVu Sans。"""
        try:
            from matplotlib import font_manager
            available = {f.name for f in font_manager.fontManager.ttflist}
            for name in ("Hiragino Sans", "Noto Sans JP", "YuGothic"):
                if name in available:
                    return name
        except Exception:
            pass
        return "DejaVu Sans"

    JP_FONT_NAME = _detect_jp_font()

    # ===== mplfinance ダークスタイル定義 =====
    _MC = mpf.make_marketcolors(
        up="#26a69a",     # 陽線: teal green
        down="#ef5350",   # 陰線: red
        edge="inherit",
        wick="inherit",
        volume={"up": "#26a69a", "down": "#ef5350"},
    )
    MPFSTYLE = mpf.make_mpf_style(
        base_mpf_style="nightclouds",
        marketcolors=_MC,
        facecolor="#1e1e1e",
        figcolor="#1e1e1e",
        gridcolor="#333333",
        gridstyle="--",
        gridaxis="both",
        y_on_right=False,
    )

except ImportError:
    MPLFINANCE_AVAILABLE = False
    MATPLOTLIB_AVAILABLE = False
    logger.warning("mplfinance が利用できません。チャート生成をスキップします。")


# ===== チャート定数 =====
MIN_DAILY_ROWS = 80    # SMA75 に必要な最小行数（75）+ バッファ
MIN_WEEKLY_ROWS = 55   # SMA52 に必要な最小行数（52）+ バッファ

FIG_WIDTH_INCHES = 6   # ~600px at DPI=100
FIG_HEIGHT_INCHES = 4.5
FIG_HEIGHT_WEEKLY = 4.5
DPI = 100

# SMA 色
COLOR_SMA5  = "#ffb74d"   # オレンジ（短期）
COLOR_SMA25 = "#ce93d8"   # 薄紫（中期）
COLOR_SMA75 = "#ef9a9a"   # 薄赤（長期）


# ===== 後方互換シム =====
def _safe_sma(series: pd.Series, length: int) -> pd.Series | None:
    """後方互換性のため残す。内部では未使用。"""
    if len(series) < length:
        return None
    return series.rolling(window=length).mean()


# ===== 内部ヘルパー =====
def _build_sma_addplots(
    close: pd.Series,
    lengths_colors: list[tuple[int, str]],
) -> list:
    """SMA の make_addplot リストを生成する。

    全値 NaN の SMA は make_addplot に渡すと ValueError になるため除外する。
    """
    addplots = []
    for length, color in lengths_colors:
        sma = close.rolling(length).mean()
        if sma.notna().any():
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                addplots.append(
                    mpf.make_addplot(
                        sma, color=color, width=0.8, linestyle="--", alpha=0.85
                    )
                )
    return addplots


# ===== 公開 API =====

def generate_chart_pair(
    df_daily: pd.DataFrame | None,
    df_weekly: pd.DataFrame | None,
    ticker: str,
    display_name: str,
    chart_dir: Path,
    safe_ticker: str,
) -> dict[str, Path | None]:
    """単一指数の日次・週次ローソク足チャートを生成して保存する。

    Args:
        df_daily:     日次OHLCVデータ（period="1y", interval="1d"）
        df_weekly:    週次OHLCVデータ（period="3y", interval="1wk"）
        ticker:       ティッカー記号（ログ用）
        display_name: チャートタイトル用表示名
        chart_dir:    保存先ディレクトリ（YYYY-MM-DD/）
        safe_ticker:  ファイル名用の安全なティッカー（^N225 → N225）

    Returns:
        {"daily": Path|None, "weekly": Path|None}
    """
    if not MPLFINANCE_AVAILABLE:
        return {"daily": None, "weekly": None}

    result: dict[str, Path | None] = {"daily": None, "weekly": None}

    # --- 日次チャート ---
    if df_daily is not None and len(df_daily) >= MIN_DAILY_ROWS:
        try:
            ap = _build_sma_addplots(
                df_daily["Close"],
                [(5, COLOR_SMA5), (25, COLOR_SMA25), (75, COLOR_SMA75)],
            )
            # mplfinance のバリデーターは isinstance(value, bool) を要求する。
            # pandas/numpy の bool_ は Python bool のサブクラスではないため、
            # 必ず bool() でキャストする。
            has_vol = bool(
                "Volume" in df_daily.columns
                and df_daily["Volume"].notna().any()
                and df_daily["Volume"].sum() > 0
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                fig, axes = mpf.plot(
                    df_daily,
                    type="candle",
                    style=MPFSTYLE,
                    addplot=ap if ap else None,
                    volume=has_vol,
                    figsize=(FIG_WIDTH_INCHES, FIG_HEIGHT_INCHES),
                    returnfig=True,
                    datetime_format="%m/%d",
                    xrotation=30,
                )
            axes[0].set_title(
                f"{display_name}（日次・1年）",
                fontname=JP_FONT_NAME,
                fontsize=11,
                color="white",
                pad=6,
            )
            chart_dir.mkdir(parents=True, exist_ok=True)
            daily_path = chart_dir / f"{safe_ticker}_daily.png"
            fig.savefig(
                str(daily_path), dpi=DPI, bbox_inches="tight",
                facecolor=fig.get_facecolor(),
            )
            plt.close(fig)
            result["daily"] = daily_path
            logger.info(f"{ticker}: 日次チャート生成完了 → {daily_path.name}")
        except Exception as e:
            logger.error(f"{ticker}: 日次チャート生成エラー: {e}")
            try:
                plt.close("all")
            except Exception:
                pass
    elif df_daily is not None:
        logger.warning(f"{ticker}: 日次データ不足 ({len(df_daily)}行 < {MIN_DAILY_ROWS}) → スキップ")

    # --- 週次チャート ---
    if df_weekly is not None and len(df_weekly) >= MIN_WEEKLY_ROWS:
        try:
            ap = _build_sma_addplots(
                df_weekly["Close"],
                [(13, COLOR_SMA5), (26, COLOR_SMA25), (52, COLOR_SMA75)],
            )
            # numpy.bool_ は Python bool のサブクラスではないため bool() でキャスト
            has_vol = bool(
                "Volume" in df_weekly.columns
                and df_weekly["Volume"].notna().any()
                and df_weekly["Volume"].sum() > 0
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                fig, axes = mpf.plot(
                    df_weekly,
                    type="candle",
                    style=MPFSTYLE,
                    addplot=ap if ap else None,
                    volume=has_vol,
                    figsize=(FIG_WIDTH_INCHES, FIG_HEIGHT_WEEKLY),
                    returnfig=True,
                    datetime_format="%Y/%m",
                    xrotation=30,
                )
            axes[0].set_title(
                f"{display_name}（週次・3年）",
                fontname=JP_FONT_NAME,
                fontsize=11,
                color="white",
                pad=6,
            )
            chart_dir.mkdir(parents=True, exist_ok=True)
            weekly_path = chart_dir / f"{safe_ticker}_weekly.png"
            fig.savefig(
                str(weekly_path), dpi=DPI, bbox_inches="tight",
                facecolor=fig.get_facecolor(),
            )
            plt.close(fig)
            result["weekly"] = weekly_path
            logger.info(f"{ticker}: 週次チャート生成完了 → {weekly_path.name}")
        except Exception as e:
            logger.error(f"{ticker}: 週次チャート生成エラー: {e}")
            try:
                plt.close("all")
            except Exception:
                pass
    elif df_weekly is not None:
        logger.warning(f"{ticker}: 週次データ不足 ({len(df_weekly)}行 < {MIN_WEEKLY_ROWS}) → スキップ")

    return result


def generate_all_charts(
    market_data: dict[str, Any],
    chart_data: dict[str, dict[str, pd.DataFrame | None]],
    target_date: date,
    static_dir: Path,
    base_url_path: str = "",
) -> dict[str, dict[str, str]]:
    """全指数の日次・週次ローソク足チャートを生成する。

    Args:
        market_data:    fetch_all_market_data()の戻り値（error フラグ確認用）
        chart_data:     fetch_chart_data()の戻り値（OHLCVデータ）
        target_date:    記事日付（チャート保存ディレクトリ名）
        static_dir:     hugo-site/static/ ディレクトリ
        base_url_path:  HugoのbaseURLパス部分（例: "/market-briefing"）

    Returns:
        {ticker: {"daily": url, "weekly": url}} の辞書。
        生成できなかったキーは欠落する。

        例（base_url_path="/market-briefing"）:
            {
                "^GSPC": {
                    "daily":  "/market-briefing/charts/2026-04-07/GSPC_daily.png",
                    "weekly": "/market-briefing/charts/2026-04-07/GSPC_weekly.png",
                },
            }
    """
    if not MPLFINANCE_AVAILABLE:
        return {}

    date_str = target_date.strftime("%Y-%m-%d")
    chart_dir = static_dir / "charts" / date_str
    chart_urls: dict[str, dict[str, str]] = {}

    all_indices = market_data.get("us_indices", []) + market_data.get("jp_indices", [])

    for idx in all_indices:
        if idx.get("error"):
            continue
        ticker = idx["ticker"]
        safe_ticker = ticker.replace("^", "").replace("=", "").replace(".", "_")

        ticker_data = chart_data.get(ticker, {})
        paths = generate_chart_pair(
            df_daily=ticker_data.get("daily"),
            df_weekly=ticker_data.get("weekly"),
            ticker=ticker,
            display_name=idx["display_name"],
            chart_dir=chart_dir,
            safe_ticker=safe_ticker,
        )

        url_pair: dict[str, str] = {}
        if paths["daily"] is not None:
            url_pair["daily"] = (
                f"{base_url_path}/charts/{date_str}/{safe_ticker}_daily.png"
            )
        if paths["weekly"] is not None:
            url_pair["weekly"] = (
                f"{base_url_path}/charts/{date_str}/{safe_ticker}_weekly.png"
            )
        if url_pair:
            chart_urls[ticker] = url_pair

    daily_cnt = sum(1 for p in chart_urls.values() if "daily" in p)
    weekly_cnt = sum(1 for p in chart_urls.values() if "weekly" in p)
    logger.info(f"チャート生成完了: 日次{daily_cnt}件 / 週次{weekly_cnt}件")
    return chart_urls
