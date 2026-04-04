"""
chart_generator.py — matplotlibチャート生成（Phase 2）

6指数 × 60日チャート（ローソク足風ライン + SMA5/25/75 + 出来高）を生成する。
出力先: hugo-site/static/charts/YYYY-MM-DD/{ticker}.png
"""

import logging
import warnings
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# matplotlib はオプション依存
try:
    import matplotlib
    matplotlib.use("Agg")  # GUIなし（headless）
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.gridspec import GridSpec
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    logger.warning("matplotlib が利用できません。チャート生成をスキップします。")

# チャート設定
CHART_DAYS = 60          # 表示日数
FIG_WIDTH = 10           # 幅 (インチ)
FIG_HEIGHT = 6           # 高さ (インチ)
DPI = 100                # 解像度
CHART_STYLE = "dark_background"

# 色定義
COLOR_PRICE = "#4fc3f7"     # メインライン（水色）
COLOR_SMA5 = "#ffb74d"      # SMA5（オレンジ）
COLOR_SMA25 = "#ce93d8"     # SMA25（薄紫）
COLOR_SMA75 = "#ef9a9a"     # SMA75（薄赤）
COLOR_VOLUME = "#546e7a"    # 出来高（グレー）
COLOR_VOL_HOVER = "#78909c" # 出来高強調


def _safe_sma(series: pd.Series, length: int) -> pd.Series | None:
    """移動平均を安全に計算する。"""
    if len(series) < length:
        return None
    return series.rolling(window=length).mean()


def generate_chart(
    df: pd.DataFrame,
    ticker: str,
    display_name: str,
    output_path: Path,
) -> bool:
    """単一指数の60日チャートを生成して保存する。

    Args:
        df: yfinanceのOHLCVデータ（120日以上推奨）
        ticker: ティッカー記号
        display_name: チャートタイトル用表示名
        output_path: 保存先パス (.png)

    Returns:
        成功したら True
    """
    if not MATPLOTLIB_AVAILABLE:
        logger.warning(f"{ticker}: matplotlib未インストール → チャートスキップ")
        return False

    try:
        # 直近60日に絞る
        df_plot = df.tail(CHART_DAYS).copy()
        if len(df_plot) < 10:
            logger.warning(f"{ticker}: データ不足 ({len(df_plot)}日) → スキップ")
            return False

        close = df_plot["Close"]
        volume = df_plot.get("Volume")
        dates = df_plot.index

        # SMA計算
        sma5 = _safe_sma(close, 5)
        sma25 = _safe_sma(close, 25)
        sma75 = _safe_sma(df["Close"], 75)  # 全データで計算してから末尾60日
        if sma75 is not None:
            sma75 = sma75.reindex(df_plot.index)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with plt.style.context(CHART_STYLE):
                has_volume = volume is not None and not volume.empty and volume.sum() > 0

                if has_volume:
                    fig = plt.figure(figsize=(FIG_WIDTH, FIG_HEIGHT))
                    gs = GridSpec(2, 1, height_ratios=[3, 1], hspace=0.05)
                    ax_price = fig.add_subplot(gs[0])
                    ax_vol = fig.add_subplot(gs[1], sharex=ax_price)
                else:
                    fig, ax_price = plt.subplots(figsize=(FIG_WIDTH, FIG_HEIGHT - 1.5))

                # ---- 価格ライン ----
                ax_price.plot(dates, close, color=COLOR_PRICE, linewidth=1.5, label=display_name)

                if sma5 is not None:
                    ax_price.plot(dates, sma5, color=COLOR_SMA5, linewidth=0.8, linestyle="--", label="SMA5", alpha=0.8)
                if sma25 is not None:
                    ax_price.plot(dates, sma25, color=COLOR_SMA25, linewidth=0.8, linestyle="--", label="SMA25", alpha=0.8)
                if sma75 is not None:
                    ax_price.plot(dates, sma75, color=COLOR_SMA75, linewidth=0.8, linestyle="--", label="SMA75", alpha=0.8)

                ax_price.set_title(f"{display_name} — 60日チャート", fontsize=12, pad=8)
                ax_price.legend(loc="upper left", fontsize=8, framealpha=0.3)
                ax_price.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))
                ax_price.grid(True, alpha=0.2)

                if has_volume:
                    # 出来高バー（上昇/下落で色分け）
                    vol_colors = []
                    close_vals = close.values
                    for i, v in enumerate(volume):
                        if i == 0 or close_vals[i] >= close_vals[i - 1]:
                            vol_colors.append("#4caf50")   # 上昇: 緑
                        else:
                            vol_colors.append("#ef5350")   # 下落: 赤

                    ax_vol.bar(dates, volume, color=vol_colors, alpha=0.6, width=0.8)
                    ax_vol.set_ylabel("出来高", fontsize=8)
                    ax_vol.yaxis.set_major_formatter(
                        plt.FuncFormatter(lambda x, _: f"{x/1e6:.0f}M" if x >= 1e6 else f"{x:,.0f}")
                    )
                    ax_vol.grid(True, alpha=0.15)

                    # X軸ラベルは出来高パネルに
                    ax_price.tick_params(labelbottom=False)
                    ax_vol.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
                    ax_vol.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
                    plt.setp(ax_vol.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
                else:
                    ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
                    ax_price.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
                    plt.setp(ax_price.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)

                fig.tight_layout()
                output_path.parent.mkdir(parents=True, exist_ok=True)
                fig.savefig(str(output_path), dpi=DPI, bbox_inches="tight")
                plt.close(fig)

        logger.info(f"{ticker}: チャート生成完了 → {output_path}")
        return True

    except Exception as e:
        logger.error(f"{ticker}: チャート生成エラー: {e}")
        try:
            plt.close("all")
        except Exception:
            pass
        return False


def generate_all_charts(
    market_data: dict[str, Any],
    target_date: date,
    static_dir: Path,
    base_url_path: str = "",
) -> dict[str, str]:
    """全指数のチャートを生成する。

    Args:
        market_data: fetch_all_market_data()の戻り値
        target_date: 記事日付（チャートの保存ディレクトリ名に使用）
        static_dir: hugo-site/static/ ディレクトリ
        base_url_path: HugoのbaseURLのパス部分（例: "/market-briefing"）。
            GitHub Pages のようにサブパスでホストされる場合に必要。
            ローカル開発時はデフォルト "" のままで可。

    Returns:
        {ticker: 絶対URL} の辞書（Hugo テンプレートで使用）
        例（base_url_path="/market-briefing"）:
            {"^GSPC": "/market-briefing/charts/2026-04-07/GSPC.png"}
        例（base_url_path=""）:
            {"^GSPC": "/charts/2026-04-07/GSPC.png"}
    """
    if not MATPLOTLIB_AVAILABLE:
        return {}

    date_str = target_date.strftime("%Y-%m-%d")
    chart_dir = static_dir / "charts" / date_str
    chart_urls: dict[str, str] = {}

    all_indices = market_data.get("us_indices", []) + market_data.get("jp_indices", [])

    for idx in all_indices:
        if idx.get("error"):
            continue
        df = idx.get("_df")
        if df is None:
            continue

        ticker = idx["ticker"]
        # ファイル名から特殊文字を除去（^N225 → N225）
        safe_ticker = ticker.replace("^", "").replace("=", "").replace(".", "_")
        output_path = chart_dir / f"{safe_ticker}.png"
        # base_url_path を先頭に付与してブラウザが正しいパスを解決できるようにする
        url = f"{base_url_path}/charts/{date_str}/{safe_ticker}.png"

        success = generate_chart(
            df=df,
            ticker=ticker,
            display_name=idx["display_name"],
            output_path=output_path,
        )
        if success:
            chart_urls[ticker] = url

    logger.info(f"チャート生成完了: {len(chart_urls)}/{len(all_indices)}件")
    return chart_urls
