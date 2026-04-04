"""
article_builder.py — Markdown記事組み立て（Jinja2テンプレート使用）

SPEC.md Section 2 の2層構造を生成する。
Phase 2: セクターETF、チャート、Fear & Greed、方向性スコア追加。
"""

import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

logger = logging.getLogger(__name__)

# テンプレートディレクトリ
TEMPLATE_DIR = Path(__file__).parent.parent / "templates"


def _format_date_title(target_date: date) -> str:
    """記事タイトル用の日付フォーマット。"""
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    wd = weekdays[target_date.weekday()]
    return f"Market Briefing {target_date.year}年{target_date.month}月{target_date.day}日（{wd}）"


def _format_week_label(target_date: date) -> str:
    """週間まとめ用のラベル（例: 2026年4月第1週）。"""
    # その週の月曜日を取得
    monday = target_date - timedelta(days=target_date.weekday())
    friday = monday + timedelta(days=4)
    return (
        f"{monday.year}年{monday.month}月{monday.day}日〜"
        f"{friday.month}月{friday.day}日"
    )


def _compute_week_change(market_data: dict) -> dict:
    """yfinance の _df から週間騰落率を計算してmarket_dataに追加する。"""
    for indices_key in ("us_indices", "jp_indices"):
        for idx in market_data.get(indices_key, []):
            if idx.get("error") or idx.get("_df") is None:
                idx["week_change_pct"] = None
                continue
            df = idx["_df"]
            try:
                if len(df) >= 6:
                    # 直近5営業日の騰落
                    week_close = float(df["Close"].iloc[-1])
                    week_prev = float(df["Close"].iloc[-6])
                    idx["week_change_pct"] = (week_close - week_prev) / week_prev * 100 if week_prev != 0 else None
                else:
                    idx["week_change_pct"] = None
            except Exception:
                idx["week_change_pct"] = None
    return market_data


def build_article(
    market_data: dict,
    indicators_data: dict,
    technical_data: dict,
    alerts: list[dict],
    news_data: dict,
    calendar_events: list[dict],
    target_date: date | None = None,
    sector_data: dict | None = None,
    breadth_data: dict | None = None,
    direction_score: dict | None = None,
    chart_urls: dict[str, dict[str, str]] | None = None,
    news_summary: dict | None = None,
) -> str:
    """全データをまとめてMarkdown記事を生成する。

    Returns:
        Hugoフロントマター付きのMarkdown文字列
    """
    if target_date is None:
        target_date = date.today()

    # Jinja2環境セットアップ
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape([]),  # Markdownなのでエスケープ不要
        trim_blocks=True,
        lstrip_blocks=True,
    )

    template = env.get_template("daily_briefing.md.j2")

    # Fear & Greed を indicators から分離して渡す
    fear_greed = None
    if breadth_data and not breadth_data.get("error"):
        fear_greed = breadth_data.get("fear_greed")

    # テンプレートコンテキスト組み立て
    context: dict[str, Any] = {
        "title": _format_date_title(target_date),
        "date_iso": target_date.strftime("%Y-%m-%dT05:00:00+09:00"),
        "description": f"{target_date.month}月{target_date.day}日の市場概況",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M JST"),

        # 市場データ
        "us_indices": market_data.get("us_indices", []),
        "jp_indices": market_data.get("jp_indices", []),

        # 指標・先物
        "indicators": indicators_data.get("indicators", []),
        "futures_commodities": indicators_data.get("futures_commodities", []),

        # テクニカル
        "us_technical": technical_data.get("us_technical", []),
        "jp_technical": technical_data.get("jp_technical", []),

        # アラート
        "alerts": alerts,

        # ニュース
        "news_top5": news_data.get("top5", []),
        "news_error": news_data.get("error", False),

        # 経済カレンダー
        "calendar_events": calendar_events,

        # Phase 2
        "sector_data": sector_data,
        "fear_greed": fear_greed,
        "direction_score": direction_score,
        "chart_urls": chart_urls or {},

        # Gemini サマリー（あれば TOP5 を置き換えて表示）
        "news_summary": news_summary,
    }

    try:
        content = template.render(**context)
        logger.info(f"記事生成完了: {len(content)}文字")
        return content
    except Exception as e:
        logger.error(f"記事生成エラー: {e}")
        raise


def build_weekly_article(
    market_data: dict,
    indicators_data: dict,
    technical_data: dict,
    news_data: dict,
    calendar_events: list[dict],
    target_date: date | None = None,
    sector_data: dict | None = None,
    breadth_data: dict | None = None,
    direction_score: dict | None = None,
) -> str:
    """週間まとめMarkdown記事を生成する。

    Returns:
        Hugoフロントマター付きのMarkdown文字列
    """
    if target_date is None:
        target_date = date.today()

    # 週間騰落率を計算
    market_data = _compute_week_change(market_data)

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape([]),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    template = env.get_template("weekly_review.md.j2")

    fear_greed = None
    if breadth_data and not breadth_data.get("error"):
        fear_greed = breadth_data.get("fear_greed")

    context: dict[str, Any] = {
        "title": f"週間マーケットレビュー {_format_week_label(target_date)}",
        "date_iso": target_date.strftime("%Y-%m-%dT05:00:00+09:00"),
        "description": f"{_format_week_label(target_date)}の週間市場まとめ",
        "week_label": _format_week_label(target_date),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M JST"),

        "us_indices": market_data.get("us_indices", []),
        "jp_indices": market_data.get("jp_indices", []),
        "indicators": indicators_data.get("indicators", []),

        "news_top5": news_data.get("top5", []),
        "news_error": news_data.get("error", False),
        "calendar_events": calendar_events,

        "sector_data": sector_data,
        "fear_greed": fear_greed,
        "direction_score": direction_score,
    }

    try:
        content = template.render(**context)
        logger.info(f"週間まとめ記事生成完了: {len(content)}文字")
        return content
    except Exception as e:
        logger.error(f"週間まとめ記事生成エラー: {e}")
        raise


def save_article(content: str, output_path: str | Path) -> None:
    """Markdown記事をファイルに保存する。"""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info(f"記事保存: {output_path}")


def get_article_filename(target_date: date) -> str:
    """Hugo記事のファイル名を返す（YYYY-MM-DD.md）。"""
    return target_date.strftime("%Y-%m-%d.md")


def get_weekly_article_filename(target_date: date) -> str:
    """週間まとめ記事のファイル名を返す（YYYY-MM-DD-weekly.md）。"""
    return target_date.strftime("%Y-%m-%d-weekly.md")
