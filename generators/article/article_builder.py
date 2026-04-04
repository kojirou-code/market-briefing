"""
article_builder.py — Markdown記事組み立て（Jinja2テンプレート使用）

SPEC.md Section 2 の2層構造を生成する。
"""

import logging
import os
from datetime import date, datetime
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


def build_article(
    market_data: dict,
    indicators_data: dict,
    technical_data: dict,
    alerts: list[dict],
    news_data: dict,
    calendar_events: list[dict],
    target_date: date | None = None,
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
    }

    try:
        content = template.render(**context)
        logger.info(f"記事生成完了: {len(content)}文字")
        return content
    except Exception as e:
        logger.error(f"記事生成エラー: {e}")
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
