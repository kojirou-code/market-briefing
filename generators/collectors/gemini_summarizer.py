"""
gemini_summarizer.py — Gemini APIによるニュースサマリー生成

24時間分の蓄積ニュースを Gemini 2.5 Flash に投げ、
構造化サマリー（結論・政治3本・経済3本・日米視点）を生成する。

APIキー: .env の GEMINI_API_KEY（python-dotenvで読み込み）
使用モデル: gemini-2.5-flash（無料枠）
出力: data/news/summary_YYYY-MM-DD.json に保存
"""

import json
import logging
import os
import time
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent
NEWS_DATA_DIR = _PROJECT_ROOT / "data" / "news"

GEMINI_MODEL = "gemini-2.5-flash"
MAX_API_RETRIES = 3       # 最大リトライ回数
RETRY_DELAY_SEC = 30      # リトライ間隔（秒）

SUMMARY_PROMPT_TEMPLATE = """\
あなたは日本在住の個人投資家向けに、日米マーケットの連動を前提として \
政治・経済・金融・地政学ニュースを整理するプロフェッショナルな市場分析アシスタントです。

以下のニュース一覧（過去24時間）を分析し、構造化サマリーを作成してください。

【出力フォーマット】

以下のキーを持つJSONオブジェクトとして出力してください（他のテキストは含めないこと）:

{{
  "conclusion": "日米市場を踏まえた本日の結論（3行以内。今日は何に警戒/注目すべきかが即座にわかる表現）",
  "political_news": [
    {{
      "headline": "見出し（30字以内）",
      "what_happened": "何が起きたか（2〜3行）",
      "why_important": "なぜ重要か（日本・米国への影響）",
      "risk_level": "高 or 中 or 低",
      "source": "引用元"
    }}
  ],
  "economic_news": [
    {{
      "headline": "見出し（30字以内）",
      "facts": "事実整理（数値・指標を明記）",
      "us_market_impact": "米国市場への影響",
      "jp_market_impact": "日本市場への波及経路",
      "source": "引用元"
    }}
  ],
  "market_perspective": {{
    "us_summary": "米国市場サマリー（株・金利・ドルの方向性）",
    "jp_summary": "日本市場サマリー（株・為替・金利の方向性）",
    "us_jp_linkage": "日米連動チェックポイント"
  }}
}}

【ルール】
- political_newsは3本、economic_newsは3本（少なくとも1本は金融政策・金利関連）
- political_newsのうち少なくとも1本は地政学・安全保障関連（中東/ロシア・ウクライナ/中国・台湾/米国外交のいずれか）を含めること。該当ニュースがない場合のみ省略可。
- 一次情報を最優先する
- 憶測、ゴシップ、感情的表現は排除する
- 事実と解釈を分け、断定は避ける
- 個別銘柄の投資助言は行わない
- 日本語で出力する
- JSONのみ出力し、```json などのマークダウンコードブロックは使わない

【ニュース一覧】
{news_list}
"""


def _load_dotenv() -> None:
    """プロジェクトルートの .env から環境変数を読み込む。"""
    env_path = _PROJECT_ROOT / ".env"
    if env_path.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
        except ImportError:
            # python-dotenv なしで手動読み込み
            with open(env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, _, value = line.partition("=")
                        os.environ.setdefault(key.strip(), value.strip())


def _format_news_list(news_items: list[dict]) -> str:
    """ニュースアイテムをプロンプト用テキストにフォーマットする。"""
    lines = []
    for i, item in enumerate(news_items, 1):
        title = item.get("title", "")
        snippet = item.get("snippet", "")
        source = item.get("source", "")
        published = item.get("published", "")[:10] if item.get("published") else ""

        line = f"{i}. [{source}] {title}"
        if published:
            line += f" ({published})"
        if snippet:
            line += f"\n   {snippet}"
        lines.append(line)

    return "\n".join(lines)


def generate_news_summary(
    news_items: list[dict[str, Any]],
    target_date: date | None = None,
) -> dict[str, Any] | None:
    """Gemini APIを使ってニュースサマリーを生成する。

    Args:
        news_items: fetch_news_headlines()等で取得したニュースリスト
        target_date: 対象日付（保存ファイル名に使用）

    Returns:
        サマリー辞書（生成失敗時はNone）
    """
    if target_date is None:
        target_date = date.today()

    _load_dotenv()
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY が設定されていません。サマリー生成をスキップ。")
        return None

    if not news_items:
        logger.warning("ニュースアイテムが0件。サマリー生成をスキップ。")
        return None

    news_list_text = _format_news_list(news_items)
    prompt = SUMMARY_PROMPT_TEMPLATE.format(news_list=news_list_text)

    for attempt in range(MAX_API_RETRIES + 1):
        try:
            import google.genai as genai

            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
            )

            raw_text = response.text.strip()

            # JSONブロック除去（```json ... ``` の場合）
            if raw_text.startswith("```"):
                lines = raw_text.split("\n")
                raw_text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

            summary = json.loads(raw_text)
            logger.info(f"Geminiサマリー生成完了: {len(str(summary))}文字")
            return summary

        except json.JSONDecodeError as e:
            logger.error(f"Gemini出力のJSONパース失敗: {e}\n出力: {raw_text[:200]}")
            return None  # JSONエラーはリトライしない

        except Exception as e:
            if attempt < MAX_API_RETRIES:
                logger.warning(
                    f"Gemini APIエラー（リトライ {attempt + 1}/{MAX_API_RETRIES}、"
                    f"{RETRY_DELAY_SEC}秒後に再試行）: {e}"
                )
                time.sleep(RETRY_DELAY_SEC)
            else:
                logger.error(
                    f"Gemini API呼び出しエラー（{MAX_API_RETRIES}回リトライ失敗）: {e}"
                )

    return None


def save_summary(summary: dict[str, Any], target_date: date | None = None) -> None:
    """サマリーを data/news/summary_YYYY-MM-DD.json に保存する。"""
    if target_date is None:
        target_date = date.today()

    NEWS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    summary_file = NEWS_DATA_DIR / f"summary_{target_date.isoformat()}.json"

    try:
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        logger.info(f"サマリー保存: {summary_file}")
    except Exception as e:
        logger.error(f"サマリー保存エラー: {e}")


def load_summary(target_date: date | None = None) -> dict[str, Any] | None:
    """data/news/summary_YYYY-MM-DD.json からサマリーを読み込む。

    Returns:
        サマリー辞書（ファイルがなければNone）
    """
    if target_date is None:
        target_date = date.today()

    summary_file = NEWS_DATA_DIR / f"summary_{target_date.isoformat()}.json"
    if not summary_file.exists():
        return None

    try:
        with open(summary_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"サマリー読み込みエラー ({summary_file}): {e}")
        return None


def generate_and_save_summary(
    news_items: list[dict[str, Any]],
    target_date: date | None = None,
) -> dict[str, Any] | None:
    """サマリーを生成してファイルに保存する（ワンストップ関数）。

    Args:
        news_items: ニュースアイテムリスト
        target_date: 対象日付

    Returns:
        サマリー辞書（失敗時はNone）
    """
    summary = generate_news_summary(news_items, target_date)
    if summary:
        save_summary(summary, target_date)
    return summary
