"""テスト: gemini_summarizer.py — Gemini APIニュースサマリー生成"""

import json
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.collectors.gemini_summarizer import (
    MAX_API_RETRIES,
    RETRY_DELAY_SEC,
    _format_news_list,
    generate_news_summary,
    save_summary,
    load_summary,
)


# ============================================================
# ニュースリストのフォーマットテスト
# ============================================================

class TestFormatNewsList:
    def _make_item(self, title: str, source: str = "Test", snippet: str = "") -> dict:
        return {
            "title": title,
            "url": "http://example.com",
            "source": source,
            "category": "米国金融",
            "language": "en",
            "published": "2026-04-04T00:00:00+00:00",
            "snippet": snippet,
        }

    def test_basic_format(self):
        """タイトルとソースが含まれる。"""
        items = [self._make_item("Fed keeps rates steady", "Reuters")]
        result = _format_news_list(items)
        assert "Fed keeps rates steady" in result
        assert "Reuters" in result

    def test_snippet_included(self):
        """スニペットがある場合は含まれる。"""
        items = [self._make_item("Test", snippet="Key detail here")]
        result = _format_news_list(items)
        assert "Key detail here" in result

    def test_multiple_items_numbered(self):
        """複数アイテムは番号付きで表示される。"""
        items = [self._make_item(f"News {i}") for i in range(3)]
        result = _format_news_list(items)
        assert "1." in result
        assert "2." in result
        assert "3." in result

    def test_empty_returns_empty_string(self):
        """空リストは空文字列。"""
        result = _format_news_list([])
        assert result == ""


# ============================================================
# サマリー生成のテスト（Gemini APIモック）
# ============================================================

VALID_SUMMARY = {
    "conclusion": "本日は関税リスクに警戒が必要。",
    "political_news": [
        {
            "headline": "米中追加関税",
            "what_happened": "米国が中国製品に25%関税を発表した。",
            "why_important": "日米市場への影響大。",
            "risk_level": "高",
            "source": "Reuters",
        },
        {
            "headline": "日銀声明",
            "what_happened": "日銀が現状維持を決定。",
            "why_important": "円安圧力継続。",
            "risk_level": "中",
            "source": "日経",
        },
        {
            "headline": "地政学リスク",
            "what_happened": "中東情勢が緊張。",
            "why_important": "原油高。",
            "risk_level": "中",
            "source": "AP",
        },
    ],
    "economic_news": [
        {
            "headline": "FOMCタカ派発言",
            "facts": "FF金利据え置き、利下げ時期不透明。",
            "us_market_impact": "金利高止まり。",
            "jp_market_impact": "円安継続。",
            "source": "CNBC",
        },
        {
            "headline": "ISM製造業",
            "facts": "ISM製造業 48.2、予想下回る。",
            "us_market_impact": "景気減速懸念。",
            "jp_market_impact": "輸出株に影響。",
            "source": "Bloomberg",
        },
        {
            "headline": "原油価格上昇",
            "facts": "WTI 80ドル超え。",
            "us_market_impact": "インフレ再燃懸念。",
            "jp_market_impact": "貿易赤字拡大。",
            "source": "Reuters",
        },
    ],
    "market_perspective": {
        "us_summary": "S&P500は上昇も上値重い。",
        "jp_summary": "日経平均は円安で堅調。",
        "us_jp_linkage": "ドル円が鍵。",
    },
}


class TestGenerateNewsSummary:
    def _make_news_items(self) -> list[dict]:
        return [
            {
                "title": f"News {i}", "url": f"http://example.com/{i}",
                "source": "Test", "category": "米国金融",
                "language": "en", "published": None, "snippet": "",
            }
            for i in range(5)
        ]

    def test_returns_summary_on_success(self, monkeypatch):
        """Gemini APIが成功するとサマリーを返す（google.genai.Clientをモック）。"""
        monkeypatch.setenv("GEMINI_API_KEY", "test_key")

        mock_response = MagicMock()
        mock_response.text = json.dumps(VALID_SUMMARY)

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        items = self._make_news_items()

        # generate_news_summary 内で `import google.genai as genai` するので
        # google.genai.Client をパッチする
        with patch("generators.collectors.gemini_summarizer._load_dotenv"):
            with patch("google.genai.Client", return_value=mock_client):
                result = generate_news_summary(items, date(2026, 4, 4))

        assert result is not None
        assert "conclusion" in result
        assert len(result["political_news"]) == 3
        assert len(result["economic_news"]) == 3

    def test_returns_none_when_no_api_key(self, monkeypatch):
        """APIキーがない場合はNoneを返す。"""
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        # _load_dotenv が .env を読む可能性があるため、os.environ を直接チェックするパスをモック
        with patch("generators.collectors.gemini_summarizer._load_dotenv"):
            with patch.dict("os.environ", {}, clear=False):
                import os
                os.environ.pop("GEMINI_API_KEY", None)
                result = generate_news_summary(self._make_news_items())
        assert result is None

    def test_returns_none_on_empty_items(self, monkeypatch):
        """ニュースアイテムが0件の場合はNoneを返す。"""
        monkeypatch.setenv("GEMINI_API_KEY", "test_key")
        result = generate_news_summary([])
        assert result is None

    def test_returns_none_on_invalid_json(self, monkeypatch):
        """GeminiがJSONでない返答をした場合はNoneを返す。"""
        monkeypatch.setenv("GEMINI_API_KEY", "test_key")

        mock_response = MagicMock()
        mock_response.text = "This is not JSON"

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        with patch("generators.collectors.gemini_summarizer._load_dotenv"):
            with patch("google.genai.Client", return_value=mock_client):
                result = generate_news_summary(self._make_news_items())

        assert result is None

    def test_returns_none_on_api_error(self, monkeypatch):
        """API呼び出しエラー時はNoneを返す（パイプライン継続）。"""
        monkeypatch.setenv("GEMINI_API_KEY", "test_key")

        mock_client = MagicMock()
        mock_client.models.generate_content.side_effect = Exception("API error")

        with patch("generators.collectors.gemini_summarizer._load_dotenv"):
            with patch("google.genai.Client", return_value=mock_client):
                with patch("generators.collectors.gemini_summarizer.time.sleep"):
                    result = generate_news_summary(self._make_news_items())

        assert result is None

    def test_retries_max_times_on_api_error(self, monkeypatch):
        """APIエラーで MAX_API_RETRIES 回リトライしてから None を返す。"""
        monkeypatch.setenv("GEMINI_API_KEY", "test_key")

        mock_client = MagicMock()
        mock_client.models.generate_content.side_effect = Exception("503 Service Unavailable")

        with patch("generators.collectors.gemini_summarizer._load_dotenv"):
            with patch("google.genai.Client", return_value=mock_client):
                with patch("generators.collectors.gemini_summarizer.time.sleep") as mock_sleep:
                    result = generate_news_summary(self._make_news_items())

        assert result is None
        # 呼び出し回数: 初回 + MAX_API_RETRIES 回リトライ
        assert mock_client.models.generate_content.call_count == MAX_API_RETRIES + 1
        # sleep は MAX_API_RETRIES 回呼ばれる（最後の失敗後は呼ばない）
        assert mock_sleep.call_count == MAX_API_RETRIES
        mock_sleep.assert_called_with(RETRY_DELAY_SEC)

    def test_succeeds_on_second_attempt(self, monkeypatch):
        """初回失敗・2回目成功でサマリーを返す。"""
        monkeypatch.setenv("GEMINI_API_KEY", "test_key")

        mock_response = MagicMock()
        mock_response.text = json.dumps(VALID_SUMMARY)

        mock_client = MagicMock()
        mock_client.models.generate_content.side_effect = [
            Exception("503 Service Unavailable"),  # 1回目: 失敗
            mock_response,                          # 2回目: 成功
        ]

        with patch("generators.collectors.gemini_summarizer._load_dotenv"):
            with patch("google.genai.Client", return_value=mock_client):
                with patch("generators.collectors.gemini_summarizer.time.sleep") as mock_sleep:
                    result = generate_news_summary(self._make_news_items(), date(2026, 4, 4))

        assert result is not None
        assert "conclusion" in result
        assert mock_client.models.generate_content.call_count == 2
        assert mock_sleep.call_count == 1  # 1回だけ待機

    def test_no_retry_on_json_decode_error(self, monkeypatch):
        """JSONDecodeError ではリトライせず即 None を返す。"""
        monkeypatch.setenv("GEMINI_API_KEY", "test_key")

        mock_response = MagicMock()
        mock_response.text = "not valid json"

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        with patch("generators.collectors.gemini_summarizer._load_dotenv"):
            with patch("google.genai.Client", return_value=mock_client):
                with patch("generators.collectors.gemini_summarizer.time.sleep") as mock_sleep:
                    result = generate_news_summary(self._make_news_items())

        assert result is None
        assert mock_client.models.generate_content.call_count == 1  # リトライなし
        mock_sleep.assert_not_called()


# ============================================================
# サマリーの保存・読み込みテスト
# ============================================================

class TestSummarySaveLoad:
    def test_save_and_load_roundtrip(self, tmp_path, monkeypatch):
        """保存して読み込むと同じデータが返る。"""
        monkeypatch.setattr(
            "generators.collectors.gemini_summarizer.NEWS_DATA_DIR", tmp_path
        )
        target_date = date(2026, 4, 4)
        save_summary(VALID_SUMMARY, target_date)
        loaded = load_summary(target_date)

        assert loaded is not None
        assert loaded["conclusion"] == VALID_SUMMARY["conclusion"]
        assert len(loaded["political_news"]) == 3

    def test_load_returns_none_when_missing(self, tmp_path, monkeypatch):
        """ファイルが存在しない場合はNoneを返す。"""
        monkeypatch.setattr(
            "generators.collectors.gemini_summarizer.NEWS_DATA_DIR", tmp_path
        )
        result = load_summary(date(2099, 1, 1))
        assert result is None

    def test_save_creates_correct_filename(self, tmp_path, monkeypatch):
        """保存ファイル名が summary_YYYY-MM-DD.json 形式。"""
        monkeypatch.setattr(
            "generators.collectors.gemini_summarizer.NEWS_DATA_DIR", tmp_path
        )
        target_date = date(2026, 4, 4)
        save_summary(VALID_SUMMARY, target_date)
        expected_file = tmp_path / "summary_2026-04-04.json"
        assert expected_file.exists()
