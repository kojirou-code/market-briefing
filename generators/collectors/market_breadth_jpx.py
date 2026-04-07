"""
market_breadth_jpx.py — 騰落レシオ・新高値新安値取得（Phase 2）

取得戦略（graceful degradation）:
  1. kabutanスクレイピング（騰落レシオ・新高値・新安値）
  2. 前回キャッシュを使用

データ保存先: data/market_breadth_jpx/YYYY-MM-DD.json
              data/market_breadth_jpx/latest.json（フォールバック用）

騰落レシオ解釈:
  130超: 過熱圏（売られすぎ警戒）
  70未満: 底値圏（買われすぎ警戒）
  70-130: 通常範囲
"""

import json
import logging
import re
import time
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5
TIMEOUT = 10

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "market_breadth_jpx"

# kabutanの市場情報ページ
KABUTAN_MARKET_URL = "https://kabutan.jp/info/market/"
KABUTAN_HIGHLOW_URL = "https://kabutan.jp/info/highlow/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}


def _ad_ratio_signal(ratio: float | None) -> str:
    """騰落レシオからシグナル文字列を返す。

    Args:
        ratio: 騰落レシオ（%）

    Returns:
        "🔴 過熱圏" | "🟢 底値圏" | "🟡 通常" | "⚪"
    """
    if ratio is None:
        return "⚪"
    if ratio >= 130:
        return "🔴 過熱圏"
    elif ratio < 70:
        return "🟢 底値圏"
    return "🟡 通常"


def _save_cache(data: dict[str, Any], target_date: date) -> None:
    """キャッシュを保存する（日付別 + latest の2つ）。"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    dated_path = CACHE_DIR / f"{target_date.strftime('%Y-%m-%d')}.json"
    latest_path = CACHE_DIR / "latest.json"
    payload = {"date": target_date.strftime("%Y-%m-%d"), **data}
    for path in (dated_path, latest_path):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"キャッシュ保存失敗 ({path.name}): {e}")


def _load_cache() -> dict[str, Any] | None:
    """最新キャッシュを読み込む。存在しなければ None。"""
    latest_path = CACHE_DIR / "latest.json"
    if not latest_path.exists():
        return None
    try:
        with open(latest_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"JPX需給キャッシュ読み込み: {data.get('date', '不明')}")
        return data
    except Exception as e:
        logger.warning(f"キャッシュ読み込み失敗: {e}")
        return None


def _parse_ad_ratio(soup: "BeautifulSoup", text: str) -> float | None:
    """kabutanページから騰落レシオ（25日）を抽出する。"""
    # パターン1: テキストから正規表現で抽出
    patterns = [
        r"騰落レシオ(?:\(25日\))?\s*[：:]\s*(\d+(?:\.\d+)?)",
        r"(\d{2,3}(?:\.\d+)?)\s*%?\s*(?:騰落レシオ|AD ratio)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            val = float(m.group(1))
            if 20 <= val <= 300:  # 合理的な範囲チェック
                logger.info(f"騰落レシオ取得（テキスト）: {val}")
                return round(val, 1)

    # パターン2: テーブルから抽出
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            for i, cell in enumerate(cells):
                cell_text = cell.get_text(strip=True)
                if "騰落" in cell_text:
                    # 同行の数値を探す
                    for j in range(len(cells)):
                        if j == i:
                            continue
                        val_text = cells[j].get_text(strip=True)
                        val_text = re.sub(r"[^\d.]", "", val_text)
                        if val_text:
                            try:
                                val = float(val_text)
                                if 20 <= val <= 300:
                                    logger.info(f"騰落レシオ取得（テーブル）: {val}")
                                    return round(val, 1)
                            except ValueError:
                                pass

    return None


def _parse_highlow(soup: "BeautifulSoup", text: str) -> tuple[int | None, int | None]:
    """kabutanページから新高値・新安値銘柄数を抽出する。

    Returns:
        (new_high, new_low): int | None の tuple
    """
    new_high = None
    new_low = None

    # パターン1: テキストから正規表現で抽出
    high_patterns = [
        r"新高値\s*[：:]\s*(\d+)",
        r"(\d+)\s*銘柄.*新高値",
        r"新高値.*?(\d+)銘柄",
    ]
    for pattern in high_patterns:
        m = re.search(pattern, text)
        if m:
            val = int(m.group(1))
            if 0 <= val <= 5000:  # 合理的な範囲
                new_high = val
                break

    low_patterns = [
        r"新安値\s*[：:]\s*(\d+)",
        r"(\d+)\s*銘柄.*新安値",
        r"新安値.*?(\d+)銘柄",
    ]
    for pattern in low_patterns:
        m = re.search(pattern, text)
        if m:
            val = int(m.group(1))
            if 0 <= val <= 5000:
                new_low = val
                break

    # パターン2: テーブルから抽出
    if new_high is None or new_low is None:
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            found_high = False
            found_low = False
            for row in rows:
                cells = row.find_all(["td", "th"])
                for i, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True)
                    if "新高値" in cell_text and not found_high:
                        for j in range(len(cells)):
                            if j == i:
                                continue
                            val_text = cells[j].get_text(strip=True)
                            val_text = re.sub(r"[^\d]", "", val_text)
                            if val_text:
                                try:
                                    val = int(val_text)
                                    if 0 <= val <= 5000:
                                        new_high = val
                                        found_high = True
                                        break
                                except ValueError:
                                    pass
                    elif "新安値" in cell_text and not found_low:
                        for j in range(len(cells)):
                            if j == i:
                                continue
                            val_text = cells[j].get_text(strip=True)
                            val_text = re.sub(r"[^\d]", "", val_text)
                            if val_text:
                                try:
                                    val = int(val_text)
                                    if 0 <= val <= 5000:
                                        new_low = val
                                        found_low = True
                                        break
                                except ValueError:
                                    pass

    return new_high, new_low


def _fetch_kabutan_market() -> dict[str, Any] | None:
    """kabutanの市場情報ページから騰落レシオを取得する。

    Returns:
        成功時はデータ辞書、失敗時は None。
    """
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as e:
        logger.warning(f"requests/bs4 が利用できません: {e}")
        return None

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                KABUTAN_MARKET_URL,
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text()

            ad_ratio = _parse_ad_ratio(soup, text)
            if ad_ratio is not None:
                logger.info(f"kabutan市場ページ: 騰落レシオ={ad_ratio}")
                return {"advance_decline_ratio": ad_ratio, "source_ad": "kabutan_market"}

        except Exception as e:
            logger.warning(f"kabutan市場ページ取得失敗 (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    return None


def _fetch_kabutan_highlow() -> dict[str, Any] | None:
    """kabutanの新高値新安値ページから銘柄数を取得する。

    Returns:
        成功時はデータ辞書、失敗時は None。
    """
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as e:
        logger.warning(f"requests/bs4 が利用できません: {e}")
        return None

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                KABUTAN_HIGHLOW_URL,
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text()

            new_high, new_low = _parse_highlow(soup, text)
            if new_high is not None or new_low is not None:
                logger.info(f"kabutan高安値: 新高値={new_high}, 新安値={new_low}")
                return {
                    "new_high": new_high,
                    "new_low": new_low,
                    "source_hl": "kabutan_highlow",
                }

        except Exception as e:
            logger.warning(f"kabutan高安値ページ取得失敗 (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    return None


def fetch_jpx_market_breadth(target_date: date) -> dict[str, Any]:
    """騰落レシオ・新高値新安値データを取得する。

    取得戦略（graceful degradation）:
      1. kabutanの市場情報ページから騰落レシオを取得
      2. kabutanの高安値ページから新高値・新安値を取得
      3. 前回キャッシュを使用する
      全失敗時: error=True を返す

    Args:
        target_date: 記事の対象日付（キャッシュ保存に使用）

    Returns:
        {
            "advance_decline_ratio": float | None,  # 騰落レシオ（25日）
            "ad_signal": str,                       # シグナル
            "new_high": int | None,                 # 新高値銘柄数
            "new_low": int | None,                  # 新安値銘柄数
            "nh_nl_ratio": float | None,            # 新高値/新安値比率
            "error": bool,
            "cached": bool,
        }
    """
    logger.info("JPX需給データ取得開始")

    # 前回キャッシュ（フォールバック用）
    prev_cache = _load_cache()

    result: dict[str, Any] = {
        "advance_decline_ratio": None,
        "ad_signal": "⚪",
        "new_high": None,
        "new_low": None,
        "nh_nl_ratio": None,
        "error": False,
        "cached": False,
    }

    # 1. 騰落レシオ取得
    market_data = _fetch_kabutan_market()
    if market_data:
        result["advance_decline_ratio"] = market_data.get("advance_decline_ratio")
        result["ad_signal"] = _ad_ratio_signal(result["advance_decline_ratio"])

    # 2. 新高値・新安値取得
    hl_data = _fetch_kabutan_highlow()
    if hl_data:
        result["new_high"] = hl_data.get("new_high")
        result["new_low"] = hl_data.get("new_low")

    # 新高値/新安値比率を計算
    if result["new_high"] is not None and result["new_low"] is not None:
        if result["new_low"] > 0:
            result["nh_nl_ratio"] = round(result["new_high"] / result["new_low"], 2)
        elif result["new_high"] > 0:
            result["nh_nl_ratio"] = float("inf")
        else:
            result["nh_nl_ratio"] = None

    # 何らかのデータが取得できた場合は保存
    if (result["advance_decline_ratio"] is not None
            or result["new_high"] is not None
            or result["new_low"] is not None):
        _save_cache(result, target_date)
        logger.info(
            f"JPX需給取得成功: 騰落レシオ={result['advance_decline_ratio']}, "
            f"新高値={result['new_high']}, 新安値={result['new_low']}"
        )
        return result

    # 3. キャッシュ フォールバック
    logger.info("JPX需給取得失敗 → キャッシュ フォールバック")
    if prev_cache is not None:
        logger.warning("前回キャッシュを使用します")
        cached_ratio = prev_cache.get("advance_decline_ratio")
        return {
            "advance_decline_ratio": cached_ratio,
            "ad_signal": _ad_ratio_signal(cached_ratio),
            "new_high": prev_cache.get("new_high"),
            "new_low": prev_cache.get("new_low"),
            "nh_nl_ratio": prev_cache.get("nh_nl_ratio"),
            "error": False,
            "cached": True,
        }

    logger.error("JPX需給データ取得完全失敗")
    return {
        "advance_decline_ratio": None,
        "ad_signal": "⚪",
        "new_high": None,
        "new_low": None,
        "nh_nl_ratio": None,
        "error": True,
        "cached": False,
    }
