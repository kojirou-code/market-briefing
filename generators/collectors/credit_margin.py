"""
credit_margin.py — 信用残高データ取得（Phase 2）

取得戦略（graceful degradation）:
  1. JPX公開ページからCSVリンクを探してダウンロード
  2. kabutanスクレイピング
  3. 前回キャッシュを使用

データ保存先: data/credit_margin/YYYY-MM-DD.json
              data/credit_margin/latest.json（フォールバック用）
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
CACHE_DIR = PROJECT_ROOT / "data" / "credit_margin"

# JPX信用残高インデックスページ
JPX_MARGIN_INDEX_URL = (
    "https://www.jpx.co.jp/markets/statistics-equities/margin/index.html"
)

# kabutan信用残高ページ（フォールバック）
KABUTAN_CREDIT_URL = "https://kabutan.jp/info/credit/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}


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
        logger.info(f"信用残高キャッシュ読み込み: {data.get('date', '不明')}")
        return data
    except Exception as e:
        logger.warning(f"キャッシュ読み込み失敗: {e}")
        return None


def _fetch_jpx_csv() -> dict[str, Any] | None:
    """JPXページからCSVリンクを取得してダウンロード・解析する。

    Returns:
        成功時は信用残高データ辞書、失敗時は None。
    """
    try:
        import requests
        from bs4 import BeautifulSoup
        import pandas as pd
    except ImportError as e:
        logger.warning(f"requests/bs4/pandas が利用できません: {e}")
        return None

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                JPX_MARGIN_INDEX_URL,
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # CSVリンクを探す（href に .csv が含まれるか、data-csv 属性など）
            csv_url = None
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "data.csv" in href.lower() or (
                    "margin" in href.lower() and "csv" in href.lower()
                ):
                    if href.startswith("http"):
                        csv_url = href
                    else:
                        csv_url = "https://www.jpx.co.jp" + href
                    break

            if not csv_url:
                # nls形式のリンクを探す
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "nlsgeu" in href and "att" in href:
                        if href.startswith("http"):
                            csv_url = href
                        else:
                            csv_url = "https://www.jpx.co.jp" + href
                        break

            if not csv_url:
                logger.warning(f"JPX: CSVリンク見つからず (attempt {attempt + 1})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                continue

            logger.info(f"JPX CSV URL: {csv_url}")
            csv_resp = requests.get(csv_url, headers=HEADERS, timeout=TIMEOUT)
            csv_resp.raise_for_status()
            # エンコーディングはShift-JISの場合が多い
            csv_resp.encoding = csv_resp.apparent_encoding or "shift_jis"

            from io import StringIO
            df = pd.read_csv(StringIO(csv_resp.text), encoding=csv_resp.encoding)

            return _parse_jpx_csv(df)

        except Exception as e:
            logger.warning(f"JPX CSV取得失敗 (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    return None


def _parse_jpx_csv(df: "pd.DataFrame") -> dict[str, Any] | None:
    """JPX CSVデータを解析して信用残高を抽出する。

    JPX CSV の主なフォーマット:
      列: 週末日, 市場区分, 信用買い残(千株), 信用売り残(千株), 信用倍率
      または: 週末日, 市場区分, 信用買い残(百万円), 信用売り残(百万円), 信用倍率

    Returns:
        解析成功時はデータ辞書、失敗時は None。
    """
    try:
        # 列名を正規化
        df.columns = [str(c).strip() for c in df.columns]

        # 「市場全体」または「東証計」の最新行を抽出
        market_keywords = ["市場全体", "東証計", "全市場", "合計", "total"]
        target_row = None
        for kw in market_keywords:
            mask = df.apply(
                lambda row: any(kw in str(v) for v in row), axis=1
            )
            if mask.any():
                target_row = df[mask].iloc[-1]
                break

        if target_row is None:
            # フォールバック: 先頭または最後の有効な行
            target_row = df.dropna(how="all").iloc[-1]

        # 信用買い残・売り残・倍率を数値列から抽出
        numeric_vals = []
        for val in target_row:
            try:
                cleaned = str(val).replace(",", "").strip()
                numeric_vals.append(float(cleaned))
            except (ValueError, TypeError):
                numeric_vals.append(None)

        # 有効な数値が3つ以上あれば解析
        valid_nums = [v for v in numeric_vals if v is not None and v > 0]
        if len(valid_nums) < 2:
            logger.warning("JPX CSV: 有効な数値が不足")
            return None

        # 大きい順に買い残・売り残（単位: 百万円 or 千株）
        # 信用倍率は通常 0.5〜20 程度
        # 最大値が百万円単位なので、兆円に変換
        sorted_vals = sorted(valid_nums, reverse=True)
        buy_raw = sorted_vals[0]   # 信用買い残（最大値）
        sell_raw = sorted_vals[1]  # 信用売り残

        # 百万円 → 兆円変換（1兆円 = 1,000,000百万円）
        buy_trillion = buy_raw / 1_000_000
        sell_trillion = sell_raw / 1_000_000

        # 信用倍率 (1-20の範囲の値を探す)
        ratio = None
        for val in valid_nums:
            if 0.1 <= val <= 50.0:
                ratio = val
                break

        # 合理性チェック（信用買い残は通常1〜30兆円程度）
        if buy_trillion > 100 or buy_trillion < 0.001:
            logger.warning(f"JPX CSV: 信用買い残の値が不合理: {buy_trillion:.2f}兆円")
            return None

        logger.info(
            f"JPX CSV解析成功: 買い残={buy_trillion:.2f}兆円, "
            f"売り残={sell_trillion:.2f}兆円, 倍率={ratio}"
        )
        return {
            "margin_buy": round(buy_trillion, 2),
            "margin_sell": round(sell_trillion, 2),
            "margin_ratio": round(ratio, 2) if ratio else None,
            "source": "JPX",
            "error": False,
        }

    except Exception as e:
        logger.warning(f"JPX CSV解析エラー: {e}")
        return None


def _fetch_kabutan_credit() -> dict[str, Any] | None:
    """kabutanから信用残高をスクレイピングする。

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
                KABUTAN_CREDIT_URL,
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            return _parse_kabutan_credit(soup)

        except Exception as e:
            logger.warning(f"kabutan信用残高取得失敗 (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    return None


def _parse_kabutan_credit(soup: "BeautifulSoup") -> dict[str, Any] | None:
    """kabutanの信用残高ページを解析する。

    kabutanのページ構造:
    - テーブルに信用買い残、売り残、倍率が記載されている
    - 数値は「X.X兆円」「XX,XXX億円」などの形式

    Returns:
        解析成功時はデータ辞書、失敗時は None。
    """
    try:
        # 「兆円」「億円」を含むテキストを検索
        buy_trillion = None
        sell_trillion = None
        ratio = None

        text = soup.get_text()

        # 信用買い残（例: "3.87兆円", "38,700億円"）
        buy_patterns = [
            r"信用買い?残[^\d]*?(\d+(?:\.\d+)?)兆円",
            r"買い?残[^\d]*?(\d+(?:,\d+)*(?:\.\d+)?)億円",
        ]
        for pattern in buy_patterns:
            m = re.search(pattern, text)
            if m:
                val_str = m.group(1).replace(",", "")
                val = float(val_str)
                if "兆" in pattern:
                    buy_trillion = val
                else:
                    buy_trillion = val / 10000  # 億円 → 兆円
                break

        # 信用売り残
        sell_patterns = [
            r"信用売り?残[^\d]*?(\d+(?:\.\d+)?)兆円",
            r"売り?残[^\d]*?(\d+(?:,\d+)*(?:\.\d+)?)億円",
        ]
        for pattern in sell_patterns:
            m = re.search(pattern, text)
            if m:
                val_str = m.group(1).replace(",", "")
                val = float(val_str)
                if "兆" in pattern:
                    sell_trillion = val
                else:
                    sell_trillion = val / 10000
                break

        # 信用倍率
        ratio_m = re.search(r"信用倍率[^\d]*?(\d+(?:\.\d+)?)", text)
        if ratio_m:
            ratio = float(ratio_m.group(1))

        if buy_trillion is None and sell_trillion is None:
            # テーブルから数値を直接抽出
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True)
                        if "買い残" in cell_text or "買残" in cell_text:
                            # 次のセルに数値がある可能性
                            if i + 1 < len(cells):
                                val_text = cells[i + 1].get_text(strip=True)
                                val_text = re.sub(r"[^\d.]", "", val_text)
                                if val_text:
                                    val = float(val_text)
                                    # 兆円単位か億円単位かを判定
                                    if val < 100:  # おそらく兆円
                                        buy_trillion = val
                                    else:  # おそらく億円
                                        buy_trillion = val / 10000

        if buy_trillion is None:
            logger.warning("kabutan: 信用買い残を取得できませんでした")
            return None

        logger.info(
            f"kabutan解析成功: 買い残={buy_trillion:.2f}兆円, "
            f"売り残={sell_trillion:.2f}兆円 (あれば), 倍率={ratio}"
        )
        return {
            "margin_buy": round(buy_trillion, 2),
            "margin_sell": round(sell_trillion, 2) if sell_trillion else None,
            "margin_ratio": round(ratio, 2) if ratio else None,
            "source": "kabutan",
            "error": False,
        }

    except Exception as e:
        logger.warning(f"kabutan信用残高解析エラー: {e}")
        return None


def _calc_change_pct(
    current: float | None,
    cached: dict[str, Any] | None,
) -> float | None:
    """前回データとの変化率（%）を計算する。"""
    if current is None or cached is None:
        return None
    prev = cached.get("margin_buy")
    if prev is None or prev == 0:
        return None
    return round((current - prev) / prev * 100, 2)


def fetch_credit_margin(target_date: date) -> dict[str, Any]:
    """信用残高データを取得する。

    取得戦略（graceful degradation）:
      1. JPX公開CSVを試みる
      2. kabutanスクレイピングを試みる
      3. 前回キャッシュを使用する
      全失敗時: error=True を返す

    Args:
        target_date: 記事の対象日付（キャッシュ保存に使用）

    Returns:
        {
            "margin_buy": float | None,    # 信用買い残（兆円）
            "margin_sell": float | None,   # 信用売り残（兆円）
            "margin_ratio": float | None,  # 信用倍率
            "buy_change_pct": float | None, # 前週比（%）
            "data_date": str | None,       # データ基準日
            "source": str,                 # データソース
            "error": bool,
            "cached": bool,                # キャッシュデータの場合 True
        }
    """
    logger.info("信用残高データ取得開始")

    # 前回キャッシュ（前週比計算用・フォールバック用）
    prev_cache = _load_cache()

    # 1. JPX CSV
    data = _fetch_jpx_csv()
    if data is not None and not data.get("error"):
        data["buy_change_pct"] = _calc_change_pct(data.get("margin_buy"), prev_cache)
        data["data_date"] = target_date.strftime("%Y-%m-%d")
        data["cached"] = False
        _save_cache(data, target_date)
        logger.info(f"JPX CSV取得成功: {data}")
        return data

    # 2. kabutan フォールバック
    logger.info("JPX CSV失敗 → kabutan フォールバック")
    data = _fetch_kabutan_credit()
    if data is not None and not data.get("error"):
        data["buy_change_pct"] = _calc_change_pct(data.get("margin_buy"), prev_cache)
        data["data_date"] = target_date.strftime("%Y-%m-%d")
        data["cached"] = False
        _save_cache(data, target_date)
        logger.info(f"kabutan取得成功: {data}")
        return data

    # 3. キャッシュ フォールバック
    logger.info("kabutan失敗 → キャッシュ フォールバック")
    if prev_cache is not None:
        logger.warning("前回キャッシュを使用します")
        return {
            "margin_buy": prev_cache.get("margin_buy"),
            "margin_sell": prev_cache.get("margin_sell"),
            "margin_ratio": prev_cache.get("margin_ratio"),
            "buy_change_pct": None,
            "data_date": prev_cache.get("date"),
            "source": prev_cache.get("source", "cache"),
            "error": False,
            "cached": True,
        }

    logger.error("信用残高データ取得完全失敗")
    return {
        "margin_buy": None,
        "margin_sell": None,
        "margin_ratio": None,
        "buy_change_pct": None,
        "data_date": None,
        "source": "none",
        "error": True,
        "cached": False,
    }
