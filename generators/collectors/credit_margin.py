"""
credit_margin.py — 信用残高データ取得（Phase 2）

取得戦略（graceful degradation）:
  1. JPX公式XLSを直接ダウンロード（金曜日基準、翌週水曜日公開）
  2. kabutanスクレイピング
  3. 前回キャッシュを使用

JPX XLS URL パターン:
  https://www.jpx.co.jp/markets/statistics-equities/margin/
  tvdivq0000001rk9-att/mtseisan{YYYYMMDD}00.xls
  （YYYYMMDD は金曜日の日付。翌週水曜日（金曜+5日）に公開）

データ保存先: data/credit_margin/YYYY-MM-DD.json
              data/credit_margin/latest.json（フォールバック用）
"""

import json
import logging
import re
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5
TIMEOUT = 10

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "credit_margin"

# JPX XLS 直接ダウンロードURL（{date} は金曜日の YYYYMMDD）
JPX_XLS_URL_TEMPLATE = (
    "https://www.jpx.co.jp/markets/statistics-equities/margin/"
    "tvdivq0000001rk9-att/mtseisan{date}00.xls"
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


def _get_candidate_fridays(target_date: date, count: int = 3) -> list[date]:
    """対象日から遡って利用可能な金曜日のリストを返す（新しい順）。

    JPXのXLSデータは金曜日基準。その翌週水曜日（金曜+5日）に公開される。
    公開日（金曜+5日）<= 対象日 の金曜日のみリストアップする。

    Args:
        target_date: 基準日
        count: 返す金曜日の数

    Returns:
        利用可能な金曜日のリスト（新しい順）
    """
    candidates: list[date] = []
    # target_date 以前の最新の金曜日を探す（target_date が金曜日なら当日）
    d = target_date
    while d.weekday() != 4:  # 4 = Friday
        d -= timedelta(days=1)

    checked = 0
    while len(candidates) < count and checked < 52:  # 最大1年分チェック
        # 金曜日 d のデータは d+5日（水曜日）に公開
        release_date = d + timedelta(days=5)
        if release_date <= target_date:
            candidates.append(d)
        d -= timedelta(days=7)
        checked += 1

    return candidates


def _extract_margin_from_sheet(
    df: "pd.DataFrame", data_date: date
) -> dict[str, Any] | None:
    """DataFrameから信用残高データを抽出する（合計行を探す）。

    JPX XLS の「合計」行には全市場合計の信用買い残・売り残・倍率が記載される。
    単位は百万円のため、1,000,000 で除して兆円に変換する。

    Args:
        df: header=None で読み込んだ DataFrame
        data_date: データの基準日（金曜日）

    Returns:
        解析成功時はデータ辞書、失敗時は None。
    """
    total_keywords = ["合計", "東証計", "市場全体", "全市場", "total"]

    for _idx, row in df.iterrows():
        row_text = " ".join(str(v) for v in row.values if str(v) != "nan")
        if not any(kw in row_text for kw in total_keywords):
            continue

        # 行から正の数値を抽出
        numeric_vals: list[float] = []
        for val in row:
            try:
                cleaned = str(val).replace(",", "").strip()
                num = float(cleaned)
                if num > 0:
                    numeric_vals.append(num)
            except (ValueError, TypeError):
                pass

        if len(numeric_vals) < 2:
            continue

        sorted_vals = sorted(numeric_vals, reverse=True)
        buy_raw = sorted_vals[0]
        sell_raw = sorted_vals[1] if len(sorted_vals) >= 2 else None

        # 500,000（百万円）以上 = 0.5兆円以上 → 百万円単位と判定
        # それ未満は千株単位など別フォーマットとみなしスキップ
        if buy_raw < 500_000:
            continue

        buy_trillion = buy_raw / 1_000_000
        sell_trillion = sell_raw / 1_000_000 if sell_raw else None

        # 合理性チェック（信用買い残は通常 1〜30 兆円程度）
        if not (0.1 <= buy_trillion <= 30.0):
            continue

        # 信用倍率（0.1〜50 程度。買い残・売り残より小さい値）
        ratio = None
        for val in numeric_vals:
            if 0.1 <= val <= 50.0:
                ratio = round(val, 2)
                break

        return {
            "margin_buy": round(buy_trillion, 2),
            "margin_sell": round(sell_trillion, 2) if sell_trillion else None,
            "margin_ratio": ratio,
            "data_date": data_date.strftime("%Y-%m-%d"),
            "source": "JPX",
            "error": False,
        }

    return None


def _parse_jpx_xls(xls_bytes: bytes, data_date: date) -> dict[str, Any] | None:
    """JPX XLSバイトデータを解析して信用残高を抽出する。

    全シートを走査し、最初に合計行が見つかったシートのデータを返す。

    Args:
        xls_bytes: XLSファイルのバイト列
        data_date: データの基準日（金曜日）

    Returns:
        解析成功時はデータ辞書、失敗時は None。
    """
    try:
        import io
        import pandas as pd
    except ImportError as e:
        logger.warning(f"pandas が利用できません: {e}")
        return None

    try:
        xls_file = io.BytesIO(xls_bytes)
        xl = pd.ExcelFile(xls_file, engine="xlrd")

        for sheet_name in xl.sheet_names:
            try:
                df = xl.parse(sheet_name, header=None)
            except Exception:
                continue

            result = _extract_margin_from_sheet(df, data_date)
            if result is not None:
                logger.info(
                    f"JPX XLS解析成功 (シート: {sheet_name}): "
                    f"買い残={result['margin_buy']:.2f}兆円"
                )
                return result

        logger.warning("JPX XLS: 全シートで合計行が見つかりませんでした")
        return None

    except Exception as e:
        logger.warning(f"JPX XLS解析エラー: {e}")
        return None


def _fetch_jpx_xls(target_date: date) -> dict[str, Any] | None:
    """JPX公式XLSを直接ダウンロードして解析する。

    直近3週分の金曜日を新しい順に試みる。404 の場合は次の金曜日へ。

    Args:
        target_date: 記事の対象日付（候補金曜日の決定に使用）

    Returns:
        成功時はデータ辞書、失敗時は None。
    """
    try:
        import requests
    except ImportError as e:
        logger.warning(f"requests が利用できません: {e}")
        return None

    fridays = _get_candidate_fridays(target_date, count=3)
    if not fridays:
        logger.warning("JPX XLS: 利用可能な金曜日が見つかりません")
        return None

    for friday in fridays:
        date_str = friday.strftime("%Y%m%d")
        url = JPX_XLS_URL_TEMPLATE.format(date=date_str)
        logger.info(f"JPX XLS試行: {url}")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 404:
                logger.info(f"JPX XLS: 404 → スキップ ({date_str})")
                continue
            resp.raise_for_status()

            result = _parse_jpx_xls(resp.content, friday)
            if result is not None:
                logger.info(f"JPX XLS取得成功: {friday}")
                return result
            else:
                logger.warning(f"JPX XLS: 解析失敗 ({date_str}) → 次の週を試みる")

        except Exception as e:
            logger.warning(f"JPX XLS取得失敗 ({date_str}): {e}")

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
                            if i + 1 < len(cells):
                                val_text = cells[i + 1].get_text(strip=True)
                                val_text = re.sub(r"[^\d.]", "", val_text)
                                if val_text:
                                    val = float(val_text)
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
      1. JPX公式XLS直接ダウンロード（直近3週の金曜日を試行）
      2. kabutanスクレイピングを試みる
      3. 前回キャッシュを使用する
      全失敗時: error=True を返す

    Args:
        target_date: 記事の対象日付（キャッシュ保存・金曜日算出に使用）

    Returns:
        {
            "margin_buy": float | None,     # 信用買い残（兆円）
            "margin_sell": float | None,    # 信用売り残（兆円）
            "margin_ratio": float | None,   # 信用倍率
            "buy_change_pct": float | None, # 前週比（%）
            "data_date": str | None,        # データ基準日（金曜日）
            "source": str,                  # データソース
            "error": bool,
            "cached": bool,                 # キャッシュデータの場合 True
        }
    """
    logger.info("信用残高データ取得開始")

    # 前回キャッシュ（前週比計算用・フォールバック用）
    prev_cache = _load_cache()

    # 1. JPX XLS 直接ダウンロード
    data = _fetch_jpx_xls(target_date)
    if data is not None and not data.get("error"):
        data["buy_change_pct"] = _calc_change_pct(data.get("margin_buy"), prev_cache)
        data.setdefault("data_date", target_date.strftime("%Y-%m-%d"))
        data["cached"] = False
        _save_cache(data, target_date)
        logger.info(f"JPX XLS取得成功: {data}")
        return data

    # 2. kabutan フォールバック
    logger.info("JPX XLS失敗 → kabutan フォールバック")
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
