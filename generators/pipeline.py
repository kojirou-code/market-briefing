"""
pipeline.py — メインパイプライン

毎朝 launchd から呼び出される。SPEC.md Section 6 の運用フロー実装。

使用方法:
    /opt/anaconda3/bin/python generators/pipeline.py [--dry-run] [--date YYYY-MM-DD]

オプション:
    --dry-run: git push を実行しない（記事生成・Hugoビルドまで）
    --date: 記事の日付を指定（デフォルト: 今日）
    --skip-deploy: デプロイのみスキップ
    --weekly: 強制的に週間まとめモードで実行（通常は土曜日に自動）
    --news-only: ニュース取得・保存のみ実行（12:00/18:00/23:00 の定期収集用）
"""

import argparse
import logging
import os
import sys
import tomllib
import traceback
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import yaml

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from generators.collectors.market_data import fetch_all_market_data
from generators.collectors.futures_commodities import fetch_all_indicators_and_futures
from generators.collectors.news_collector import fetch_and_save_news, fetch_news_headlines
from generators.collectors.gemini_summarizer import generate_and_save_summary, load_summary
from generators.collectors.economic_calendar import load_upcoming_events
from generators.collectors.sector_etf import fetch_sector_etfs
from generators.collectors.market_breadth import fetch_market_breadth
from generators.analyzers.technical import analyze_all_indices
from generators.analyzers.direction_scorer import calculate_direction_score
from generators.article.alert_checker import check_alerts
from generators.article.article_builder import (
    build_article,
    build_weekly_article,
    save_article,
    get_article_filename,
    get_weekly_article_filename,
)
from generators.article.chart_generator import generate_all_charts
from generators.publisher.hugo_builder import build_hugo
from generators.publisher.deployer import deploy
from generators.notifier.email_notifier import send_failure_notification

# ===== ロギング設定 =====
LOG_DIR = PROJECT_ROOT / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pipeline")

# ===== 設定ファイルパス =====
CONFIG_DIR = PROJECT_ROOT / "generators" / "config"
SETTINGS_PATH = CONFIG_DIR / "settings.yaml"
ALERT_THRESHOLDS_PATH = CONFIG_DIR / "alert_thresholds.yaml"
TRUSTED_SOURCES_PATH = CONFIG_DIR / "trusted_sources.yaml"
ECONOMIC_EVENTS_PATH = CONFIG_DIR / "economic_events.yaml"


def load_settings() -> dict:
    """settings.yaml を読み込む。"""
    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_trusted_sources() -> dict:
    """trusted_sources.yaml を読み込む。"""
    with open(TRUSTED_SOURCES_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_skip_day(target_date: date) -> bool:
    """日曜日はスキップ（SPEC.md Section 7）。"""
    return target_date.weekday() == 6  # 0=月, 6=日


def is_weekly_day(target_date: date) -> bool:
    """土曜日は週間まとめモード（SPEC.md Section 7）。"""
    return target_date.weekday() == 5  # 5=土


def run_news_only(target_date: date) -> bool:
    """ニュース取得・保存のみ実行する（--news-only モード）。

    12:00 / 18:00 / 23:00 の定期収集用。記事生成・デプロイは行わない。

    Returns:
        成功なら True
    """
    logger.info(f"===== ニュース取得のみ実行: {target_date} =====")
    try:
        items = fetch_and_save_news(target_date)
        logger.info(f"ニュース取得・保存完了: {len(items)}件")
        logger.info("===== ニュース取得完了 =====")
        return True
    except Exception as e:
        logger.error(f"ニュース取得エラー: {e}")
        return False


def run_pipeline(
    target_date: date,
    dry_run: bool = False,
    skip_deploy: bool = False,
    force_weekly: bool = False,
) -> bool:
    """メインパイプラインを実行する。

    Returns:
        成功なら True
    """
    logger.info(f"===== パイプライン開始: {target_date} =====")

    # [0] 曜日判定
    if is_skip_day(target_date):
        logger.info(f"{target_date} は日曜日 → スキップ")
        return True

    weekly_mode = force_weekly or is_weekly_day(target_date)
    if weekly_mode:
        logger.info(f"週間まとめモードで実行: {target_date}")

    settings = load_settings()
    failed_step = None

    try:
        # [1] 市場データ取得
        logger.info("[1] 市場データ取得")
        failed_step = "市場データ取得"
        market_data = fetch_all_market_data(settings)

        # [2] テクニカル分析
        logger.info("[2] テクニカル分析")
        failed_step = "テクニカル分析"
        technical_data = analyze_all_indices(market_data)

        # [3] 指標・先物・コモディティ取得
        logger.info("[3] 指標・先物取得")
        failed_step = "指標・先物取得"
        indicators_data = fetch_all_indicators_and_futures(settings)

        # [4] ニュース取得（Google News + JSON蓄積）
        logger.info("[4] ニュース取得")
        failed_step = "ニュース取得"
        trusted_sources = load_trusted_sources()
        # 朝5:00実行時: Google Newsで取得・保存し、蓄積済み全件を返す
        fetch_and_save_news(target_date)
        news_data = fetch_news_headlines(trusted_sources, target_date)

        # [4.5] Gemini APIサマリー生成（失敗してもパイプラインは継続）
        logger.info("[4.5] Geminiサマリー生成")
        news_summary = load_summary(target_date)  # 既存サマリーがあれば再利用
        if news_summary is None and news_data.get("all_items"):
            news_summary = generate_and_save_summary(news_data["all_items"], target_date)
        if news_summary:
            logger.info("Geminiサマリー利用可能")
        else:
            logger.info("Geminiサマリーなし → RSS TOP5でフォールバック")

        # [5] 経済カレンダー読み込み
        logger.info("[5] 経済カレンダー読み込み")
        failed_step = "経済カレンダー読み込み"
        calendar_events = load_upcoming_events(str(ECONOMIC_EVENTS_PATH), today=target_date)

        # [6] 異常値チェック（週間まとめは省略）
        if not weekly_mode:
            logger.info("[6] 異常値チェック")
            failed_step = "異常値チェック"
            alerts = check_alerts(market_data, indicators_data, str(ALERT_THRESHOLDS_PATH))
            if alerts:
                logger.info(f"アラート {len(alerts)} 件検出")
        else:
            alerts = []

        # ===== Phase 2 ステップ =====

        # [P2-1] セクターETF取得
        logger.info("[P2-1] セクターETF取得")
        sector_tickers = settings.get("sector_etf_tickers", [])
        sector_data = fetch_sector_etfs(sector_tickers if sector_tickers else None)

        # [P2-2] 市場ブレデス / Fear & Greed
        logger.info("[P2-2] Fear & Greed スコア計算")
        breadth_data = fetch_market_breadth(market_data, indicators_data)

        # [P2-3] 方向性推定スコア
        logger.info("[P2-3] 方向性推定スコア計算")
        direction_score = calculate_direction_score(technical_data, breadth_data)

        # [P2-4] チャート生成（週間まとめ以外）
        chart_urls: dict[str, str] = {}
        if not weekly_mode:
            logger.info("[P2-4] チャート生成")
            hugo_site_dir = PROJECT_ROOT / settings.get("hugo_site_dir", "hugo-site")
            static_dir = hugo_site_dir / "static"

            # hugo.toml の baseURL からパス部分を取得（GitHub Pages サブパス対応）
            # 例: "https://kojirou-code.github.io/market-briefing/" → "/market-briefing"
            base_url_path = ""
            hugo_toml_path = hugo_site_dir / "hugo.toml"
            if hugo_toml_path.exists():
                try:
                    with open(hugo_toml_path, "rb") as f:
                        hugo_config = tomllib.load(f)
                    parsed = urlparse(hugo_config.get("baseURL", ""))
                    base_url_path = parsed.path.rstrip("/")
                    if base_url_path:
                        logger.info(f"hugo.toml baseURL パス: {base_url_path}")
                except Exception as e:
                    logger.warning(f"hugo.toml の読み込みに失敗: {e}")

            chart_urls = generate_all_charts(market_data, target_date, static_dir, base_url_path)

        # ===== 記事生成 =====

        if weekly_mode:
            # [7] 週間まとめ記事生成
            logger.info("[7] 週間まとめ記事生成")
            failed_step = "週間まとめ記事生成"
            article_content = build_weekly_article(
                market_data=market_data,
                indicators_data=indicators_data,
                technical_data=technical_data,
                news_data=news_data,
                calendar_events=calendar_events,
                target_date=target_date,
                sector_data=sector_data,
                breadth_data=breadth_data,
                direction_score=direction_score,
            )
            filename = get_weekly_article_filename(target_date)
        else:
            # [7] 通常記事生成
            logger.info("[7] Markdown記事生成")
            failed_step = "記事生成"
            article_content = build_article(
                market_data=market_data,
                indicators_data=indicators_data,
                technical_data=technical_data,
                alerts=alerts,
                news_data=news_data,
                calendar_events=calendar_events,
                target_date=target_date,
                sector_data=sector_data,
                breadth_data=breadth_data,
                direction_score=direction_score,
                chart_urls=chart_urls,
                news_summary=news_summary,
            )
            filename = get_article_filename(target_date)

        article_path = PROJECT_ROOT / "hugo-site" / "content" / "posts" / filename
        save_article(article_content, article_path)

        if dry_run or skip_deploy:
            logger.info(f"dry-run / skip-deploy モード: 記事生成完了 → {article_path}")
            logger.info("===== パイプライン完了（デプロイスキップ） =====")
            return True

        # [8] Hugo build
        logger.info("[8] Hugo build")
        failed_step = "Hugo build"
        hugo_site_dir = PROJECT_ROOT / settings.get("hugo_site_dir", "hugo-site")
        hugo_path = settings.get("hugo_path", "hugo")
        if not build_hugo(hugo_site_dir, hugo_path):
            raise RuntimeError("Hugo ビルドに失敗しました")

        # [9] git push
        logger.info("[9] git push → GitHub Pages デプロイ")
        failed_step = "git push"
        if not deploy(PROJECT_ROOT, target_date):
            raise RuntimeError("デプロイ（git push）に失敗しました")

        logger.info("===== パイプライン完了 =====")
        return True

    except Exception as e:
        error_detail = f"{e}\n\n{traceback.format_exc()}"
        logger.error(f"パイプライン失敗 [{failed_step}]: {error_detail}")

        # メール通知
        send_failure_notification(
            error_message=error_detail,
            step=failed_step or "不明",
        )
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily Market Briefing パイプライン")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="記事生成まで実行し、Hugo build / git push はスキップ",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="記事の日付 (YYYY-MM-DD)。デフォルトは今日。",
    )
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="git push のみスキップ",
    )
    parser.add_argument(
        "--weekly",
        action="store_true",
        help="強制的に週間まとめモードで実行",
    )
    parser.add_argument(
        "--news-only",
        action="store_true",
        help="ニュース取得・保存のみ実行（12:00/18:00/23:00 の定期収集用）",
    )
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today()

    if args.news_only:
        success = run_news_only(target_date)
    else:
        success = run_pipeline(
            target_date=target_date,
            dry_run=args.dry_run,
            skip_deploy=args.skip_deploy,
            force_weekly=args.weekly,
        )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
