"""
pipeline.py — メインパイプライン

毎朝 launchd から呼び出される。SPEC.md Section 6 の運用フロー実装。

使用方法:
    /opt/anaconda3/bin/python generators/pipeline.py [--dry-run] [--date YYYY-MM-DD]

オプション:
    --dry-run: git push を実行しない（記事生成・Hugoビルドまで）
    --date: 記事の日付を指定（デフォルト: 今日）
    --skip-deploy: デプロイのみスキップ
"""

import argparse
import logging
import os
import sys
import traceback
from datetime import date, datetime
from pathlib import Path

import yaml

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from generators.collectors.market_data import fetch_all_market_data
from generators.collectors.futures_commodities import fetch_all_indicators_and_futures
from generators.collectors.news_collector import fetch_news_headlines
from generators.collectors.economic_calendar import load_upcoming_events
from generators.analyzers.technical import analyze_all_indices
from generators.article.alert_checker import check_alerts
from generators.article.article_builder import build_article, save_article, get_article_filename
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


def run_pipeline(target_date: date, dry_run: bool = False, skip_deploy: bool = False) -> bool:
    """メインパイプラインを実行する。

    Returns:
        成功なら True
    """
    logger.info(f"===== パイプライン開始: {target_date} =====")

    # [0] 曜日判定
    if is_skip_day(target_date):
        logger.info(f"{target_date} は日曜日 → スキップ")
        return True

    settings = load_settings()
    failed_step = None

    try:
        # [1] 市場データ取得
        logger.info("[1] 市場データ取得")
        market_data = fetch_all_market_data(settings)

        # [2] テクニカル分析
        logger.info("[2] テクニカル分析")
        technical_data = analyze_all_indices(market_data)

        # [3] 指標・先物・コモディティ取得
        logger.info("[3] 指標・先物取得")
        indicators_data = fetch_all_indicators_and_futures(settings)

        # [4] ニュースRSS取得
        logger.info("[4] ニュースRSS取得")
        failed_step = "ニュースRSS取得"
        trusted_sources = load_trusted_sources()
        news_data = fetch_news_headlines(trusted_sources)

        # [5] 経済カレンダー読み込み
        logger.info("[5] 経済カレンダー読み込み")
        failed_step = "経済カレンダー読み込み"
        calendar_events = load_upcoming_events(str(ECONOMIC_EVENTS_PATH), today=target_date)

        # [6] 異常値チェック
        logger.info("[6] 異常値チェック")
        failed_step = "異常値チェック"
        alerts = check_alerts(market_data, indicators_data, str(ALERT_THRESHOLDS_PATH))
        if alerts:
            logger.info(f"アラート {len(alerts)} 件検出")

        # [7] Markdown記事生成
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
        )

        # 記事の保存パス
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
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today()

    success = run_pipeline(
        target_date=target_date,
        dry_run=args.dry_run,
        skip_deploy=args.skip_deploy,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
