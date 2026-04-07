"""
deployer.py — Git push による GitHub Pages デプロイ

git add → git commit → git push を実行する。
GitHub Actions が push を検知して自動デプロイする。
"""

import logging
import subprocess
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


def _run_git(args: list[str], cwd: str | Path, timeout: int = 60) -> tuple[bool, str]:
    """gitコマンドを実行して (success, output) を返す。"""
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except Exception as e:
        return False, str(e)


def deploy(project_root: str | Path, target_date: date | None = None) -> bool:
    """記事を git commit して push する。

    Args:
        project_root: market-briefing プロジェクトルート
        target_date: 記事の日付（コミットメッセージ用）

    Returns:
        デプロイ成功なら True
    """
    if target_date is None:
        target_date = date.today()

    project_root = Path(project_root)
    commit_msg = f"briefing: {target_date.strftime('%Y-%m-%d')}"

    # git add（記事・チャート画像・ビルド済みHTML・ニュースデータをまとめて追加）
    ok, out = _run_git(["add", "hugo-site/", "data/"], cwd=project_root)
    if not ok:
        logger.error(f"git add 失敗: {out}")
        return False

    # git status で変更確認
    ok, status = _run_git(["status", "--porcelain"], cwd=project_root)
    if not status.strip():
        logger.info("変更なし。デプロイをスキップします。")
        return True

    # git commit
    ok, out = _run_git(["commit", "-m", commit_msg], cwd=project_root)
    if not ok:
        logger.error(f"git commit 失敗: {out}")
        return False

    # git push（リトライ付き）
    for attempt in range(MAX_RETRIES):
        ok, out = _run_git(["push", "origin", "main"], cwd=project_root, timeout=120)
        if ok:
            logger.info(f"git push 成功 (attempt {attempt+1}): {commit_msg}")
            return True
        logger.warning(f"git push 失敗 (attempt {attempt+1}): {out}")

    logger.error("git push 全リトライ失敗")
    return False
