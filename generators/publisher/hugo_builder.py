"""
hugo_builder.py — Hugo ビルド実行

hugo build を実行してサイトを生成する。
"""

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


def build_hugo(hugo_site_dir: str | Path, hugo_path: str = "hugo") -> bool:
    """Hugo ビルドを実行する。

    Args:
        hugo_site_dir: hugo-site ディレクトリのパス
        hugo_path: hugo実行ファイルのパス

    Returns:
        ビルド成功なら True
    """
    hugo_site_dir = Path(hugo_site_dir)
    if not hugo_site_dir.exists():
        logger.error(f"Hugo サイトディレクトリが存在しない: {hugo_site_dir}")
        return False

    for attempt in range(MAX_RETRIES):
        try:
            result = subprocess.run(
                [hugo_path, "--minify"],
                cwd=str(hugo_site_dir),
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0:
                logger.info(f"Hugo ビルド成功 (attempt {attempt+1})")
                return True
            else:
                logger.warning(
                    f"Hugo ビルド失敗 (attempt {attempt+1}): "
                    f"returncode={result.returncode}\n"
                    f"stdout: {result.stdout}\n"
                    f"stderr: {result.stderr}"
                )
        except subprocess.TimeoutExpired:
            logger.warning(f"Hugo ビルドタイムアウト (attempt {attempt+1})")
        except Exception as e:
            logger.warning(f"Hugo ビルドエラー (attempt {attempt+1}): {e}")

    logger.error("Hugo ビルド全リトライ失敗")
    return False
