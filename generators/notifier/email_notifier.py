"""
email_notifier.py — パイプライン失敗時のメール通知（Gmail + smtplib）

事前設定: Gmail アプリパスワードを環境変数に設定すること。
  export BRIEFING_EMAIL_FROM=your@gmail.com
  export BRIEFING_EMAIL_TO=your@gmail.com
  export BRIEFING_EMAIL_PASSWORD=xxxx-xxxx-xxxx-xxxx  # Gmailアプリパスワード
"""

import logging
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


def send_failure_notification(
    error_message: str,
    step: str = "不明",
    from_addr: str | None = None,
    to_addr: str | None = None,
    password: str | None = None,
) -> bool:
    """パイプライン失敗をメールで通知する。

    Args:
        error_message: エラーの詳細
        step: 失敗したパイプラインのステップ名
        from_addr: 送信元アドレス（環境変数 BRIEFING_EMAIL_FROM をフォールバック）
        to_addr: 送信先アドレス（環境変数 BRIEFING_EMAIL_TO をフォールバック）
        password: Gmailアプリパスワード（環境変数 BRIEFING_EMAIL_PASSWORD をフォールバック）

    Returns:
        送信成功なら True
    """
    from_addr = from_addr or os.environ.get("BRIEFING_EMAIL_FROM", "")
    to_addr = to_addr or os.environ.get("BRIEFING_EMAIL_TO", "")
    password = password or os.environ.get("BRIEFING_EMAIL_PASSWORD", "")

    if not from_addr or not to_addr or not password:
        logger.warning(
            "メール通知設定が不完全。環境変数 BRIEFING_EMAIL_FROM / TO / PASSWORD を確認してください。"
        )
        return False

    now = datetime.now().strftime("%Y-%m-%d %H:%M JST")
    subject = f"[Market Briefing] パイプライン失敗: {step} ({now})"
    body = f"""Daily Market Briefing パイプラインでエラーが発生しました。

発生ステップ: {step}
発生日時: {now}

エラー内容:
{error_message}

---
このメールは自動送信されています。
"""

    try:
        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(from_addr, password)
            server.sendmail(from_addr, to_addr, msg.as_string())

        logger.info(f"失敗通知メール送信成功: {to_addr}")
        return True

    except Exception as e:
        logger.error(f"メール送信エラー: {e}")
        return False
