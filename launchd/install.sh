#!/bin/bash
# launchd インストールスクリプト
# 使用方法: bash launchd/install.sh

PLIST_NAME="com.kojirou.market-briefing.plist"
PLIST_SRC="$(cd "$(dirname "$0")" && pwd)/$PLIST_NAME"
PLIST_DST="$HOME/Library/LaunchAgents/$PLIST_NAME"

echo "=== Market Briefing launchd インストール ==="

# パスの存在確認
if [ ! -f "$PLIST_SRC" ]; then
  echo "エラー: $PLIST_SRC が見つかりません"
  exit 1
fi

# コピー
cp "$PLIST_SRC" "$PLIST_DST"
echo "コピー完了: $PLIST_DST"

# ロード
launchctl load "$PLIST_DST"
echo "launchd ロード完了"

# 確認
launchctl list | grep com.kojirou.market-briefing
echo ""
echo "=== インストール完了 ==="
echo ""
echo "次のステップ:"
echo "1. $PLIST_DST を開いてメール通知の環境変数を設定"
echo "   - BRIEFING_EMAIL_FROM"
echo "   - BRIEFING_EMAIL_TO"
echo "   - BRIEFING_EMAIL_PASSWORD"
echo ""
echo "2. 設定後に reload:"
echo "   launchctl unload $PLIST_DST"
echo "   launchctl load $PLIST_DST"
echo ""
echo "3. 手動テスト実行:"
echo "   launchctl start com.kojirou.market-briefing"
