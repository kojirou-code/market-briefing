#!/bin/bash
# launchd インストールスクリプト
# 使用方法: bash launchd/install.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"

install_plist() {
  local plist_name="$1"
  local src="$SCRIPT_DIR/$plist_name"
  local dst="$LAUNCH_AGENTS/$plist_name"

  if [ ! -f "$src" ]; then
    echo "エラー: $src が見つかりません"
    return 1
  fi

  cp "$src" "$dst"
  echo "コピー完了: $dst"
  launchctl load "$dst"
  echo "ロード完了: $plist_name"
}

echo "=== Market Briefing launchd インストール ==="

# メインパイプライン（毎朝 5:00）
install_plist "com.kojirou.market-briefing.plist"

# ニュース収集（12:00 / 18:00 / 23:00）
install_plist "com.kojirou.market-briefing-news.plist"

# 確認
launchctl list | grep com.kojirou.market-briefing
echo ""
echo "=== インストール完了 ==="
echo ""
echo "次のステップ:"
echo "1. $LAUNCH_AGENTS/com.kojirou.market-briefing.plist を開いてメール通知の環境変数を設定"
echo "   - BRIEFING_EMAIL_FROM"
echo "   - BRIEFING_EMAIL_TO"
echo "   - BRIEFING_EMAIL_PASSWORD"
echo ""
echo "2. 設定後に reload:"
echo "   launchctl unload $LAUNCH_AGENTS/com.kojirou.market-briefing.plist"
echo "   launchctl load $LAUNCH_AGENTS/com.kojirou.market-briefing.plist"
echo ""
echo "3. 手動テスト:"
echo "   launchctl start com.kojirou.market-briefing        # 朝の記事生成"
echo "   launchctl start com.kojirou.market-briefing-news   # ニュース収集のみ"
