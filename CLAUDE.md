# Market Briefing — Claude Code ガイド
## 保護ルール
- 他プロジェクト（stock-screener, stock-analyzer, timing-judge,
  pipeline-controller, dividend-db）のファイルを変更・削除しないこと
- ~/projects/investing/ 直下の共有ファイルを上書きしないこと
## 技術スタック
- Python: /opt/anaconda3/bin/python (conda)
- Hugo / GitHub Pages / pytest
- pandas-ta / yfinance / feedparser / Jinja2
- matplotlib（Phase 2〜）
## コーディング規約
- docstring + 型ヒント
- エラーハンドリング（リトライ3回 → graceful degradation）
- 各collectorは独立して失敗可能（1つ失敗しても他は継続）
- Phase 3以降のスタブには # TODO: Phase N とコメント
## Phase 1 完了状況（2026-04-04）
- 全13ティッカー取得 + テクニカル分析（信号機） ✅
- 異常値アラート ✅
- RSSヘッドライン（NHK経済のみ稼働、Reuters要修正） ✅
- 経済カレンダー（YAML手動） ✅
- Jinja2 → Markdown → Hugo build → GitHub Pages ✅
- launchd（毎朝5:00）✅
- pytest 49テスト全パス ✅
- NumPy警告あり（動作に支障なし、pyarrow/numexpr互換性）
## 既知の課題
- Reuters RSSがエラー（URLまたはフィード仕様変更の可能性）
- NumPy 2.x と pyarrow/numexpr の互換性警告（動作には影響なし）
- CNBC Markets RSS: 0件（フィードURL要確認）
