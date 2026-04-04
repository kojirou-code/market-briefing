# Market Briefing — Claude Code ガイド
## 保護ルール
- 他プロジェクト（stock-screener, stock-analyzer, timing-judge,
  pipeline-controller, dividend-db）のファイルを変更・削除しないこと
- ~/projects/investing/ 直下の共有ファイルを上書きしないこと
## 技術スタック
- Python: /opt/anaconda3/bin/python (conda)
- Hugo / GitHub Pages / pytest
- pandas-ta / yfinance / feedparser / Jinja2
- matplotlib 3.10.8（NumPy 1.26.4 対応版）
## コーディング規約
- docstring + 型ヒント
- エラーハンドリング（リトライ3回 → graceful degradation）
- 各collectorは独立して失敗可能（1つ失敗しても他は継続）
- Phase 3以降のスタブには # TODO: Phase N とコメント
## Phase 1 完了状況（2026-04-04）
- 全13ティッカー取得 + テクニカル分析（信号機） ✅
- 異常値アラート ✅
- RSSヘッドライン（NHK経済のみ稼働） ✅
- 経済カレンダー（YAML手動） ✅
- Jinja2 → Markdown → Hugo build → GitHub Pages ✅
- launchd（毎朝5:00）✅
- pytest 49テスト全パス ✅
## Phase 2 完了状況（2026-04-04）
- matplotlibチャート生成（6指数×60日、SMA5/25/75 + 出来高） ✅
- セクターETF騰落率（米国11セクター: XLK〜XLC）✅
- Fear & Greed 自作スコア（VIX/モメンタム/RSI/MACD/52週高低値 複合）✅
- 方向性推定スコア（テクニカル+F&G統合、-10〜+10点）✅
- 週間まとめ記事（土曜日自動生成 / --weekly オプション）✅
- RSS修正（Reuters削除→Yahoo Finance/MarketWatch追加、CNBC URL正規化）✅
- matplotlib NumPy互換問題解消（matplotlib 3.10.8 + NumPy 1.26.4 に更新）✅
- pytest 116テスト全パス ✅
## Phase 3 実装状況（2026-04-04）
- Google News RSSキーワード検索（日本語・英語）に刷新 ✅
- data/news/YYYY-MM-DD.json への蓄積（4回/日 → --news-only オプション） ✅
- Gemini API（gemini-2.5-flash）によるニュースサマリー生成 ✅
- summary_YYYY-MM-DD.json に保存、テンプレートで構造化表示 ✅
- TOPIXデータ異常修正（df_long統一で価格スケール一致） ✅
- launchd plist追加（12:00/18:00/23:00 ニュース収集） ✅
- Google Newsキーワードを4クエリ分割（JP/EN × 経済・金融/地政学） ✅
- NHK国際 / BBC World / AP国際ニュース 直接RSS追加（地政学カバレッジ強化） ✅
- TOP5選別に地政学補充ロジック追加 ✅
- Geminiプロンプトに地政学必須ルール追加 ✅
- pytest 169テスト全パス ✅
## 既知の課題
- Reuters RSS: feeds.reuters.com が全ドメインでアクセス不可（削除済み）
- numexpr 2.8.7 が古い（Pandas推奨は 2.10.2+）→ 動作に影響なし
- bottleneck 1.3.7 が古い（Pandas推奨は 1.4.2+）→ 動作に影響なし
- google.generativeai は非推奨、google-genai（google.genai）に移行済み
