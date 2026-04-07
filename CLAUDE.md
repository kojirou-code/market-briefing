# Market Briefing — Claude Code ガイド
## 保護ルール
- 他プロジェクト（stock-screener, stock-analyzer, timing-judge,
  pipeline-controller, dividend-db）のファイルを変更・削除しないこと
- ~/projects/investing/ 直下の共有ファイルを上書きしないこと
## 技術スタック
- Python: /opt/anaconda3/bin/python (conda)
- Hugo / GitHub Pages / pytest
- pandas-ta / yfinance / feedparser / Jinja2 / mplfinance
- Gemini API (gemini-2.5-flash) / google-generativeai / python-dotenv
## コーディング規約
- docstring + 型ヒント
- エラーハンドリング（リトライ3回 → graceful degradation）
- 各collectorは独立して失敗可能（1つ失敗しても他は継続）
- スクレイピングは必ずフォールバックを実装
## Phase 1〜2 完了状況（2026-04-07）
- yfinance 13ティッカー + テクニカル信号機 ✅
- ローソク足チャート日足1年+週足3年（mplfinance）✅
- セクターETF騰落（米国11セクター）✅
- Fear & Greed自作スコア ✅
- 方向性推定スコア ✅
- 異常値アラート ✅
- Google News RSS（経済+地政学 4クエリ）+ NHK国際 + BBC World ✅
- Gemini APIニュースサマリー（リトライ3回付き）✅
- 経済カレンダー（YAML手動）✅
- Hugo build → GitHub Pages デプロイ ✅
- launchd 4:00/6:30 2回実行 + ニュース12:00/18:00/23:00 ✅
- deployer.py: hugo-site/ + data/ を git add ✅
- pytest 200テスト全パス ✅
- トップページカード改善（S&P/日経+Gemini結論）✅
- 信号機凡例追加 ✅
- TOPIX不連続データ補正 ✅
- Hugo unsafe HTML有効化 ✅
## Phase 2 残り（未実装）
- 需給データ（信用残・騰落レシオ・新高値新安値）
- ファンダメンタル指標推移チャート（VIX/債券/為替/原油 90日）
- 経済カレンダー自動取得（investing.comスクレイピング）
## 既知の課題
- AP International RSS: syntax errorで取得失敗（NHK/BBCでカバー）
- Google News EN 経済: 0件の場合あり（地政学ENでカバー）
