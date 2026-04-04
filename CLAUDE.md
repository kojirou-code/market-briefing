# Market Briefing — Claude Code ガイド

## 保護ルール
- 他プロジェクト（stock-screener, stock-analyzer, timing-judge,
  pipeline-controller, dividend-db）のファイルを変更・削除しないこと
- ~/projects/investing/ 直下の共有ファイルを上書きしないこと
- このプロジェクト（market-briefing）のファイルのみ変更可能

## 技術スタック
- Python: /opt/anaconda3/bin/python (conda)
- Hugo v0.159.2+ / GitHub Pages / pytest
- pandas-ta / yfinance / feedparser / Jinja2
- matplotlib（Phase 2〜）

## コーディング規約
- docstring + 型ヒント必須
- エラーハンドリング: リトライ3回 → graceful degradation（該当セクション「取得失敗」表示）
- 各collectorは独立して失敗可能（1つ失敗しても他は継続）
- Phase 2以降のスタブには # TODO: Phase N とコメント
- ログ: logging モジュール使用（print禁止）

## ディレクトリ構成
```
market-briefing/
├── generators/          # Python記事生成エンジン
│   ├── pipeline.py      # メインパイプライン
│   ├── collectors/      # データ収集モジュール
│   ├── analyzers/       # 分析モジュール
│   ├── article/         # 記事生成
│   ├── publisher/       # Hugo build + git push
│   ├── notifier/        # メール通知
│   ├── config/          # YAML設定ファイル
│   └── templates/       # Jinja2テンプレート
├── hugo-site/           # Hugoプロジェクト
├── data/                # 生成データ保存
└── tests/               # pytest
```

## 実行コマンド
```bash
# パイプライン手動実行
/opt/anaconda3/bin/python generators/pipeline.py

# テスト
/opt/anaconda3/bin/python -m pytest tests/ -v

# Hugo開発サーバー
cd hugo-site && hugo server
```

## 現在のPhase
- Phase 1: MVP実装中
- 仕様書: SPEC.md 参照
