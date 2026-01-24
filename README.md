# bitFlyer JPY DCA (GitHub Actions)

bitFlyer Lightning API を使って、BTC_JPY を **木曜 20,000円 / 日曜 5,000円** で自動積立（DCA）するテンプレートです。

## 機能

- GitHub Actions cron 実行（木曜・日曜）
- Secrets 管理（APIキー/シークレット/LINE Notify トークン）
- 失敗時：**JST日時 + スクリプト名** を件名に入れて **LINE Notify** に通知（スタックトレース付き）
- 長文は **分割送信** + 分割間 `sleep(0.2)` でレート制限回避
- 通知スロットリング（同一エラーは一定時間に1回）
- 成功通知（要約）ON/OFF
- メンテSKIP
  - **時刻レンジSKIP**（任意、JST）
  - **APIメンテっぽい(502/503/504等)はSKIP扱いにして通知**（任意、デフォルト推奨ON）

## セットアップ

1. このリポジトリを作成して push
2. GitHub → Settings → Secrets and variables → Actions で Secrets を設定

### 必須Secrets

- `BITFLYER_API_KEY`
- `BITFLYER_API_SECRET`
- `LINE_NOTIFY_TOKEN`

### 実行設定（workflow内env）

- `PRODUCT_CODE` : `BTC_JPY`（固定）
- `BUY_AMOUNT_JPY` : scheduleから木曜/日曜で自動設定
- `MAX_BUY_AMOUNT_JPY` : 誤設定ガード（例 30000）
- `DRY_RUN` : 最初は `true` 推奨（注文せずログのみ）
- `NOTIFY_ON_SUCCESS` : 成功通知（true/false）
- `SKIP_TIME_RANGES_JST` : 例 `"04:00-05:00,12:30-12:45"`（空なら無効）
- `NOTIFY_ON_SKIP_TIME` : 時刻レンジSKIPの通知（true/false）
- `NOTIFY_ON_SKIP_API_MAINT` : APIメンテSKIPの通知（true/false）
- `ALERT_THROTTLE_SECONDS` : 同一エラーの通知間隔（秒）

## ローカルテスト

```bash
pip install -r requirements.txt -r requirements-dev.txt
coverage run --branch -m pytest -q
coverage report --show-missing --fail-under=100
```

## 重要

- APIキーは **出金権限を付けない**（残高取得・注文のみ）
- ログに秘密情報を出しません（ヘッダ/トークンは出力しない設計）
