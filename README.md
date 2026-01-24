
# bitflyer-dca

bitFlyer Lightning API を利用して BTC/JPY を **自動積立（DCA: Dollar Cost Averaging）** する Python スクリプトです。  
GitHub Actions などのスケジューラと連携し、**完全自動のBTC積立投資環境**を構築できます。

---

## 特徴 / Features

### 自動売買

- 成行(MARKET)注文によるBTC自動購入
- 指定金額（JPY）からBTC数量を自動計算（小数8桁・切り捨て）
- 残高不足時は自動エラー停止

### スケジューリング

- GitHub Actions cron / OS cron で定期実行
- 曜日・時間指定による積立実行

### セーフティ設計

- `MAX_BUY_AMOUNT_JPY` による誤発注防止
- `DRY_RUN` モードによる疑似実行
- APIメンテナンス検知（502/503/504）による自動SKIP
- 時刻レンジSKIP（JST指定）

### 通知機能

- Discord 通知（Webhook + スレッド対応）
- ntfy 通知（Click連携でDiscordジャンプ）
- エラー時スタックトレース通知
- 成功通知 / SKIP通知 切替
- 通知スロットリング（同一エラー一定時間1回）

### ログ / 状態管理

- 成功累積管理（購入回数 / BTC量 / JPY金額）
- 状態保存ディレクトリ `.state`
- JSONベースの状態管理

---

## 必須環境変数（Required）

```env
BITFLYER_API_KEY
BITFLYER_API_SECRET
PRODUCT_CODE=BTC_JPY
BUY_AMOUNT_JPY=20000
```

---

## 任意環境変数（Optional）

```env
BITFLYER_BASE_URL=https://api.bitflyer.com
MAX_BUY_AMOUNT_JPY=30000
DRY_RUN=true

DISCORD_WEBHOOK_URL=
DISCORD_GUILD_ID=
DISCORD_CHANNEL_ID=
DISCORD_BOT_TOKEN=

NTFY_TOPIC_URL=
NTFY_TOKEN=

NOTIFY_ON_SUCCESS=true
NOTIFY_ON_SKIP_TIME=false
NOTIFY_ON_SKIP_API_MAINT=true
NOTIFY_ON_NTFY=true
NOTIFY_ON_DISCORD=true

LOG_STACKTRACE=true
ALERT_THROTTLE_SECONDS=3600
SKIP_TIME_RANGES_JST="04:00-05:00,12:30-12:45"
STATE_DIR=.state
```

---

## GitHub Actions 連携例

```yaml
name: bitflyer-dca

on:
  schedule:
    - cron: '0 11 * * 4'   # 木曜 20:00 JST
    - cron: '0 11 * * 0'   # 日曜 20:00 JST

jobs:
  dca:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python bitflyer_dca.py
        env:
          BITFLYER_API_KEY: ${{ secrets.BITFLYER_API_KEY }}
          BITFLYER_API_SECRET: ${{ secrets.BITFLYER_API_SECRET }}
          PRODUCT_CODE: BTC_JPY
          BUY_AMOUNT_JPY: 20000
```

---

## ローカル実行

```bash
python bitflyer_dca.py
```

---

## セキュリティ

- 出金権限なしAPIキー使用
- 環境変数管理
- ログに秘密情報を出さない設計

---

MIT License
