"""bitFlyer Lightning API で BTC_JPY を円から自動積立（DCA）するスクリプト。

## 主な機能
- スケジューラ実行（GitHub Actions / cron など）
- 残高チェック（JPY available）→ ticker(LTP)取得 → 成行(MARKET) BUY 発注
- 安全装置:
  - MAX_BUY_AMOUNT_JPY による誤設定ガード
  - DRY_RUN（注文せず計算のみ）
  - 時刻レンジSKIP（JST、任意）
  - APIメンテっぽいエラー(502/503/504)は SKIP 扱い（任意、通知可）
- 通知:
  - Discord（Webhook。Bot Token/Channel ID が揃えば「親メッセージ→スレッド」に詳細を投稿）
  - ntfy（要約通知。Click で Discord 親メッセージへ誘導）
  - 長文分割送信＋分割間 sleep(0.2) によるレート制限回避
  - 通知スロットリング（同一エラーは一定時間に1回）
  - 成功通知 ON/OFF（NOTIFY_ON_SUCCESS）
- 状態管理:
  - alert_state.json: エラー通知スロットリング用
  - success_state.json: 成功時の累積（約定回数/金額/BTC量）

## 設計ポリシー
- 署名/トークン等の秘密情報をログに出さない
- 例外通知は「調査に必要な情報（スタックトレース）」を Discord 側に含める
- 型は可能な限り厳密にし、Any の使用を極力避ける

## 必須環境変数
- BITFLYER_API_KEY, BITFLYER_API_SECRET
- PRODUCT_CODE (例: BTC_JPY)
- BUY_AMOUNT_JPY (例: 20000)

## 任意環境変数
- BITFLYER_BASE_URL (default: https://api.bitflyer.com)
- MAX_BUY_AMOUNT_JPY
- DRY_RUN (true/false)
- DISCORD_WEBHOOK_URL
- DISCORD_GUILD_ID
- DISCORD_CHANNEL_ID
- DISCORD_BOT_TOKEN（スレッド作成に必要。未設定でも動作するがスレッド化されない）
- NTFY_TOPIC_URL
- NTFY_TOKEN (任意: Bearer)
- ALERT_THROTTLE_SECONDS (例: 3600)
- STATE_DIR (例: .state)
- NOTIFY_ON_SUCCESS (true/false: default true)
- SKIP_TIME_RANGES_JST (例: "04:00-05:00,12:30-12:45")
- NOTIFY_ON_SKIP_TIME (true/false: default false)
- NOTIFY_ON_SKIP_API_MAINT (true/false: default true)
- NOTIFY_ON_NTFY (true/false: default true)
- NOTIFY_ON_DISCORD (true/false: default true)
- LOG_STACKTRACE (true/false: default true)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Literal, Mapping, Sequence, TypedDict, TypeAlias, cast

import requests
from requests import Response
from requests.exceptions import RequestException

# Constant Values
CONTENT_TYPE_JSON = "application/json"


class ConfigError(RuntimeError):
    """設定（環境変数や入力値）が不正なときに投げる例外。"""


@dataclass
class ApiError(RuntimeError):
    """外部API呼び出しが失敗したときに投げる例外。"""

    message: str
    status_code: int | None = None

    def __str__(self) -> str:
        if self.status_code is None:
            return self.message
        return f"{self.message} (status={self.status_code})"


@dataclass(frozen=True)
class NotifyConfig:
    """通知関連の設定をまとめた構造体。"""

    # Discord
    discord_webhook_url: str | None
    discord_guild_id: str | None
    discord_channel_id: str | None
    discord_bot_token: str | None

    # ntfy
    ntfy_topic_url: str | None
    ntfy_token: str | None

    # flags
    notify_on_discord: bool
    notify_on_ntfy: bool
    notify_on_skip_time: bool
    notify_on_success: bool


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | Mapping[str,
                                            "JsonValue"] | Sequence["JsonValue"]


class BalanceRow(TypedDict, total=False):
    """bitFlyer getbalance の1要素（必要最小限）。"""

    currency_code: str
    available: float


class ChildOrderResponse(TypedDict, total=False):
    """bitFlyer sendchildorder のレスポンス（必要最小限）。"""

    child_order_acceptance_id: str


DcaStatus: TypeAlias = Literal["OK", "SKIP", "SKIP_TIME"]


class DcaResult(TypedDict, total=False):
    """DCA実行結果（戻り値として扱う最小構造）。"""

    status: DcaStatus
    reason: str
    product_code: str
    buy_amount_jpy: Decimal
    ltp: Decimal
    size: Decimal
    acceptance_id: str | None


def env(name: str, default: str | None = None) -> str:
    """環境変数を取得する。

    Args:
        name: 環境変数名
        default: 未設定時のデフォルト値

    Returns:
        環境変数の値（空文字は許容しない）

    Raises:
        ConfigError: 必須環境変数が未設定／空の場合
    """
    value = os.getenv(name, default)
    if value is None:
        raise ConfigError(f"Missing env var: {name}")
    if value == "":
        raise ConfigError(f"Empty env var: {name}")
    return value


def bool_env(name: str, default: str = "false") -> bool:
    """環境変数を bool として解釈する。"""
    raw = env(name, default).strip().lower()
    return raw in ("1", "true", "yes", "on")


def now_jst_str() -> str:
    """現在時刻をJST(UTC+9)で 'YYYY-MM-DD HH:MM:SS JST' 形式で返す。"""
    jst = timezone(timedelta(hours=9))
    return datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S JST")


def script_name() -> str:
    """実行中スクリプト名（ファイル名）を返す。"""
    return Path(__file__).name


def sign_hmac_sha256_hex(secret: str, text: str) -> str:
    """HMAC-SHA256 署名を 16進hex で返す。"""
    return hmac.new(secret.encode("utf-8"), text.encode("utf-8"), hashlib.sha256).hexdigest()


def json_dumps_compact(data: Mapping[str, JsonValue]) -> str:
    """署名の安定化のために、JSONの出力形式を固定して文字列化する。"""
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def _resp_json(resp: Response) -> JsonValue:
    """Response.json() を JsonValue に寄せて返す。"""
    parsed = resp.json()
    return cast(JsonValue, parsed)


def http_request_json(
    method: str,
    url: str,
    headers: Mapping[str, str],
    body_str: str | None,
    timeout_sec: int = 20,
) -> JsonValue:
    """HTTPリクエストを実行し、JSONを返す。

    Raises:
        ApiError: 通信失敗／JSONでない等
    """
    try:
        resp = requests.request(method, url, headers=dict(
            headers), data=body_str, timeout=timeout_sec)
    except RequestException as exc:
        raise ApiError(f"HTTP request failed: {exc}", None) from None

    status = resp.status_code
    try:
        return _resp_json(resp)
    except ValueError:
        text = resp.text[:2000]
        raise ApiError(f"Non-JSON response: body={text}", status) from None


def bf_public_getticker(base_url: str, product_code: str) -> Decimal:
    """公開APIで ticker を取得し、ltp（最終取引価格）を返す。"""
    url = base_url.rstrip("/") + f"/v1/getticker?product_code={product_code}"
    data = http_request_json("GET", url, headers={
                             "Content-Type": CONTENT_TYPE_JSON}, body_str=None)

    if not isinstance(data, Mapping):
        raise ApiError(f"getticker invalid response type: {type(data)}", None)

    ltp_raw = data.get("ltp")
    if ltp_raw is None:
        raise ApiError(f"getticker missing ltp: {data}", None)

    ltp = Decimal(str(ltp_raw))
    if ltp <= 0:
        raise ApiError(f"getticker invalid ltp: {ltp}", None)

    return ltp


def bf_private_request(
    method: str,
    base_url: str,
    path: str,
    api_key: str,
    api_secret: str,
    body: Mapping[str, JsonValue] | None = None,
) -> JsonValue:
    """bitFlyer Lightning の Private API を認証付きで叩く共通関数。"""
    method_u = method.upper()
    url = base_url.rstrip("/") + path

    body_str: str | None = None
    if body is not None:
        body_str = json_dumps_compact(body)

    ts = str(int(time.time()))
    sign_text = ts + method_u + path + (body_str or "")
    sign = sign_hmac_sha256_hex(api_secret, sign_text)

    headers = {
        "Content-Type": CONTENT_TYPE_JSON,
        "ACCESS-KEY": api_key,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-SIGN": sign,
    }

    data = http_request_json(method_u, url, headers=headers, body_str=body_str)

    if isinstance(data, Mapping) and "error_message" in data:
        raise ApiError(f"Private API error: {data}", None)

    return data


def bf_get_jpy_available_balance(base_url: str, api_key: str, api_secret: str) -> Decimal:
    """利用可能なJPY残高（available）を返す。"""
    data = bf_private_request(
        "GET", base_url, "/v1/me/getbalance", api_key, api_secret)

    if not isinstance(data, Sequence):
        raise ApiError(f"getbalance invalid response: {data}", None)

    for row in data:
        if not isinstance(row, Mapping):
            continue
        r = cast(BalanceRow, row)
        if r.get("currency_code") == "JPY":
            return Decimal(str(r.get("available", 0)))
    return Decimal("0")


def compute_order_size_btc(buy_amount_jpy: Decimal, ltp_jpy_per_btc: Decimal) -> Decimal:
    """指定JPY金額と価格から、注文サイズ（BTC）を算出（小数8桁切り捨て）。"""
    raw = buy_amount_jpy / ltp_jpy_per_btc
    return raw.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)


def validate_amounts(buy_amount_jpy: Decimal, max_buy_amount_jpy: Decimal | None) -> None:
    """誤設定・事故防止のための購入額チェック。"""
    if buy_amount_jpy < 0:
        raise ConfigError("BUY_AMOUNT_JPY must be >= 0")

    if max_buy_amount_jpy is None:
        return

    if max_buy_amount_jpy <= 0:
        raise ConfigError("MAX_BUY_AMOUNT_JPY must be positive when set")

    if buy_amount_jpy > max_buy_amount_jpy:
        raise ConfigError(
            f"BUY_AMOUNT_JPY({buy_amount_jpy}) exceeds MAX_BUY_AMOUNT_JPY({max_buy_amount_jpy})")


def validate_min_size(product_code: str, size_btc: Decimal) -> None:
    """銘柄ごとの最小注文数量チェック（現状は BTC_JPY のみ）。"""
    if product_code != "BTC_JPY":
        return

    min_size = Decimal("0.001")
    if size_btc < min_size:
        raise ConfigError(
            f"Order size {size_btc} is below minimum {min_size} for {product_code}")


def bf_send_market_buy(
    base_url: str,
    api_key: str,
    api_secret: str,
    product_code: str,
    size_btc: Decimal,
) -> str:
    """成行（MARKET）で買い注文を出す。成功時は acceptance_id を返す。"""
    body: Mapping[str, JsonValue] = {
        "product_code": product_code,
        "child_order_type": "MARKET",
        "side": "BUY",
        "size": float(size_btc),
    }
    data = bf_private_request(
        "POST", base_url, "/v1/me/sendchildorder", api_key, api_secret, body=body)

    if not isinstance(data, Mapping):
        raise ApiError(
            f"sendchildorder invalid response type: {type(data)}", None)

    d = cast(ChildOrderResponse, data)
    acceptance_id = d.get("child_order_acceptance_id")
    if not acceptance_id:
        raise ApiError(f"sendchildorder failed: {data}", None)

    return str(acceptance_id)


def chunk_text(text: str, max_len: int) -> list[str]:
    """テキストを max_len 文字以内のチャンクに分割する（順序維持）。"""
    if max_len <= 0:
        raise ValueError("max_len must be positive")
    if text == "":
        return [""]

    chunks: list[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + max_len, n)
        chunks.append(text[start:end])
        start = end
    return chunks


def _discord_webhook_url(
    webhook_url: str,
    *,
    wait: bool,
    thread_id: str | None,
) -> str:
    """Discord Webhook URL に wait/thread_id を付与したURLを返す。"""
    params: list[str] = []
    if wait:
        params.append("wait=true")
    if thread_id:
        params.append(f"thread_id={thread_id}")
    if not params:
        return webhook_url

    sep = "&" if "?" in webhook_url else "?"
    return f"{webhook_url}{sep}{'&'.join(params)}"


def _discord_post_json(url: str, payload: Mapping[str, JsonValue]) -> Response:
    """Discord Webhook に JSON を POST し、Response を返す。"""
    try:
        return requests.post(url, json=dict(payload), timeout=15)
    except RequestException as exc:
        raise ApiError(
            f"Discord webhook request failed: {exc}", None) from None


def _discord_raise_for_status(resp: Response) -> None:
    """Discord Webhook の HTTP エラーを ApiError に変換する。"""
    if resp.status_code >= 400:
        raise ApiError(
            f"Discord webhook failed: body={resp.text[:300]}", resp.status_code)


def _discord_extract_message_id(resp: Response) -> str:
    """Discord Webhook のレスポンスJSONから message_id を抽出する。"""
    try:
        data = _resp_json(resp)
    except ValueError:
        raise ApiError(
            f"Discord webhook returned non-JSON: body={resp.text[:300]}", resp.status_code) from None

    if not isinstance(data, Mapping):
        raise ApiError(
            f"Discord webhook invalid response: {data}", resp.status_code)

    msg_id = data.get("id")
    if not isinstance(msg_id, str) or msg_id.strip() == "":
        raise ApiError(f"Discord webhook missing id: {data}", resp.status_code)

    return msg_id


def post_discord_webhook(
    webhook_url: str,
    message: str,
    *,
    thread_id: str | None = None,
    max_body: int = 1900,
    wait: bool = True,
) -> str | None:
    """Discord Webhook にメッセージを投稿する。

    注意:
        - content は 2000 文字制限があるため、必要なら分割して投稿する。
        - wait=True の場合のみ、最初の投稿 message_id を返す（Click生成用途）。
        - thread_id 指定時はスレッドに投稿する（Webhookがスレッド投稿対応の場合）。

    Args:
        webhook_url: Discord Webhook URL
        message: 投稿本文
        thread_id: 投稿先スレッドID（未指定ならチャンネル）
        max_body: 分割時の1チャンク最大文字数（安全側に 1900）
        wait: message_id が必要なとき True

    Returns:
        wait=True の場合は最初の message_id、wait=False の場合は None
    """
    if webhook_url.strip() == "":
        raise ValueError("webhook_url must not be empty")

    url = _discord_webhook_url(webhook_url, wait=wait, thread_id=thread_id)

    parts = chunk_text(message, max_body)
    total = len(parts)

    def content_with_prefix(index: int, part: str) -> str:
        prefix = f"({index}/{total}) " if total > 1 else ""
        return prefix + part

    first_resp = _discord_post_json(
        url, {"content": content_with_prefix(1, parts[0])})
    _discord_raise_for_status(first_resp)
    first_id = _discord_extract_message_id(first_resp) if wait else None

    for index, part in enumerate(parts[1:], start=2):
        resp = _discord_post_json(
            url, {"content": content_with_prefix(index, part)})
        _discord_raise_for_status(resp)
        if index < total:
            time.sleep(0.2)

    return first_id


def discord_message_link(guild_id: str, channel_id: str, message_id: str) -> str:
    """DiscordメッセージのジャンプURLを作る。"""
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"


def create_discord_thread(
    *,
    bot_token: str,
    channel_id: str,
    message_id: str,
    name: str,
    auto_archive_duration: int = 1440,
) -> str:
    """Discord のメッセージからスレッドを作成し、thread_id を返す。

    Args:
        bot_token: Discord Bot Token
        channel_id: 親メッセージがあるチャンネルID
        message_id: 親メッセージID
        name: スレッド名
        auto_archive_duration: 自動アーカイブ（分）。既定: 1440(24h)

    Raises:
        ApiError: 作成失敗

    Returns:
        thread_id
    """
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}/threads"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": CONTENT_TYPE_JSON,
    }
    payload = {"name": name, "auto_archive_duration": auto_archive_duration}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
    except RequestException as exc:
        raise ApiError(f"Create thread request failed: {exc}", None) from None

    if resp.status_code >= 400:
        raise ApiError(
            f"Create thread failed: body={resp.text[:300]}", resp.status_code)

    try:
        data = _resp_json(resp)
    except ValueError:
        raise ApiError(
            f"Create thread returned non-JSON: body={resp.text[:300]}", resp.status_code) from None

    if not isinstance(data, Mapping):
        raise ApiError(
            f"Create thread invalid response: {data}", resp.status_code)

    thread_id = data.get("id")
    if not isinstance(thread_id, str) or thread_id.strip() == "":
        raise ApiError(f"Create thread missing id: {data}", resp.status_code)

    return thread_id


def _ntfy_should_use_title_header(title: str) -> bool:
    """Title ヘッダを利用できるか判定する（ASCII のみ許可）。"""
    return title.isascii()


def _ntfy_build_headers_base(
    title: str,
    *,
    use_title_header: bool,
    click_url: str | None,
    token: str | None,
) -> dict[str, str]:
    """送信に使う基本ヘッダを組み立てる。"""
    headers: dict[str, str] = {}
    if use_title_header:
        headers["Title"] = title
    if click_url:
        headers["Click"] = click_url
    if token and token.strip() != "":
        headers["Authorization"] = f"Bearer {token.strip()}"
    return headers


def _ntfy_sanitize_headers(headers: dict[str, str]) -> dict[str, str]:
    """latin-1 にエンコードできないヘッダ値を除外する。"""
    safe: dict[str, str] = {}
    for key, value in headers.items():
        try:
            value.encode("latin-1")
        except UnicodeEncodeError:
            continue
        safe[key] = value
    return safe


def _ntfy_prepare_message_for_chunk(title: str, message: str, *, use_title_header: bool) -> str:
    """分割対象となるメッセージ本文を作る。"""
    if use_title_header:
        return message
    return f"{title}\n{message}"


def _ntfy_build_part(
    part: str,
    *,
    index: int,
    total: int,
    use_title_header: bool,
) -> str:
    """分割送信時の本文（prefix含む）を作る。"""
    if total <= 1:
        return part
    if use_title_header:
        return part
    return f"({index}/{total}) {part}"


def post_ntfy_notify(
    topic_url: str,
    title: str,
    message: str,
    *,
    click_url: str | None = None,
    token: str | None = None,
    max_body: int = 3500,
) -> None:
    """ntfy へ通知を送信する（分割送信＋送信間sleep）。

    注意:
        requests/urllib3 は HTTP ヘッダを latin-1 でエンコードするため、ヘッダ値に
        日本語などが混入すると `UnicodeEncodeError` で送信に失敗する。
        そのため本関数では以下を行う。

        - Title ヘッダは ASCII のときのみ設定
        - さらに保険として「latin-1 で encode できないヘッダ値」を自動除外
        - Title ヘッダが使えない場合は本文先頭に title を埋め込む
    """
    if topic_url.strip() == "":
        raise ValueError("topic_url must not be empty")

    use_title_header = _ntfy_should_use_title_header(title)
    headers_base = _ntfy_build_headers_base(
        title,
        use_title_header=use_title_header,
        click_url=click_url,
        token=token,
    )

    message_for_chunk = _ntfy_prepare_message_for_chunk(
        title, message, use_title_header=use_title_header)
    parts = chunk_text(message_for_chunk, max_body)
    total = len(parts)

    for index, part in enumerate(parts, start=1):
        headers = dict(headers_base)
        if total > 1 and use_title_header:
            headers["Title"] = f"({index}/{total}) {title}"

        body = _ntfy_build_part(
            part, index=index, total=total, use_title_header=use_title_header)

        try:
            resp = requests.post(
                topic_url,
                headers=_ntfy_sanitize_headers(headers),
                data=body.encode("utf-8"),
                timeout=15,
            )
        except RequestException as exc:
            raise ApiError(f"ntfy request failed: {exc}", None) from None

        if resp.status_code >= 400:
            raise ApiError(
                f"ntfy failed: body={resp.text[:300]}", resp.status_code)

        if index < total:
            time.sleep(0.2)


def format_ntfy_error_body(exc: Exception) -> str:
    """ntfy 用のエラー本文を生成する（スタックトレース無し）。"""
    return (
        f"Error ：{type(exc).__name__}: {exc}\n"
        f"日時 ：{now_jst_str()}\n"
        "🔔 詳細はタップしてDiscordへ"
    )


def log_error(exc: Exception, *, with_trace: bool) -> None:
    """標準出力へエラーを出力する（スタックトレースON/OFF）。"""
    if with_trace:
        print(traceback.format_exc())
    else:
        print(f"ERROR: {type(exc).__name__}: {exc}")


def notify_error_discord_thread_and_ntfy(
    exc: Exception,
    *,
    discord_webhook_url: str | None,
    discord_guild_id: str | None,
    discord_channel_id: str | None,
    discord_bot_token: str | None,
    ntfy_topic_url: str | None,
    ntfy_token: str | None,
    notify_on_discord: bool,
    notify_on_ntfy: bool,
) -> None:
    """エラー通知を Discord(スレッド) + ntfy に送信する。

    - Discord: 親メッセージ（要約）→ スレッドに詳細（スタックトレース）
    - ntfy: 要約本文（スタックトレース無し）+ Click で Discord 親にジャンプ
    """
    # まず Discord 側（親を作って message_id を得る）
    link: str | None = None
    parent_id: str | None = None

    if notify_on_discord and discord_webhook_url:
        parent_subject = "【bitfler-dca】 Error通知"
        parent_body = f"{parent_subject}\nError: {type(exc).__name__}: {exc}\n日時: {now_jst_str()}"
        parent_id = cast(str | None, post_discord_webhook(
            discord_webhook_url, parent_body, wait=True))

        if parent_id and discord_guild_id and discord_channel_id:
            link = discord_message_link(
                discord_guild_id, discord_channel_id, parent_id)

        # スレッド化（Bot Token + channel_id + parent_id が揃った場合のみ）
        if parent_id and discord_bot_token and discord_channel_id:
            thread_name = f"bitfler-dca ERROR {now_jst_str()[:16]}"
            thread_id = create_discord_thread(
                bot_token=discord_bot_token,
                channel_id=discord_channel_id,
                message_id=parent_id,
                name=thread_name,
            )
            # 詳細はスレッドへ
            detail = traceback.format_exc()
            post_discord_webhook(discord_webhook_url, detail,
                                 thread_id=thread_id, wait=False)
        else:
            # Bot Token 未設定などの場合、親メッセージに続けて詳細を投稿（スレッドなし）
            detail = traceback.format_exc()
            post_discord_webhook(discord_webhook_url, detail, wait=False)

    # 次に ntfy（Click はDiscord親リンクがあれば付ける）
    if notify_on_ntfy and ntfy_topic_url:
        post_ntfy_notify(
            ntfy_topic_url,
            "【bitfler-dca】 Error通知",
            format_ntfy_error_body(exc),
            click_url=link,
            token=ntfy_token,
        )


def notify_discord_and_ntfy(
    subject: str,
    body: str,
    *,
    discord_webhook_url: str | None,
    discord_guild_id: str | None,
    discord_channel_id: str | None,
    ntfy_topic_url: str | None,
    ntfy_token: str | None,
    notify_on_discord: bool,
    notify_on_ntfy: bool,
) -> None:
    """通常通知（成功・SKIPなど）を Discord と ntfy に送る（Discord→ntfy の順）。"""
    link: str | None = None

    if notify_on_discord and discord_webhook_url:
        msg_id = cast(str | None, post_discord_webhook(
            discord_webhook_url, f"{subject}\n{body}", wait=True))
        if msg_id and discord_guild_id and discord_channel_id:
            link = discord_message_link(
                discord_guild_id, discord_channel_id, msg_id)

    if notify_on_ntfy and ntfy_topic_url:
        post_ntfy_notify(
            ntfy_topic_url,
            subject,
            body,
            click_url=link,
            token=ntfy_token,
        )


def parse_time_ranges_jst(raw: str) -> list[tuple[int, int]]:
    """'HH:MM-HH:MM,HH:MM-HH:MM' を JSTの分単位レンジにパースする。"""
    raw = raw.strip()
    if raw == "":
        return []

    ranges: list[tuple[int, int]] = []
    items = [x.strip() for x in raw.split(",") if x.strip() != ""]
    for item in items:
        if "-" not in item:
            raise ConfigError(f"Invalid SKIP_TIME_RANGES_JST item: {item}")

        a, b = [x.strip() for x in item.split("-", 1)]
        try:
            sh, sm = a.split(":")
            eh, em = b.split(":")
            start = int(sh) * 60 + int(sm)
            end = int(eh) * 60 + int(em)
        except (ValueError, TypeError):
            raise ConfigError(f"Invalid time range: {item}") from None

        if start < 0 or start >= 24 * 60 or end < 0 or end > 24 * 60 or start == end:
            raise ConfigError(f"Invalid time range: {item}")
        ranges.append((start, end))
    return ranges


def is_now_in_skip_range_jst(ranges: list[tuple[int, int]]) -> bool:
    """現在時刻（JST）が指定レンジ内なら True。日跨ぎにも対応。"""
    if not ranges:
        return False

    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    cur = now.hour * 60 + now.minute

    for start, end in ranges:
        if start < end:
            if start <= cur < end:
                return True
        else:
            if cur >= start or cur < end:
                return True
    return False


def is_maintenance_like_api_error(exc: Exception) -> bool:
    """502/503/504 など「メンテ/障害っぽい」エラーなら True。"""
    if isinstance(exc, ApiError) and exc.status_code in (502, 503, 504):
        return True
    msg = str(exc).lower()
    return ("maintenance" in msg) or ("temporarily" in msg and "unavailable" in msg)


def state_paths(state_dir: str) -> tuple[Path, Path]:
    """状態保存ディレクトリとファイルパスを返す。"""
    d = Path(state_dir)
    f = d / "alert_state.json"
    return d, f


def load_alert_state(state_file: Path) -> dict[str, int]:
    """アラート送信状態を読み込む（存在しなければ空）。"""
    if not state_file.exists():
        return {}
    try:
        raw = json.loads(state_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}

    if not isinstance(raw, Mapping):
        return {}

    state: dict[str, int] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, int):
            state[k] = v
    return state


def save_alert_state(state_dir: Path, state_file: Path, state: Mapping[str, int]) -> None:
    """アラート送信状態を保存する（ディレクトリが無ければ作る）。"""
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(
        dict(state), ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


@dataclass(frozen=True)
class SuccessTotals:
    """成功時の累積サマリ（約定数/金額）。"""

    total_size_btc: Decimal
    total_amount_jpy: Decimal
    total_count: int


def _success_state_paths(state_dir: str) -> tuple[Path, Path]:
    """成功サマリの状態保存パスを返す。"""
    d = Path(state_dir)
    f = d / "success_state.json"
    return d, f


def load_success_totals(state_file: Path) -> SuccessTotals:
    """成功サマリの状態を読み込む（存在しなければ 0 で初期化）。"""
    if not state_file.exists():
        return SuccessTotals(Decimal("0"), Decimal("0"), 0)

    try:
        raw = json.loads(state_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return SuccessTotals(Decimal("0"), Decimal("0"), 0)

    if not isinstance(raw, Mapping):
        return SuccessTotals(Decimal("0"), Decimal("0"), 0)

    size_raw = raw.get("total_size_btc", "0")
    amount_raw = raw.get("total_amount_jpy", "0")
    count_raw = raw.get("total_count", 0)

    try:
        size = Decimal(str(size_raw))
        amount = Decimal(str(amount_raw))
        count = int(count_raw)
    except (ValueError, ArithmeticError):
        return SuccessTotals(Decimal("0"), Decimal("0"), 0)

    return SuccessTotals(size, amount, count)


def save_success_totals(state_dir: Path, state_file: Path, totals: SuccessTotals) -> None:
    """成功サマリの状態を保存する（ディレクトリが無ければ作る）。"""
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "total_size_btc": str(totals.total_size_btc),
        "total_amount_jpy": str(totals.total_amount_jpy),
        "total_count": totals.total_count,
    }
    state_file.write_text(json.dumps(
        payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def compute_and_update_success_totals(
    result: DcaResult,
    *,
    state_dir_raw: str,
) -> tuple[Decimal, Decimal, SuccessTotals]:
    """成功時の約定情報と累積情報を計算し、累積を更新する。

    Returns:
        (executed_size_btc, executed_amount_jpy, updated_totals)
    """
    status = result.get("status")
    if status != "OK":
        return Decimal("0"), Decimal("0"), SuccessTotals(Decimal("0"), Decimal("0"), 0)

    size = result.get("size") or Decimal("0")
    amount = result.get("buy_amount_jpy") or Decimal("0")
    acc = result.get("acceptance_id")
    reason = result.get("reason", "")

    executed = acc is not None and reason != "DRY_RUN"
    executed_size = size if executed else Decimal("0")
    executed_amount = amount if executed else Decimal("0")

    state_dir, state_file = _success_state_paths(state_dir_raw)
    current = load_success_totals(state_file)

    updated = SuccessTotals(
        total_size_btc=current.total_size_btc + executed_size,
        total_amount_jpy=current.total_amount_jpy + executed_amount,
        total_count=current.total_count + (1 if executed else 0),
    )

    save_success_totals(state_dir, state_file, updated)
    return executed_size, executed_amount, updated


def error_fingerprint(exc: Exception) -> str:
    """同一エラー判定用のフィンガープリントを作る（秘密情報を含めない想定）。"""
    msg = str(exc)
    head = msg[:200]
    return f"{type(exc).__name__}:{head}"


def should_send_alert(state: Mapping[str, int], fp: str, throttle_sec: int, now_epoch: int) -> bool:
    """スロットリング判定：同一fpが throttle_sec 以内に送られていれば False。"""
    last = state.get(fp)
    if last is None:
        return True
    return (now_epoch - last) >= throttle_sec


def run_dca() -> DcaResult:
    """DCAを実行し、結果情報を dict（TypedDict）で返す。"""
    base_url = env("BITFLYER_BASE_URL", "https://api.bitflyer.com")
    api_key = env("BITFLYER_API_KEY")
    api_secret = env("BITFLYER_API_SECRET")

    product_code = env("PRODUCT_CODE")
    buy_amount_jpy = Decimal(env("BUY_AMOUNT_JPY"))
    dry_run = bool_env("DRY_RUN", "false")

    max_raw = os.getenv("MAX_BUY_AMOUNT_JPY", "").strip()
    max_buy = Decimal(max_raw) if max_raw != "" else None

    validate_amounts(buy_amount_jpy, max_buy)

    if buy_amount_jpy == 0:
        return {"status": "SKIP", "reason": "BUY_AMOUNT_JPY is 0", "product_code": product_code, "buy_amount_jpy": buy_amount_jpy}

    skip_ranges_raw = os.getenv("SKIP_TIME_RANGES_JST", "").strip()
    skip_ranges = parse_time_ranges_jst(
        skip_ranges_raw) if skip_ranges_raw != "" else []
    if is_now_in_skip_range_jst(skip_ranges):
        return {"status": "SKIP_TIME", "reason": f"In skip time range (JST): {skip_ranges_raw}", "product_code": product_code, "buy_amount_jpy": buy_amount_jpy}

    jpy_avail = bf_get_jpy_available_balance(base_url, api_key, api_secret)
    if jpy_avail < buy_amount_jpy:
        raise ConfigError(
            f"Insufficient JPY balance: available={jpy_avail}, required={buy_amount_jpy}")

    ltp = bf_public_getticker(base_url, product_code)
    size = compute_order_size_btc(buy_amount_jpy, ltp)
    validate_min_size(product_code, size)

    if dry_run:
        return {
            "status": "OK",
            "reason": "DRY_RUN",
            "product_code": product_code,
            "buy_amount_jpy": buy_amount_jpy,
            "ltp": ltp,
            "size": size,
            "acceptance_id": None,
        }

    acceptance_id = bf_send_market_buy(
        base_url, api_key, api_secret, product_code, size)
    return {
        "status": "OK",
        "reason": "ORDER_SENT",
        "product_code": product_code,
        "buy_amount_jpy": buy_amount_jpy,
        "ltp": ltp,
        "size": size,
        "acceptance_id": acceptance_id,
    }


def _try_notify_all(
    subject: str,
    message: str,
    *,
    discord_webhook_url: str | None,
    discord_guild_id: str | None,
    discord_channel_id: str | None,
    ntfy_topic_url: str | None,
    ntfy_token: str | None,
    notify_on_discord: bool,
    notify_on_ntfy: bool,
) -> None:
    """通知失敗は本体処理を邪魔しない方針のため、ここで握りつぶす。"""
    try:
        notify_discord_and_ntfy(
            subject,
            message,
            discord_webhook_url=discord_webhook_url,
            discord_guild_id=discord_guild_id,
            discord_channel_id=discord_channel_id,
            ntfy_topic_url=ntfy_topic_url,
            ntfy_token=ntfy_token,
            notify_on_discord=notify_on_discord,
            notify_on_ntfy=notify_on_ntfy,
        )
    except ApiError:
        return


def _handle_result(
    result: DcaResult,
    notify: NotifyConfig,
    *,
    end_datetime_jst: str,
    duration_sec: float,
    executed_size_btc: Decimal,
    executed_amount_jpy: Decimal,
    totals: SuccessTotals | None,
) -> None:
    """run_dca() の結果を解釈し、必要に応じて通知する。"""
    status = result.get("status")

    if status == "SKIP_TIME":
        reason = result.get("reason", "")
        print(
            f"SKIP_TIME: product={result.get('product_code')} "
            f"jpy={result.get('buy_amount_jpy')} reason={reason}"
        )
        if notify.notify_on_skip_time:
            subject = f"[bitflyer-dca] {now_jst_str()} {script_name()} SKIP_TIME"
            body = f"reason: {reason}"
            _try_notify_all(
                subject,
                body,
                discord_webhook_url=notify.discord_webhook_url,
                discord_guild_id=notify.discord_guild_id,
                discord_channel_id=notify.discord_channel_id,
                ntfy_topic_url=notify.ntfy_topic_url,
                ntfy_token=notify.ntfy_token,
                notify_on_discord=notify.notify_on_discord,
                notify_on_ntfy=notify.notify_on_ntfy,
            )
        return

    if status == "SKIP":
        reason = result.get("reason", "")
        print(
            f"SKIP: product={result.get('product_code')} "
            f"jpy={result.get('buy_amount_jpy')} reason={reason}"
        )
        return

    # OK
    product = result.get("product_code")
    jpy = result.get("buy_amount_jpy")
    ltp = result.get("ltp")
    size = result.get("size")
    acc = result.get("acceptance_id")

    print(
        f"OK: product={product} jpy={jpy} ltp={ltp} size={size} acceptance_id={acc}")
    if not notify.notify_on_success:
        return
    # 成功時の通知（NOTIFY_ON_SUCCESS で制御）
    subject = f"[bitflyer-dca] {end_datetime_jst} {script_name()} OK"

    total_size = totals.total_size_btc if totals is not None else Decimal("0")
    total_amount = totals.total_amount_jpy if totals is not None else Decimal(
        "0")
    total_count = totals.total_count if totals is not None else 0

    body = (
        f"終了日時: {end_datetime_jst}\n"
        f"約定数(BTC): {executed_size_btc}\n"
        f"約定金額(JPY): {executed_amount_jpy}\n"
        f"トータル約定数(回): {total_count}\n"
        f"トータル約定数(BTC): {total_size}\n"
        f"トータル約定金額(JPY): {total_amount}\n"
        f"処理時間(sec): {duration_sec:.3f}\n"
        f"product={product}\n"
        f"jpy={jpy}\n"
        f"ltp={ltp}\n"
        f"size={size}\n"
        f"acceptance_id={acc}"
    )

    _try_notify_all(
        subject,
        body,
        discord_webhook_url=notify.discord_webhook_url,
        discord_guild_id=notify.discord_guild_id,
        discord_channel_id=notify.discord_channel_id,
        ntfy_topic_url=notify.ntfy_topic_url,
        ntfy_token=notify.ntfy_token,
        notify_on_discord=notify.notify_on_discord,
        notify_on_ntfy=notify.notify_on_ntfy,
    )


def _handle_exception(
    exc: Exception,
    *,
    discord_webhook_url: str | None,
    discord_guild_id: str | None,
    discord_channel_id: str | None,
    discord_bot_token: str | None,
    ntfy_topic_url: str | None,
    ntfy_token: str | None,
    notify_on_discord: bool,
    notify_on_ntfy: bool,
    notify_on_skip_api_maint: bool,
    state_dir_raw: str,
    throttle_sec: int,
    log_stacktrace: bool,
) -> None:
    """例外を分類し、通知やスロットリングを適用する。"""
    log_error(exc, with_trace=log_stacktrace)

    if is_maintenance_like_api_error(exc):
        reason = f"Maintenance-like error: {exc}"
        print(f"SKIP_API_MAINT: {reason}")
        if notify_on_skip_api_maint:
            notify_error_discord_thread_and_ntfy(
                exc,
                discord_webhook_url=discord_webhook_url,
                discord_guild_id=discord_guild_id,
                discord_channel_id=discord_channel_id,
                discord_bot_token=discord_bot_token,
                ntfy_topic_url=ntfy_topic_url,
                ntfy_token=ntfy_token,
                notify_on_discord=notify_on_discord,
                notify_on_ntfy=notify_on_ntfy,
            )
        return

    fp = error_fingerprint(exc)
    now_ep = int(time.time())

    state_dir, state_file = state_paths(state_dir_raw)
    state = load_alert_state(state_file)

    if should_send_alert(state, fp, throttle_sec, now_ep):
        notify_error_discord_thread_and_ntfy(
            exc,
            discord_webhook_url=discord_webhook_url,
            discord_guild_id=discord_guild_id,
            discord_channel_id=discord_channel_id,
            discord_bot_token=discord_bot_token,
            ntfy_topic_url=ntfy_topic_url,
            ntfy_token=ntfy_token,
            notify_on_discord=notify_on_discord,
            notify_on_ntfy=notify_on_ntfy,
        )
        state[fp] = now_ep
        save_alert_state(state_dir, state_file, state)
    else:
        print(f"ALERT_THROTTLED: fp={fp} within {throttle_sec}s")


def main() -> None:
    """エントリポイント。"""
    start_epoch = time.time()
    discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip() or None
    discord_guild_id = os.getenv("DISCORD_GUILD_ID", "").strip() or None
    discord_channel_id = os.getenv("DISCORD_CHANNEL_ID", "").strip() or None
    discord_bot_token = os.getenv("DISCORD_BOT_TOKEN", "").strip() or None

    ntfy_topic_url = os.getenv("NTFY_TOPIC_URL", "").strip() or None
    ntfy_token = os.getenv("NTFY_TOKEN", "").strip() or None

    notify_on_discord = bool_env("NOTIFY_ON_DISCORD", "true")
    notify_on_ntfy = bool_env("NOTIFY_ON_NTFY", "true")

    notify_on_success = bool_env("NOTIFY_ON_SUCCESS", "true")

    notify_on_skip_time = bool_env("NOTIFY_ON_SKIP_TIME", "false")
    notify_on_skip_api_maint = bool_env("NOTIFY_ON_SKIP_API_MAINT", "true")
    log_stacktrace = bool_env("LOG_STACKTRACE", "true")

    throttle_sec = int(os.getenv("ALERT_THROTTLE_SECONDS", "3600"))
    state_dir_raw = os.getenv("STATE_DIR", ".state")

    notify = NotifyConfig(
        discord_webhook_url=discord_webhook_url,
        discord_guild_id=discord_guild_id,
        discord_channel_id=discord_channel_id,
        discord_bot_token=discord_bot_token,
        ntfy_topic_url=ntfy_topic_url,
        ntfy_token=ntfy_token,
        notify_on_discord=notify_on_discord,
        notify_on_ntfy=notify_on_ntfy,
        notify_on_skip_time=notify_on_skip_time,
        notify_on_success=notify_on_success,
    )

    try:
        result = run_dca()
        end_datetime_jst = now_jst_str()
        duration_sec = time.time() - start_epoch
        executed_size_btc, executed_amount_jpy, totals = compute_and_update_success_totals(
            result,
            state_dir_raw=state_dir_raw,
        )
        _handle_result(
            result,
            notify,
            end_datetime_jst=end_datetime_jst,
            duration_sec=duration_sec,
            executed_size_btc=executed_size_btc,
            executed_amount_jpy=executed_amount_jpy,
            totals=totals,
        )
    except Exception as exc:
        _handle_exception(
            exc,
            discord_webhook_url=discord_webhook_url,
            discord_guild_id=discord_guild_id,
            discord_channel_id=discord_channel_id,
            discord_bot_token=discord_bot_token,
            ntfy_topic_url=ntfy_topic_url,
            ntfy_token=ntfy_token,
            notify_on_discord=notify_on_discord,
            notify_on_ntfy=notify_on_ntfy,
            notify_on_skip_api_maint=notify_on_skip_api_maint,
            state_dir_raw=state_dir_raw,
            throttle_sec=throttle_sec,
            log_stacktrace=log_stacktrace,
        )


if __name__ == "__main__":
    print(f"START: {now_jst_str()} - {script_name()}")
    main()
    print(f"END: {now_jst_str()} - {script_name()}")
