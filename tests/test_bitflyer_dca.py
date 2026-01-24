"""
bitflyer_dca.py のユニットテスト。

方針:
- 外部I/O（HTTP、sleep、時刻）を monkeypatch で固定し、純粋にロジックを検証する
- coverage --branch で 100% を維持する（SonarQubeの一般的な期待値にも合わせやすい）
"""

from __future__ import annotations


from decimal import Decimal
from typing import Mapping

import pytest

import src.bitflyer_dca as m


class DummyResp:
    """requests.Response 互換の最小ダミー実装（テスト用）。"""

    def __init__(self, status_code: int = 200, text: str = "ok") -> None:
        """初期化。

        Args:
            status_code: HTTPステータスコード
            text: レスポンス本文（エラー用）
        """
        self.status_code = status_code
        self.text = text

    def json(self):  # noqa: ANN001 - テスト用のダミーで簡潔にする
        """JSONレスポンスを返す（テストでは未使用）。"""
        return {"ok": True}


def set_env(monkeypatch: pytest.MonkeyPatch, **kwargs: str) -> None:
    """環境変数をまとめて設定する（テスト補助）。"""
    for k, v in kwargs.items():
        monkeypatch.setenv(k, v)


def test_chunk_text() -> None:
    """chunk_text が意図どおりに分割することを確認する。"""
    assert m.chunk_text("abcdef", 2) == ["ab", "cd", "ef"]
    assert m.chunk_text("", 10) == [""]
    with pytest.raises(ValueError):
        m.chunk_text("x", 0)


def test_parse_time_ranges_jst() -> None:
    """時刻レンジのパースが正しく行われることを確認する。"""
    assert m.parse_time_ranges_jst("04:00-05:00") == [(240, 300)]
    assert m.parse_time_ranges_jst("23:00-02:00") == [(1380, 120)]
    with pytest.raises(m.ConfigError):
        m.parse_time_ranges_jst("bad")


def test_should_send_alert() -> None:
    """通知スロットリング判定が期待通りであることを確認する。"""
    st: Mapping[str, int] = {}
    assert m.should_send_alert(st, "fp", 3600, 1000) is True
    st2 = {"fp": 999}
    assert m.should_send_alert(st2, "fp", 3600, 1000) is False
    assert m.should_send_alert(st2, "fp", 1, 1000) is True


def test_post_discord_webhook_splits_and_sleeps(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discord Webhook が分割送信され、sleep が呼ばれることを確認する。"""
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    calls: list[dict] = []

    class Resp:
        status_code = 200
        text = '{"id":"mid"}'

        def __init__(self, payload: dict, *, include_id: bool) -> None:
            self._payload = payload
            self._include_id = include_id

        def json(self):  # noqa: ANN001
            # 最初のレスポンスのみ id を返す
            if self._include_id:
                return {"id": "mid"}
            return {"ok": True}

    first = {"done": False}

    def fake_post(url, json=None, timeout=None):  # noqa: ANN001
        calls.append({"url": url, "json": json})
        include_id = not first["done"]
        first["done"] = True
        return Resp(json, include_id=include_id)

    monkeypatch.setattr(m.requests, "post", fake_post)

    mid = m.post_discord_webhook(
        "https://discord.example/webhook", "a" * 4000, max_body=1900)
    assert mid == "mid"
    assert len(calls) == 3
    assert calls[0]["url"].endswith("wait=true")
    assert calls[0]["json"]["content"].startswith("(1/3) ")


def test_run_dca_skip_amount_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """BUY_AMOUNT_JPY=0 の場合にSKIPとなることを確認する。"""
    set_env(monkeypatch, BITFLYER_API_KEY="k", BITFLYER_API_SECRET="s",
            PRODUCT_CODE="BTC_JPY", BUY_AMOUNT_JPY="0")
    r = m.run_dca()
    assert r["status"] == "SKIP"


def test_run_dca_skip_time_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """SKIP_TIME_RANGES_JST に該当する場合にSKIP_TIMEとなることを確認する。"""
    monkeypatch.setattr(m, "is_now_in_skip_range_jst", lambda _: True)
    set_env(monkeypatch, BITFLYER_API_KEY="k", BITFLYER_API_SECRET="s",
            PRODUCT_CODE="BTC_JPY", BUY_AMOUNT_JPY="20000", SKIP_TIME_RANGES_JST="04:00-05:00")
    r = m.run_dca()
    assert r["status"] == "SKIP_TIME"


def test_run_dca_insufficient_balance(monkeypatch: pytest.MonkeyPatch) -> None:
    """残高不足時に ConfigError となることを確認する。"""
    set_env(monkeypatch, BITFLYER_API_KEY="k", BITFLYER_API_SECRET="s",
            PRODUCT_CODE="BTC_JPY", BUY_AMOUNT_JPY="20000")
    monkeypatch.setattr(m, "bf_get_jpy_available_balance", lambda *a, **k: Decimal("1"))  # noqa: ANN001
    with pytest.raises(m.ConfigError):
        m.run_dca()


def test_run_dca_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """DRY_RUN=true の場合に注文せずOKとなることを確認する。"""
    set_env(monkeypatch, BITFLYER_API_KEY="k", BITFLYER_API_SECRET="s",
            PRODUCT_CODE="BTC_JPY", BUY_AMOUNT_JPY="20000", DRY_RUN="true")
    monkeypatch.setattr(m, "bf_get_jpy_available_balance", lambda *a, **k: Decimal("999999"))  # noqa: ANN001
    monkeypatch.setattr(m, "bf_public_getticker", lambda *a, **k: Decimal("10000000"))  # noqa: ANN001
    r = m.run_dca()
    assert r["status"] == "OK"
    assert r["acceptance_id"] is None


def test_run_dca_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """通常実行で注文IDが返ることを確認する。"""
    set_env(monkeypatch, BITFLYER_API_KEY="k", BITFLYER_API_SECRET="s",
            PRODUCT_CODE="BTC_JPY", BUY_AMOUNT_JPY="20000", DRY_RUN="false")
    monkeypatch.setattr(m, "bf_get_jpy_available_balance", lambda *a, **k: Decimal("999999"))  # noqa: ANN001
    monkeypatch.setattr(m, "bf_public_getticker", lambda *a, **k: Decimal("10000000"))  # noqa: ANN001
    monkeypatch.setattr(m, "bf_send_market_buy", lambda *a, **k: "accept")  # noqa: ANN001
    r = m.run_dca()
    assert r["status"] == "OK"
    assert r["acceptance_id"] == "accept"


def test_main_success_notify(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """正常終了時に Discord/ntfy 通知が行われ、本文に必要情報が含まれることを確認する。"""
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    monkeypatch.setenv("DISCORD_WEBHOOK_URL",
                       "https://discord.example/webhook")
    monkeypatch.setenv("NTFY_TOPIC_URL", "https://ntfy.example/topic")
    monkeypatch.setenv("NOTIFY_ON_DISCORD", "true")
    monkeypatch.setenv("NOTIFY_ON_NTFY", "true")
    monkeypatch.setenv("NOTIFY_ON_SUCCESS", "true")  # true なら成功通知する

    # run_dca を成功で固定
    monkeypatch.setattr(
        m,
        "run_dca",
        lambda: {
            "status": "OK",
            "reason": "ORDER_SENT",
            "product_code": "BTC_JPY",
            "buy_amount_jpy": Decimal("20000"),
            "ltp": Decimal("100"),
            "size": Decimal("0.2"),
            "acceptance_id": "id",
        },
    )

    sent: list[tuple[str, str]] = []

    def fake_notify_all(subject: str, body: str, **kwargs):  # noqa: ANN001
        sent.append((subject, body))

    monkeypatch.setattr(m, "_try_notify_all", fake_notify_all)
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    m.main()

    assert sent, "success notify must be sent"
    subject, body = sent[0]
    assert " OK" in subject
    assert "終了日時:" in body
    assert "約定数(BTC):" in body
    assert "約定金額(JPY):" in body
    assert "トータル約定数(回):" in body
    assert "トータル約定金額(JPY):" in body
    assert "処理時間(sec):" in body


def test_main_error_throttled(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """同一エラーが連続した場合にスロットリングで通知回数が抑制されることを確認する。"""
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    monkeypatch.setenv("ALERT_THROTTLE_SECONDS", "3600")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL",
                       "https://discord.example/webhook")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "")
    monkeypatch.setenv("DISCORD_GUILD_ID", "g")
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "c")
    monkeypatch.setenv("NTFY_TOPIC_URL", "https://ntfy.example/topic")
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    def boom() -> m.DcaResult:
        raise RuntimeError("boom")

    monkeypatch.setattr(m, "run_dca", boom)

    sent = {"discord": 0, "ntfy": 0}

    class DiscordResp:
        status_code = 200
        text = '{"id":"mid"}'

        def json(self):  # noqa: ANN001
            return {"id": "mid"}

    class NtfyResp:
        status_code = 200
        text = "ok"

        def json(self):  # noqa: ANN001
            return {"ok": True}

    def fake_post(url, headers=None, data=None, json=None, timeout=None):  # noqa: ANN001
        if str(url).startswith("https://discord.example"):
            sent["discord"] += 1
            return DiscordResp()
        if str(url).startswith("https://ntfy.example"):
            sent["ntfy"] += 1
            return NtfyResp()
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(m.requests, "post", fake_post)

    m.main()
    assert sent["discord"] == 2
    assert sent["ntfy"] == 1

    m.main()
    assert sent["discord"] == 2
    assert sent["ntfy"] == 1


def test_main_maintenance_like_skip_notify(monkeypatch: pytest.MonkeyPatch) -> None:
    """APIメンテっぽいエラーがSKIP_API_MAINTとして扱われ、通知されることを確認する。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL",
                       "https://discord.example/webhook")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "")
    monkeypatch.setenv("NTFY_TOPIC_URL", "https://ntfy.example/topic")
    monkeypatch.setenv("NOTIFY_ON_SKIP_API_MAINT", "true")
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    def boom():
        raise m.ApiError("bad gateway", 502)

    monkeypatch.setattr(m, "run_dca", boom)

    sent = {"discord": 0, "ntfy": 0}

    class DiscordResp:
        status_code = 200
        text = '{"id":"mid"}'

        def json(self):  # noqa: ANN001
            return {"id": "mid"}

    class NtfyResp:
        status_code = 200
        text = "ok"

        def json(self):  # noqa: ANN001
            return {"ok": True}

    def fake_post(url, headers=None, data=None, json=None, timeout=None):  # noqa: ANN001
        if str(url).startswith("https://discord.example"):
            sent["discord"] += 1
            return DiscordResp()
        if str(url).startswith("https://ntfy.example"):
            sent["ntfy"] += 1
            return NtfyResp()
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(m.requests, "post", fake_post)

    m.main()
    assert sent["discord"] >= 1
    assert sent["ntfy"] >= 1


def test_main_skip_time_notify(monkeypatch: pytest.MonkeyPatch) -> None:
    """SKIP_TIMEの通知が有効な場合に Discord→ntfy 送信されることを確認する。"""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL",
                       "https://discord.example/webhook")
    monkeypatch.setenv("NTFY_TOPIC_URL", "https://ntfy.example/topic")
    monkeypatch.setenv("NOTIFY_ON_SKIP_TIME", "true")
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    monkeypatch.setattr(m, "run_dca", lambda: {
        "status": "SKIP_TIME",
        "reason": "x",
        "product_code": "BTC_JPY",
        "buy_amount_jpy": Decimal("20000"),
    })

    sent = {"discord": 0, "ntfy": 0}

    class DiscordResp:
        status_code = 200
        text = '{"id":"mid"}'

        def json(self):  # noqa: ANN001
            return {"id": "mid"}

    class NtfyResp:
        status_code = 200
        text = "ok"

        def json(self):  # noqa: ANN001
            return {"ok": True}

    def fake_post(url, headers=None, data=None, json=None, timeout=None):  # noqa: ANN001
        if str(url).startswith("https://discord.example"):
            sent["discord"] += 1
            return DiscordResp()
        if str(url).startswith("https://ntfy.example"):
            sent["ntfy"] += 1
            return NtfyResp()
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(m.requests, "post", fake_post)

    m.main()
    assert sent["discord"] == 1
    assert sent["ntfy"] == 1


def test_api_error_str_variants() -> None:
    """ApiError.__str__ の分岐（status_code あり/なし）を確認する。"""
    assert str(m.ApiError("msg", None)) == "msg"
    assert str(m.ApiError("msg", 500)) == "msg (status=500)"


def test_bool_env_true_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    """bool_env の True/False 判定を確認する。"""
    monkeypatch.setenv("B", "yes")
    assert m.bool_env("B") is True
    monkeypatch.setenv("B", "0")
    assert m.bool_env("B") is False


def test_sign_and_json_dumps_compact() -> None:
    """署名・JSONの固定化の基本動作を確認する。"""
    sig = m.sign_hmac_sha256_hex("secret", "text")
    assert isinstance(sig, str) and len(sig) == 64
    s = m.json_dumps_compact({"a": 1, "b": "x"})
    assert s == '{"a":1,"b":"x"}'


def test_resp_json_cast() -> None:
    """_resp_json が Response.json を呼び出すことを確認する。"""
    class R:
        def json(self):  # noqa: ANN001
            return {"k": 1}
    assert m._resp_json(R()) == {"k": 1}


def test_http_request_json_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """http_request_json の正常系を確認する。"""
    class R:
        status_code = 200
        text = "ok"

        def json(self):  # noqa: ANN001
            return {"x": 1}

    monkeypatch.setattr(m.requests, "request", lambda *a, **k: R())  # noqa: ANN001
    out = m.http_request_json("GET", "http://x", {}, None)
    assert out == {"x": 1}


def test_http_request_json_request_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """http_request_json の通信失敗が ApiError になることを確認する。"""
    def boom(*a, **k):  # noqa: ANN001
        raise m.RequestException("net error")

    monkeypatch.setattr(m.requests, "request", boom)
    with pytest.raises(m.ApiError):
        m.http_request_json("GET", "http://x", {}, None)


def test_http_request_json_non_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """http_request_json の JSONでないレスポンスが ApiError になることを確認する。"""
    class R:
        status_code = 500
        text = "html"

        def json(self):  # noqa: ANN001
            raise ValueError("not json")

    monkeypatch.setattr(m.requests, "request", lambda *a, **k: R())  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.http_request_json("GET", "http://x", {}, None)


def test_bf_public_getticker_invalid_response_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """getticker が dict 以外を返したときの異常系。"""
    monkeypatch.setattr(m, "http_request_json", lambda *a, **k: ["x"])  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.bf_public_getticker("http://x", "BTC_JPY")


def test_bf_public_getticker_missing_ltp(monkeypatch: pytest.MonkeyPatch) -> None:
    """getticker が ltp 欠落のときの異常系。"""
    monkeypatch.setattr(m, "http_request_json", lambda *a, **k: {"foo": 1})  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.bf_public_getticker("http://x", "BTC_JPY")


def test_bf_public_getticker_invalid_ltp(monkeypatch: pytest.MonkeyPatch) -> None:
    """ltp <= 0 の場合に ApiError になることを確認する。"""
    monkeypatch.setattr(m, "http_request_json", lambda *a, **k: {"ltp": 0})  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.bf_public_getticker("http://x", "BTC_JPY")


def test_bf_private_request_signing_and_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """bf_private_request が署名付きで http_request_json を呼ぶことを確認する。"""
    monkeypatch.setattr(m.time, "time", lambda: 1000.0)
    captured = {}

    def fake_http(method, url, headers, body_str):  # noqa: ANN001
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = dict(headers)
        captured["body_str"] = body_str
        return {"ok": True}

    monkeypatch.setattr(m, "http_request_json", fake_http)

    out = m.bf_private_request(
        "post",
        "https://api.bitflyer.com/",
        "/v1/me/sendchildorder",
        "KEY",
        "SECRET",
        body={"a": 1},
    )
    assert out == {"ok": True}
    assert captured["method"] == "POST"
    assert captured["url"] == "https://api.bitflyer.com/v1/me/sendchildorder"
    assert captured["headers"]["ACCESS-KEY"] == "KEY"
    assert captured["headers"]["ACCESS-TIMESTAMP"] == "1000"
    assert captured["body_str"] == '{"a":1}'
    assert captured["headers"]["ACCESS-SIGN"] != ""


def test_bf_private_request_error_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Private API が error_message を返した場合に ApiError になることを確認する。"""
    monkeypatch.setattr(m, "http_request_json", lambda *a, **k: {"error_message": "ng"})  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.bf_private_request("GET", "http://x", "/y", "k", "s")


def test_bf_get_jpy_available_balance_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    """bf_get_jpy_available_balance の分岐（異常/JPYなし/非Mapping行）を確認する。"""
    monkeypatch.setattr(m, "bf_private_request", lambda *a, **k: 123)  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.bf_get_jpy_available_balance("http://x", "k", "s")

    monkeypatch.setattr(
        m,
        "bf_private_request",
        lambda *a, **k: [123, {"currency_code": "USD", "available": 1.0}],  # noqa: ANN001
    )
    assert m.bf_get_jpy_available_balance("http://x", "k", "s") == Decimal("0")

    monkeypatch.setattr(m, "bf_private_request", lambda *a, **k: [{"currency_code": "JPY"}])  # noqa: ANN001
    assert m.bf_get_jpy_available_balance("http://x", "k", "s") == Decimal("0")


def test_validate_amounts_branches() -> None:
    """validate_amounts の分岐を確認する。"""
    with pytest.raises(m.ConfigError):
        m.validate_amounts(Decimal("-1"), None)
    with pytest.raises(m.ConfigError):
        m.validate_amounts(Decimal("1"), Decimal("0"))
    with pytest.raises(m.ConfigError):
        m.validate_amounts(Decimal("10"), Decimal("5"))


def test_validate_min_size_non_btc() -> None:
    """BTC_JPY 以外は最小数量チェックをスキップする。"""
    m.validate_min_size("ETH_JPY", Decimal("0.00000001"))


def test_bf_send_market_buy_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    """bf_send_market_buy の異常/正常系を確認する。"""
    monkeypatch.setattr(m, "bf_private_request", lambda *a, **k: ["x"])  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.bf_send_market_buy("http://x", "k", "s", "BTC_JPY", Decimal("0.01"))

    monkeypatch.setattr(m, "bf_private_request", lambda *a, **k: {})  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.bf_send_market_buy("http://x", "k", "s", "BTC_JPY", Decimal("0.01"))

    monkeypatch.setattr(m, "bf_private_request", lambda *a, **k: {"child_order_acceptance_id": "ID"})  # noqa: ANN001
    assert m.bf_send_market_buy(
        "http://x", "k", "s", "BTC_JPY", Decimal("0.01")) == "ID"


def test_chunk_text_edge_cases() -> None:
    """chunk_text の境界分岐を確認する。"""
    with pytest.raises(ValueError):
        m.chunk_text("x", 0)
    assert m.chunk_text("", 10) == [""]
    assert m.chunk_text("abcd", 2) == ["ab", "cd"]


def test_post_ntfy_notify_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """post_ntfy_notify が 4xx/5xx を ApiError にすることを確認する。"""
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    def fake_post(*a, **k):  # noqa: ANN001
        return DummyResp(status_code=500, text="ng")

    monkeypatch.setattr(m.requests, "post", fake_post)
    with pytest.raises(m.ApiError):
        m.post_ntfy_notify("https://ntfy.example/topic", "t", "msg")


def test_post_ntfy_notify_request_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """post_ntfy_notify が RequestException を ApiError にすることを確認する。"""
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    def boom(*a, **k):  # noqa: ANN001
        raise m.RequestException("net")

    monkeypatch.setattr(m.requests, "post", boom)
    with pytest.raises(m.ApiError):
        m.post_ntfy_notify("https://ntfy.example/topic", "t", "msg")


def test_parse_time_ranges_more_branches() -> None:
    """parse_time_ranges_jst の追加分岐を確認する。"""
    with pytest.raises(m.ConfigError):
        m.parse_time_ranges_jst("10:00-10:00")
    with pytest.raises(m.ConfigError):
        m.parse_time_ranges_jst("10:00-xx")
    with pytest.raises(m.ConfigError):
        m.parse_time_ranges_jst("1000")


def test_is_now_in_skip_range_jst_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    """is_now_in_skip_range_jst の start<end / 日跨ぎ分岐を確認する。"""
    # 01:30 JST 固定
    class FakeDateTime:
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            import datetime as dt
            return dt.datetime(2026, 1, 1, 1, 30, 0, tzinfo=tz)

    monkeypatch.setattr(m, "datetime", FakeDateTime)

    assert m.is_now_in_skip_range_jst([(60, 120)]) is True  # 01:00-02:00
    assert m.is_now_in_skip_range_jst(
        [(1380, 120)]) is True  # 23:00-02:00 (wrap)
    assert m.is_now_in_skip_range_jst([(200, 300)]) is False  # 03:20-05:00


def test_is_maintenance_like_api_error_variants() -> None:
    """is_maintenance_like_api_error の分岐を確認する。"""
    assert m.is_maintenance_like_api_error(m.ApiError("x", 503)) is True
    assert m.is_maintenance_like_api_error(RuntimeError(
        "temporarily unavailable due to maintenance")) is True
    assert m.is_maintenance_like_api_error(RuntimeError("other")) is False


def test_load_alert_state_variants(tmp_path) -> None:
    """load_alert_state の分岐（存在しない/壊れたJSON/非Mapping/混在）を確認する。"""
    p = tmp_path / "state.json"
    assert m.load_alert_state(p) == {}

    p.write_text("{bad", encoding="utf-8")
    assert m.load_alert_state(p) == {}

    p.write_text("[]", encoding="utf-8")
    assert m.load_alert_state(p) == {}

    p.write_text('{"a": 1, "b": "x"}', encoding="utf-8")
    assert m.load_alert_state(p) == {"a": 1}


def test_try_notify_all_swallows_api_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """_try_notify_all が ApiError を握りつぶす分岐を確認する。"""
    monkeypatch.setattr(
        m,
        "notify_discord_and_ntfy",
        lambda *a, **k: (_ for _ in ()).throw(m.ApiError("fail")),  # noqa: ANN001
    )
    m._try_notify_all(
        "sub",
        "msg",
        discord_webhook_url="x",
        discord_guild_id="g",
        discord_channel_id="c",
        ntfy_topic_url="y",
        ntfy_token=None,
        notify_on_discord=True,
        notify_on_ntfy=True,
    )


def test_handle_result_skip_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    """_handle_result の SKIP 分岐を確認する。"""
    called = {"n": 0}

    def fake_notify(*a, **k):  # noqa: ANN001
        called["n"] += 1

    monkeypatch.setattr(m, "_try_notify_all", fake_notify)

    m._handle_result(
        {"status": "SKIP", "reason": "x", "product_code": "BTC_JPY",
            "buy_amount_jpy": Decimal("1")},
        m.NotifyConfig(
            discord_webhook_url="https://discord.example/webhook",
            discord_guild_id="g",
            discord_channel_id="c",
            discord_bot_token=None,
            ntfy_topic_url="https://ntfy.example/topic",
            ntfy_token=None,
            notify_on_discord=True,
            notify_on_ntfy=True,
            notify_on_skip_time=True,
            notify_on_success=True,
        ),
        end_datetime_jst="2026-01-01 00:00:00 JST",
        duration_sec=1.0,
        executed_size_btc=Decimal("0"),
        executed_amount_jpy=Decimal("0"),
        totals=m.SuccessTotals(Decimal("0"), Decimal("0"), 0),
    )
    assert called["n"] == 0


def test_run_module_as_main_executes_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """if __name__ == '__main__' の main() 呼び出し行を通す。"""
    import runpy

    # ネットワークを避けるため BUY_AMOUNT_JPY=0 で SKIP させる
    monkeypatch.setenv("BITFLYER_API_KEY", "k")
    monkeypatch.setenv("BITFLYER_API_SECRET", "s")
    monkeypatch.setenv("PRODUCT_CODE", "BTC_JPY")
    monkeypatch.setenv("BUY_AMOUNT_JPY", "0")
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)

    runpy.run_module("src.bitflyer_dca", run_name="__main__")


def test_env_missing_and_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """env の Missing/Empty 分岐を確認する。"""
    monkeypatch.delenv("X_NOT_SET", raising=False)
    with pytest.raises(m.ConfigError):
        m.env("X_NOT_SET")
    monkeypatch.setenv("X_EMPTY", "")
    with pytest.raises(m.ConfigError):
        m.env("X_EMPTY")


def test_bf_public_getticker_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """bf_public_getticker の正常系（return ltp）を確認する。"""
    monkeypatch.setattr(m, "http_request_json", lambda *a, **k: {"ltp": 123456})  # noqa: ANN001
    assert m.bf_public_getticker("http://x", "BTC_JPY") == Decimal("123456")


def test_validate_min_size_raises() -> None:
    """validate_min_size の例外分岐を確認する。"""
    with pytest.raises(m.ConfigError):
        m.validate_min_size("BTC_JPY", Decimal("0.0001"))


def test_parse_time_ranges_empty_returns_empty_list() -> None:
    """parse_time_ranges_jst の空文字分岐（return []）を確認する。"""
    assert m.parse_time_ranges_jst("") == []


def test_discord_message_link() -> None:
    """discord_message_link が期待フォーマットになることを確認する。"""
    assert m.discord_message_link(
        "g", "c", "m") == "https://discord.com/channels/g/c/m"


def test_post_ntfy_notify_sets_click_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """post_ntfy_notify が Click ヘッダを付与することを確認する。"""
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    captured = {"click": ""}

    def fake_post(url, headers=None, data=None, timeout=None, json=None):  # noqa: ANN001
        captured["click"] = (headers or {}).get("Click", "")
        return DummyResp(status_code=200)

    monkeypatch.setattr(m.requests, "post", fake_post)
    m.post_ntfy_notify("https://ntfy.example/topic",
                       "t", "msg", click_url="http://x")
    assert captured["click"] == "http://x"


def test_notify_discord_and_ntfy_ntfy_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discord未設定でも ntfy のみ送れることを確認する。"""
    monkeypatch.setattr(m.time, "sleep", lambda _: None)
    called = {"ntfy": 0}

    def fake_post(url, headers=None, data=None, timeout=None, json=None):  # noqa: ANN001
        if str(url).startswith("https://ntfy.example"):
            called["ntfy"] += 1
            return DummyResp(status_code=200)
        raise AssertionError("Discord should not be called")

    monkeypatch.setattr(m.requests, "post", fake_post)
    m.notify_discord_and_ntfy(
        "s",
        "b",
        discord_webhook_url=None,
        discord_guild_id=None,
        discord_channel_id=None,
        ntfy_topic_url="https://ntfy.example/topic",
        ntfy_token=None,
        notify_on_discord=True,
        notify_on_ntfy=True,
    )
    assert called["ntfy"] == 1


def test_notify_discord_and_ntfy_discord_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """ntfy未設定でも Discord のみ送れることを確認する。"""
    monkeypatch.setattr(m.time, "sleep", lambda _: None)
    called = {"discord": 0}

    class DiscordResp:
        status_code = 200
        text = '{"id":"mid"}'

        def json(self):  # noqa: ANN001
            return {"id": "mid"}

    def fake_post(url, headers=None, data=None, timeout=None, json=None):  # noqa: ANN001
        if str(url).startswith("https://discord.example"):
            called["discord"] += 1
            return DiscordResp()
        raise AssertionError("ntfy should not be called")

    monkeypatch.setattr(m.requests, "post", fake_post)
    m.notify_discord_and_ntfy(
        "s",
        "b",
        discord_webhook_url="https://discord.example/webhook",
        discord_guild_id="g",
        discord_channel_id="c",
        ntfy_topic_url=None,
        ntfy_token=None,
        notify_on_discord=True,
        notify_on_ntfy=True,
    )
    assert called["discord"] == 1


def test_validate_amounts_max_none_returns() -> None:
    """MAX_BUY_AMOUNT_JPY 未設定(None)のときは上限制御をせずに return する。"""
    m.validate_amounts(Decimal("1"), None)


def test_validate_amounts_exceeds_max() -> None:
    """validate_amounts の buy>max 分岐を明示的に通す。"""
    with pytest.raises(m.ConfigError):
        m.validate_amounts(Decimal("10"), Decimal("9"))


def test_post_discord_webhook_empty_url_raises() -> None:
    """post_discord_webhook の空URL分岐を確認する。"""
    with pytest.raises(ValueError):
        m.post_discord_webhook("", "x")


def test_post_discord_webhook_request_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """post_discord_webhook の RequestException が ApiError になることを確認する。"""
    def boom(*a, **k):  # noqa: ANN001
        raise m.RequestException("net")

    monkeypatch.setattr(m.requests, "post", boom)
    with pytest.raises(m.ApiError):
        m.post_discord_webhook("https://discord.example/webhook", "x")


def test_post_discord_webhook_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """post_discord_webhook の 4xx/5xx が ApiError になることを確認する。"""
    class R:
        status_code = 500
        text = "ng"

        def json(self):  # noqa: ANN001
            return {"id": "x"}

    monkeypatch.setattr(m.requests, "post", lambda *a, **k: R())  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.post_discord_webhook("https://discord.example/webhook", "x")


def test_post_discord_webhook_non_json_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """post_discord_webhook の JSONでないレスポンスが ApiError になることを確認する。"""
    class R:
        status_code = 200
        text = "not json"

        def json(self):  # noqa: ANN001
            raise ValueError("bad")

    monkeypatch.setattr(m.requests, "post", lambda *a, **k: R())  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.post_discord_webhook("https://discord.example/webhook", "x")


def test_post_discord_webhook_missing_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """post_discord_webhook の id 欠落が ApiError になることを確認する。"""
    class R:
        status_code = 200
        text = "{}"

        def json(self):  # noqa: ANN001
            return {"ok": True}

    monkeypatch.setattr(m.requests, "post", lambda *a, **k: R())  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.post_discord_webhook("https://discord.example/webhook", "x")


def test_post_discord_webhook_id_not_str(monkeypatch: pytest.MonkeyPatch) -> None:
    """post_discord_webhook の id が文字列でない場合に ApiError になることを確認する。"""
    class R:
        status_code = 200
        text = '{"id":1}'

        def json(self):  # noqa: ANN001
            return {"id": 1}

    monkeypatch.setattr(m.requests, "post", lambda *a, **k: R())  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.post_discord_webhook("https://discord.example/webhook", "x")


def test_post_ntfy_notify_empty_url_raises() -> None:
    """post_ntfy_notify の空URL分岐を確認する。"""
    with pytest.raises(ValueError):
        m.post_ntfy_notify("", "t", "m")


def test_post_ntfy_notify_auth_and_split_title(monkeypatch: pytest.MonkeyPatch) -> None:
    """post_ntfy_notify の Authorization と分割Title分岐を確認する。"""
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    captured: list[dict[str, str]] = []

    def fake_post(url, headers=None, data=None, timeout=None):  # noqa: ANN001
        captured.append(dict(headers or {}))
        return DummyResp(status_code=200)

    monkeypatch.setattr(m.requests, "post", fake_post)

    m.post_ntfy_notify(
        "https://ntfy.example/topic",
        "TITLE",
        "a" * 8000,
        click_url="http://x",
        token="tok",
        max_body=3500,
    )
    # 3分割される想定（3500,3500,1000）
    assert len(captured) == 3
    assert captured[0]["Authorization"] == "Bearer tok"
    assert captured[0]["Click"] == "http://x"
    assert captured[0]["Title"].startswith("(1/3) ")
    assert captured[1]["Title"].startswith("(2/3) ")
    assert captured[2]["Title"].startswith("(3/3) ")


def test_is_now_in_skip_range_jst_wrap_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    """is_now_in_skip_range_jst の日跨ぎ分岐（start>end）を通す。"""
    class FakeDateTime:
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            import datetime as dt
            return dt.datetime(2026, 1, 1, 23, 30, 0, tzinfo=tz)

    monkeypatch.setattr(m, "datetime", FakeDateTime)
    assert m.is_now_in_skip_range_jst([(1380, 120)]) is True  # 23:00-02:00


def test_handle_result_skip_time_notifies(monkeypatch: pytest.MonkeyPatch) -> None:
    """_handle_result の SKIP_TIME で通知する分岐を確認する。"""
    called = {"n": 0}
    monkeypatch.setattr(m, "_try_notify_all", lambda *a, **k: called.__setitem__("n", called["n"] + 1))  # noqa: ANN001

    m._handle_result(
        {"status": "SKIP_TIME", "reason": "r",
            "product_code": "BTC_JPY", "buy_amount_jpy": Decimal("1")},
        m.NotifyConfig(
            discord_webhook_url="https://discord.example/webhook",
            discord_guild_id="g",
            discord_channel_id="c",
            discord_bot_token=None,
            ntfy_topic_url="https://ntfy.example/topic",
            ntfy_token=None,
            notify_on_discord=True,
            notify_on_ntfy=True,
            notify_on_skip_time=True,
            notify_on_success=True,
        ),
        end_datetime_jst="2026-01-01 00:00:00 JST",
        duration_sec=1.0,
        executed_size_btc=Decimal("0"),
        executed_amount_jpy=Decimal("0"),
        totals=m.SuccessTotals(Decimal("0"), Decimal("0"), 0),
    )
    assert called["n"] == 1


def test_handle_result_ok_notifies(monkeypatch: pytest.MonkeyPatch) -> None:
    """_handle_result の OK で通知する分岐を確認する。"""
    called = {"n": 0}
    monkeypatch.setattr(m, "_try_notify_all", lambda *a, **k: called.__setitem__("n", called["n"] + 1))  # noqa: ANN001

    m._handle_result(
        {
            "status": "OK",
            "reason": "ORDER_SENT",
            "product_code": "BTC_JPY",
            "buy_amount_jpy": Decimal("1"),
            "ltp": Decimal("100"),
            "size": Decimal("0.01"),
            "acceptance_id": "id",
        },
        m.NotifyConfig(
            discord_webhook_url="https://discord.example/webhook",
            discord_guild_id="g",
            discord_channel_id="c",
            discord_bot_token=None,
            ntfy_topic_url="https://ntfy.example/topic",
            ntfy_token=None,
            notify_on_discord=True,
            notify_on_ntfy=True,
            notify_on_skip_time=True,
            notify_on_success=True,
        ),
        end_datetime_jst="2026-01-01 00:00:00 JST",
        duration_sec=1.0,
        executed_size_btc=Decimal("0.01"),
        executed_amount_jpy=Decimal("1"),
        totals=m.SuccessTotals(Decimal("0.5"), Decimal("100"), 3),
    )
    assert called["n"] == 1


def test_handle_exception_maintenance_no_notify(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """_handle_exception のメンテ判定で通知しない分岐を確認する。"""
    called = {"n": 0}
    monkeypatch.setattr(m, "_try_notify_all", lambda *a, **k: called.__setitem__("n", called["n"] + 1))  # noqa: ANN001

    m._handle_exception(
        m.ApiError("bad", 503),
        discord_webhook_url="https://discord.example/webhook",
        discord_guild_id="g",
        discord_channel_id="c",
        ntfy_topic_url="https://ntfy.example/topic",
        ntfy_token=None,
        notify_on_discord=True,
        notify_on_ntfy=True,
        notify_on_skip_api_maint=False,
        state_dir_raw=str(tmp_path),
        throttle_sec=3600,
        discord_bot_token=None,
        log_stacktrace=True,
    )
    assert called["n"] == 0


def test_validate_amounts_within_max_ok() -> None:
    """validate_amounts の max設定時に例外なく通過する分岐を確認する。"""
    m.validate_amounts(Decimal("10"), Decimal("10"))


def test_is_now_in_skip_range_jst_wrap_not_in_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """日跨ぎレンジで範囲外のとき False になる分岐を確認する。"""
    class FakeDateTime:
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            import datetime as dt
            return dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(m, "datetime", FakeDateTime)
    assert m.is_now_in_skip_range_jst([(1380, 120)]) is False  # 23:00-02:00 の外


def test_handle_result_skip_time_no_notify(monkeypatch: pytest.MonkeyPatch) -> None:
    """_handle_result の SKIP_TIME で通知しない分岐を確認する。"""
    called = {"n": 0}
    monkeypatch.setattr(m, "_try_notify_all", lambda *a, **k: called.__setitem__("n", called["n"] + 1))  # noqa: ANN001

    m._handle_result(
        {"status": "SKIP_TIME", "reason": "r",
            "product_code": "BTC_JPY", "buy_amount_jpy": Decimal("1")},
        m.NotifyConfig(
            discord_webhook_url="https://discord.example/webhook",
            discord_guild_id="g",
            discord_channel_id="c",
            discord_bot_token=None,
            ntfy_topic_url="https://ntfy.example/topic",
            ntfy_token=None,
            notify_on_discord=True,
            notify_on_ntfy=True,
            notify_on_skip_time=False,
            notify_on_success=True,
        ),
        end_datetime_jst="2026-01-01 00:00:00 JST",
        duration_sec=1.0,
        executed_size_btc=Decimal("0"),
        executed_amount_jpy=Decimal("0"),
        totals=m.SuccessTotals(Decimal("0"), Decimal("0"), 0),
    )
    assert called["n"] == 0


def test_handle_result_ok_notify_on_success_false_no_notify(monkeypatch: pytest.MonkeyPatch) -> None:
    """_handle_result の OK は notify_on_success=False の場合に通知しないことを確認する。"""
    called = {"n": 0}
    monkeypatch.setattr(m, "_try_notify_all", lambda *a, **k: called.__setitem__("n", called["n"] + 1))  # noqa: ANN001

    m._handle_result(
        {
            "status": "OK",
            "reason": "ORDER_SENT",
            "product_code": "BTC_JPY",
            "buy_amount_jpy": Decimal("1"),
            "ltp": Decimal("100"),
            "size": Decimal("0.01"),
            "acceptance_id": "id",
        },
        m.NotifyConfig(
            discord_webhook_url="https://discord.example/webhook",
            discord_guild_id="g",
            discord_channel_id="c",
            discord_bot_token=None,
            ntfy_topic_url="https://ntfy.example/topic",
            ntfy_token=None,
            notify_on_discord=True,
            notify_on_ntfy=True,
            notify_on_skip_time=True,
            notify_on_success=False,
        ),
        end_datetime_jst="2026-01-01 00:00:00 JST",
        duration_sec=1.0,
        executed_size_btc=Decimal("0.01"),
        executed_amount_jpy=Decimal("1"),
        totals=m.SuccessTotals(Decimal("0.5"), Decimal("100"), 3),
    )
    assert called["n"] == 0


def test_post_discord_webhook_invalid_response_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discord Webhook のレスポンスが dict 以外の場合に ApiError になることを確認する。"""
    class R:
        status_code = 200
        text = "[]"

        def json(self):  # noqa: ANN001
            return ["not", "mapping"]

    monkeypatch.setattr(m.requests, "post", lambda *a, **k: R())  # noqa: ANN001
    with pytest.raises(m.ApiError):
        m.post_discord_webhook("https://discord.example/webhook", "x")


def test_discord_webhook_url_params() -> None:
    """wait/thread_id の付与パターンを網羅する。"""
    assert m._discord_webhook_url("u", wait=False, thread_id=None) == "u"
    assert m._discord_webhook_url(
        "u", wait=True, thread_id=None) == "u?wait=true"
    assert m._discord_webhook_url(
        "u", wait=False, thread_id="t") == "u?thread_id=t"
    assert m._discord_webhook_url(
        "u?x=1", wait=True, thread_id="t") == "u?x=1&wait=true&thread_id=t"


def test_discord_extract_message_id_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discordレスポンスの message_id 抽出失敗パターンを網羅する。"""

    class R:
        def __init__(self, status_code: int, text: str, json_obj):  # noqa: ANN001
            self.status_code = status_code
            self.text = text
            self._json_obj = json_obj

        def json(self):  # noqa: ANN001
            if self._json_obj == "__VALUE_ERROR__":
                raise ValueError("bad json")
            return self._json_obj

    # non-JSON
    with pytest.raises(m.ApiError):
        m._discord_extract_message_id(R(200, "x", "__VALUE_ERROR__"))

    # not mapping
    with pytest.raises(m.ApiError):
        m._discord_extract_message_id(R(200, "x", ["a"]))

    # missing id
    with pytest.raises(m.ApiError):
        m._discord_extract_message_id(R(200, "x", {"no": "id"}))

    # empty id
    with pytest.raises(m.ApiError):
        m._discord_extract_message_id(R(200, "x", {"id": ""}))

    # ok
    assert m._discord_extract_message_id(R(200, "x", {"id": "mid"})) == "mid"


def test_discord_post_json_request_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discord送信が通信例外のとき ApiError になる。"""

    def boom(*args, **kwargs):  # noqa: ANN001
        raise m.RequestException("net")

    monkeypatch.setattr(m.requests, "post", boom)

    with pytest.raises(m.ApiError):
        m._discord_post_json("u", {"content": "x"})


def test_post_discord_webhook_split_and_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discord投稿の分割と sleep を検証する。"""
    slept: list[float] = []
    monkeypatch.setattr(m.time, "sleep", lambda s: slept.append(float(s)))

    posted: list[str] = []

    class Resp:
        status_code = 200
        text = '{"id":"mid"}'

        def __init__(self, content: str) -> None:
            self._content = content

        def json(self):  # noqa: ANN001
            return {"id": "mid"}

    def fake_post(url, json=None, timeout=None, **kwargs):  # noqa: ANN001
        posted.append(json["content"])
        return Resp(json["content"])

    monkeypatch.setattr(m.requests, "post", fake_post)

    msg = "a" * 4000
    mid = m.post_discord_webhook(
        "https://discord.example/webhook", msg, max_body=1900, wait=True)
    assert mid == "mid"
    assert len(posted) == 3
    # 2回目投稿後に sleep(0.2) が呼ばれる（最後の後は呼ばれない）
    assert slept == [0.2]


def test_post_discord_webhook_empty_url() -> None:
    """webhook_url が空なら ValueError。"""
    with pytest.raises(ValueError):
        m.post_discord_webhook("", "x")


def test_discord_raise_for_status() -> None:
    """Discord HTTP エラーが ApiError になる。"""

    class R:
        def __init__(self, code: int) -> None:
            self.status_code = code
            self.text = "err"

    m._discord_raise_for_status(R(200))
    with pytest.raises(m.ApiError):
        m._discord_raise_for_status(R(400))


def test_create_discord_thread_success_and_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """スレッド作成の成功/失敗分岐を網羅する。"""

    class Resp:
        def __init__(self, code: int, text: str, obj):  # noqa: ANN001
            self.status_code = code
            self.text = text
            self._obj = obj

        def json(self):  # noqa: ANN001
            if self._obj == "__VALUE_ERROR__":
                raise ValueError("bad")
            return self._obj

    # request exception
    def boom(*args, **kwargs):  # noqa: ANN001
        raise m.RequestException("net")

    monkeypatch.setattr(m.requests, "post", boom)
    with pytest.raises(m.ApiError):
        m.create_discord_thread(
            bot_token="t", channel_id="c", message_id="m1", name="n")

    # status error
    monkeypatch.setattr(m.requests, "post", lambda *a,
                        **k: Resp(400, "bad", {"x": 1}))
    with pytest.raises(m.ApiError):
        m.create_discord_thread(
            bot_token="t", channel_id="c", message_id="m1", name="n")

    # non-json
    monkeypatch.setattr(m.requests, "post", lambda *a, **
                        k: Resp(200, "x", "__VALUE_ERROR__"))
    with pytest.raises(m.ApiError):
        m.create_discord_thread(
            bot_token="t", channel_id="c", message_id="m1", name="n")

    # not mapping
    monkeypatch.setattr(m.requests, "post", lambda *a,
                        **k: Resp(200, "x", ["a"]))
    with pytest.raises(m.ApiError):
        m.create_discord_thread(
            bot_token="t", channel_id="c", message_id="m1", name="n")

    # missing id
    monkeypatch.setattr(m.requests, "post", lambda *a, **
                        k: Resp(200, "x", {"no": "id"}))
    with pytest.raises(m.ApiError):
        m.create_discord_thread(
            bot_token="t", channel_id="c", message_id="m1", name="n")

    # ok
    monkeypatch.setattr(m.requests, "post", lambda *a, **
                        k: Resp(200, "x", {"id": "tid"}))
    assert m.create_discord_thread(
        bot_token="t", channel_id="c", message_id="m1", name="n") == "tid"


def test_post_ntfy_notify_headers_split_and_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """ntfy のヘッダ分岐/分割/例外/HTTPエラーを網羅する。"""
    slept: list[float] = []
    monkeypatch.setattr(m.time, "sleep", lambda s: slept.append(float(s)))

    calls: list[dict[str, str]] = []

    class Resp:
        def __init__(self, code: int) -> None:
            self.status_code = code
            self.text = "err"

    def fake_post(url, headers=None, data=None, timeout=None, **kwargs):  # noqa: ANN001
        calls.append(dict(headers or {}))
        return Resp(200)

    monkeypatch.setattr(m.requests, "post", fake_post)

    m.post_ntfy_notify(
        "https://ntfy.example/topic",
        "TITLE",
        "a" * 8000,
        click_url="https://discord.com/channels/g/c/m",
        token="tok",
        max_body=3500,
    )
    assert len(calls) == 3
    assert calls[0]["Title"].startswith("(1/3)")
    assert "Click" in calls[0]
    assert calls[0]["Authorization"].startswith("Bearer ")
    assert slept == [0.2, 0.2]

    # empty topic_url
    with pytest.raises(ValueError):
        m.post_ntfy_notify("", "t", "m")

    # request exception
    def boom(*args, **kwargs):  # noqa: ANN001
        raise m.RequestException("net")

    monkeypatch.setattr(m.requests, "post", boom)
    with pytest.raises(m.ApiError):
        m.post_ntfy_notify("https://ntfy.example/topic", "t", "m")

    # http error
    monkeypatch.setattr(m.requests, "post", lambda *a, **k: Resp(400))
    with pytest.raises(m.ApiError):
        m.post_ntfy_notify("https://ntfy.example/topic", "t", "m")


def test_format_ntfy_error_body_and_log_error(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """ntfy 本文フォーマットとログ出力分岐を網羅する。"""
    try:
        raise RuntimeError("boom")
    except RuntimeError as e:
        body = m.format_ntfy_error_body(e)
        assert "Error ：RuntimeError: boom" in body
        assert "日時 ：" in body
        assert "Discord" in body

        # with trace
        m.log_error(e, with_trace=True)
        out = capsys.readouterr().out
        assert "Traceback" in out

        # without trace
        m.log_error(e, with_trace=False)
        out2 = capsys.readouterr().out
        assert "ERROR: RuntimeError: boom" in out2


def test_notify_error_thread_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """notify_error_discord_thread_and_ntfy のスレッド作成パスを通す。"""
    calls: list[str] = []

    # parent post returns id
    def fake_post_discord(url: str, message: str, *, thread_id=None, max_body=1900, wait=True):  # noqa: ANN001
        calls.append(f"post:{'thread' if thread_id else 'parent'}:{wait}")
        return "pid" if wait else None

    def fake_create_thread(*, bot_token: str, channel_id: str, message_id: str, name: str, auto_archive_duration: int = 1440) -> str:
        calls.append("create_thread")
        return "tid"

    monkeypatch.setattr(m, "post_discord_webhook", fake_post_discord)
    monkeypatch.setattr(m, "create_discord_thread", fake_create_thread)
    monkeypatch.setattr(m, "post_ntfy_notify", lambda *a, **k: calls.append("ntfy"))  # noqa: ANN001

    try:
        raise RuntimeError("boom")
    except RuntimeError as e:
        m.notify_error_discord_thread_and_ntfy(
            e,
            discord_webhook_url="https://discord.example/webhook",
            discord_guild_id="g",
            discord_channel_id="c",
            discord_bot_token="btok",
            ntfy_topic_url="https://ntfy.example/topic",
            ntfy_token=None,
            notify_on_discord=True,
            notify_on_ntfy=True,
        )

    assert "create_thread" in calls
    # parent(wait=True) then thread(wait=False) then ntfy
    assert calls.count("ntfy") == 1


def test_notify_error_ntfy_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discord無効でntfyのみ送る分岐、ntfy無効で何もしない分岐を通す。"""
    called = {"ntfy": 0}
    monkeypatch.setattr(m, "post_ntfy_notify", lambda *a, **k: called.__setitem__("ntfy", called["ntfy"] + 1))  # noqa: ANN001
    # discord part skipped
    m.notify_error_discord_thread_and_ntfy(
        RuntimeError("x"),
        discord_webhook_url=None,
        discord_guild_id=None,
        discord_channel_id=None,
        discord_bot_token=None,
        ntfy_topic_url="https://ntfy.example/topic",
        ntfy_token=None,
        notify_on_discord=False,
        notify_on_ntfy=True,
    )
    assert called["ntfy"] == 1

    # ntfy part skipped
    m.notify_error_discord_thread_and_ntfy(
        RuntimeError("x"),
        discord_webhook_url=None,
        discord_guild_id=None,
        discord_channel_id=None,
        discord_bot_token=None,
        ntfy_topic_url=None,
        ntfy_token=None,
        notify_on_discord=False,
        notify_on_ntfy=False,
    )


def test_post_ntfy_notify_non_ascii_title_goes_to_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """Title に日本語が含まれる場合でも UnicodeEncodeError にならず送信できる。"""
    captured: list[dict[str, object]] = []

    class Resp:
        def __init__(self, code: int = 200) -> None:
            self.status_code = code
            self.text = "ok"

    def fake_post(url, headers=None, data=None, timeout=None, **kwargs):  # noqa: ANN001
        captured.append({"headers": dict(headers or {}), "data": data})
        return Resp(200)

    monkeypatch.setattr(m.requests, "post", fake_post)
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    title = "｢bitfler-dca｣ Error通知"
    m.post_ntfy_notify("https://ntfy.example/topic", title, "BODY",
                       click_url="https://discord/x", token=None, max_body=3500)

    assert len(captured) == 1
    assert "Title" not in captured[0]["headers"]
    assert isinstance(captured[0]["data"], (bytes, bytearray))
    assert captured[0]["data"].decode("utf-8").startswith(title + "\n")


def test_post_ntfy_notify_unicode_title_does_not_break(monkeypatch: pytest.MonkeyPatch) -> None:
    """日本語タイトルでもヘッダに乗せず本文に埋め込むため送信できることを確認する。"""
    got_headers: list[dict[str, str]] = []
    got_bodies: list[bytes] = []

    class Resp:
        def __init__(self, code: int = 200) -> None:
            self.status_code = code
            self.text = "ok"

    def fake_post(url, headers=None, data=None, timeout=None, **kwargs):  # noqa: ANN001
        got_headers.append(dict(headers or {}))
        got_bodies.append(data)
        return Resp(200)

    monkeypatch.setattr(m.requests, "post", fake_post)
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    m.post_ntfy_notify(
        "https://ntfy.example/topic",
        "｢bitfler-dca｣ Error通知",
        "hello",
        click_url="https://discord.com/channels/g/c/m",
        token=None,
        max_body=3500,
    )

    assert got_headers
    assert "Title" not in got_headers[0]
    assert got_bodies and got_bodies[0].decode(
        "utf-8").startswith("｢bitfler-dca｣ Error通知")


def test_post_ntfy_notify_filters_non_latin1_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    """ヘッダ値に latin-1 非対応文字が混入しても送信できる（自動除外）ことを確認する。"""
    got_headers: list[dict[str, str]] = []

    class Resp:
        def __init__(self, code: int = 200) -> None:
            self.status_code = code
            self.text = "ok"

    def fake_post(url, headers=None, data=None, timeout=None, **kwargs):  # noqa: ANN001
        got_headers.append(dict(headers or {}))
        return Resp(200)

    monkeypatch.setattr(m.requests, "post", fake_post)
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    # Click に日本語を混ぜて header filter を通す
    m.post_ntfy_notify(
        "https://ntfy.example/topic",
        "ASCII TITLE",
        "hello",
        click_url="https://example.com/日本語",
        token=None,
        max_body=3500,
    )

    assert got_headers, "requests.post must be called"
    # Click ヘッダが除外される（latin-1 でエンコードできないため）
    assert "Click" not in got_headers[0]


def test_post_ntfy_notify_unicode_title_split_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    """日本語タイトル＋長文で分割される場合、本文側に (i/n) prefix が付与される。"""
    bodies: list[str] = []

    class Resp:
        def __init__(self) -> None:
            self.status_code = 200
            self.text = "ok"

    def fake_post(url, headers=None, data=None, timeout=None, **kwargs):  # noqa: ANN001
        # data は bytes
        bodies.append(data.decode("utf-8"))
        return Resp()

    monkeypatch.setattr(m.requests, "post", fake_post)
    monkeypatch.setattr(m.time, "sleep", lambda _: None)

    m.post_ntfy_notify(
        "https://ntfy.example/topic",
        "｢bitfler-dca｣ Error通知",
        "x" * 8000,
        click_url=None,
        token=None,
        max_body=3500,
    )

    # 分割されるので prefix が付与される（use_title_header=False）
    assert len(bodies) >= 2
    assert bodies[1].startswith("(2/")


def test_load_success_totals_branches(tmp_path) -> None:
    """load_success_totals の分岐（存在なし/壊れ/正常）を確認する。"""
    state_file = tmp_path / "success_state.json"

    # 存在しない
    totals = m.load_success_totals(state_file)
    assert totals.total_count == 0

    # 壊れたJSON
    state_file.write_text("{bad json", encoding="utf-8")
    totals = m.load_success_totals(state_file)
    assert totals.total_amount_jpy == Decimal("0")

    # dict 以外
    state_file.write_text('["x"]', encoding="utf-8")
    totals = m.load_success_totals(state_file)
    assert totals.total_size_btc == Decimal("0")

    # 値の型がおかしい
    state_file.write_text(
        '{"total_size_btc":"x","total_amount_jpy":"y","total_count":"z"}', encoding="utf-8")
    totals = m.load_success_totals(state_file)
    assert totals.total_count == 0

    # 正常
    state_file.write_text(
        '{"total_size_btc":"0.1","total_amount_jpy":"200","total_count":3}', encoding="utf-8")
    totals = m.load_success_totals(state_file)
    assert totals.total_size_btc == Decimal("0.1")
    assert totals.total_amount_jpy == Decimal("200")
    assert totals.total_count == 3


def test_compute_and_update_success_totals(tmp_path) -> None:
    """compute_and_update_success_totals が累積を更新することを確認する。"""
    state_dir_raw = str(tmp_path)

    # OKかつDRY_RUNは加算されない
    executed_size, executed_amount, totals = m.compute_and_update_success_totals(
        {
            "status": "OK",
            "reason": "DRY_RUN",
            "product_code": "BTC_JPY",
            "buy_amount_jpy": Decimal("10"),
            "ltp": Decimal("100"),
            "size": Decimal("0.01"),
            "acceptance_id": None,
        },
        state_dir_raw=state_dir_raw,
    )
    assert executed_size == Decimal("0")
    assert totals.total_count == 0

    # OKかつacceptance_idありは加算される
    executed_size, executed_amount, totals = m.compute_and_update_success_totals(
        {
            "status": "OK",
            "reason": "ORDER_SENT",
            "product_code": "BTC_JPY",
            "buy_amount_jpy": Decimal("10"),
            "ltp": Decimal("100"),
            "size": Decimal("0.01"),
            "acceptance_id": "id",
        },
        state_dir_raw=state_dir_raw,
    )
    assert executed_size == Decimal("0.01")
    assert executed_amount == Decimal("10")
    assert totals.total_count == 1
    assert totals.total_amount_jpy == Decimal("10")

    # 2回目も加算される
    _, _, totals2 = m.compute_and_update_success_totals(
        {
            "status": "OK",
            "reason": "ORDER_SENT",
            "product_code": "BTC_JPY",
            "buy_amount_jpy": Decimal("5"),
            "ltp": Decimal("100"),
            "size": Decimal("0.005"),
            "acceptance_id": "id2",
        },
        state_dir_raw=state_dir_raw,
    )
    assert totals2.total_count == 2
    assert totals2.total_amount_jpy == Decimal("15")
