# bot/server.py  v2.3.5  │  2025‑04‑19
# -----------------------------------------------------------
import os, logging, datetime as dt
from flask import Flask, request, abort

# ── v3‧Messaging (送訊息) ───────────────────────────────────
from linebot.v3.messaging import (
    MessagingApi, Configuration, ApiClient,
    TextMessage, ReplyMessageRequest
)

# ── v2‧Webhook (收事件) ─────────────────────────────────────
from linebot.webhook  import WebhookHandler
from linebot.models   import MessageEvent, TextMessage as V2Text
from linebot.exceptions import InvalidSignatureError

# ── 專案內部 ────────────────────────────────────────────────
from crawler.fut_contracts import latest as fut_latest, fetch as fut_fetch
from crawler.pc_ratio      import latest as pc_latest, fetch as pc_fetch

# ───────────────────────────────────────────────────────────
LINE_CHANNEL_SECRET   = os.getenv("LINE_CHANNEL_SECRET")
LINE_CHANNEL_TOKEN    = os.getenv("LINE_CHANNEL_TOKEN")
ADMIN_USER_IDS        = set(x.strip() for x in os.getenv("ADMIN_USER_IDS","").split(",") if x.strip())

app      = Flask(__name__)
handler  = WebhookHandler(LINE_CHANNEL_SECRET)

cfg  = Configuration(access_token=LINE_CHANNEL_TOKEN)
line_api = MessagingApi(ApiClient(cfg))

def reply(reply_token: str, text: str):
    """v3 統一回覆介面"""
    req = ReplyMessageRequest(
        reply_token = reply_token,
        messages    = [TextMessage(text=text)]
    )
    line_api.reply_message(req)

# ── 資料組裝 ────────────────────────────────────────────────
def safe_latest(prod: str) -> str:
    doc = fut_latest(prod)            # 只取單日
    if not doc:
        return "–"
    val = doc["retail_net"]
    return f"{val:+,}"

def build_report() -> str:
    today = dt.date.today().strftime("%Y/%m/%d (%a)")
    pc_data = pc_latest()

    # pc_latest 可能回傳 list，也可能是空；統一取第一筆
    if isinstance(pc_data, list):
        pc_data = pc_data[0] if pc_data else {}
    pc_ratio = pc_data.get("pc_oi_ratio", "–")

    return (
        f"日期：{today}\n"
        f"🧮 PC ratio 未平倉比：{pc_ratio}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{safe_latest('mtx')}\n"
        f"微台：{safe_latest('imtx')}"
    )

# ── Webhook 入口 ───────────────────────────────────────────
@app.post("/callback")
def callback():
    sig  = request.headers.get("X-Line-Signature", "")
    body = request.data.decode("utf-8")

    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)
    return "OK"

# ── 處理文字訊息 ────────────────────────────────────────────
@handler.add(MessageEvent, message=V2Text)
def on_message(event: MessageEvent):
    text = event.message.text.strip()

    if text == "/today":
        reply(event.reply_token, build_report())
        return

    # 只有管理員可手動抓資料
    if text == "/reset_fut" and event.source.user_id in ADMIN_USER_IDS:
        fut_fetch(force=True)
        reply(event.reply_token, "期貨資料已重抓 ✅")
        return

    if text == "/reset_pc" and event.source.user_id in ADMIN_USER_IDS:
        pc_fetch(force=True)
        reply(event.reply_token, "PC ratio 已重抓 ✅")
        return

    # 其他文字：回聲
    reply(event.reply_token, f"你說的是：{text}")

# ───────────────────────────────────────────────────────────
if __name__ == "__main__":      # 本地測試用
    app.run("0.0.0.0", 8000, debug=True)
