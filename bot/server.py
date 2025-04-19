# bot/server.py  v2.3.2   (2025‑04‑19)
# ==========================================================
import os, logging, datetime as dt
from flask import Flask, request, abort

from linebot.v3.webhook     import WebhookHandler
from linebot.v3.messaging   import (
    MessagingApi, Configuration,
    TextMessage, ReplyMessageRequest
)
from linebot.v3.webhooks    import MessageEvent
from linebot.v3.exceptions  import InvalidSignatureError

from crawler.fut_contracts  import latest as fut_latest, fetch as fut_fetch
from crawler.pc_ratio       import latest as pc_latest
from utils.db               import get_col

LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_TOKEN  = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
ADMIN_USER_IDS      = set(filter(None, os.environ.get("ADMIN_USER_IDS", "").split(",")))

app      = Flask(__name__)
handler  = WebhookHandler(LINE_CHANNEL_SECRET)
cfg      = Configuration(access_token=LINE_CHANNEL_TOKEN)
line_api = MessagingApi(cfg)

COL_FUT  = get_col("fut_contracts")


# ---------- 工具 ----------
def reply(token: str, text: str):
    """包一層 ReplyMessageRequest"""
    req = ReplyMessageRequest(
        reply_token=token,
        messages=[TextMessage(text=text)]
    )
    line_api.reply_message(req)

def fmt_num(n: int) -> str:
    return f"{n:+,}"

def safe_latest(prod: str) -> str:
    doc = fut_latest(prod)
    return "–" if not doc else fmt_num(doc["retail_net"])

def build_report() -> str:
    today = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y/%m/%d (%a)")
    pc    = (pc_latest() or {}).get("pc_oi_ratio", "–")
    return (
        f"日期：{today}\n"
        f"🧮 PC ratio 未平倉比：{pc}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{safe_latest('mtx')}\n"
        f"微台：{safe_latest('imtx')}"
    )


# ---------- Webhook ----------
@app.route("/callback", methods=["POST"])
def callback():
    sig  = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)
    return "OK"


# ---------- Event Handler ----------
@handler.add(MessageEvent)
def on_message(event: MessageEvent):
    if event.message.type != "text":
        return

    text = event.message.text.strip().lower()
    uid  = event.source.user_id

    # /today -------------------------------------------------
    if text == "/today":
        reply(event.reply_token, build_report())
        return

    # /update_fut -------------------------------------------
    if text == "/update_fut":
        try:
            fut_fetch()             # 週末自動跳過
            msg = "✅ fut_contracts 已更新"
        except RuntimeError as e:
            msg = str(e)
        except Exception as e:
            logging.exception(e)
            msg = f"更新失敗：{e}"
        reply(event.reply_token, msg)
        return

    # /reset_fut --------------------------------------------
    if text == "/reset_fut":
        if uid not in ADMIN_USER_IDS:
            reply(event.reply_token, "❌ 你沒有權限執行 /reset_fut")
            return
        COL_FUT.drop()
        try:
            fut_fetch(force=True)
            cnt = COL_FUT.count_documents({})
            msg = f"✨ fut_contracts 已重建，現有 {cnt} 筆"
        except Exception as e:
            logging.exception(e)
            msg = f"重抓失敗：{e}"
        reply(event.reply_token, msg)
        return


# ---------- 本地測試 ----------
if __name__ == "__main__":
    app.run("0.0.0.0", 5000, debug=True)
