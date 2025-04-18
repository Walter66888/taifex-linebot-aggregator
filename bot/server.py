# -*- coding: utf-8 -*-
"""
LINE Bot server
"""
import logging
import os

from flask import Flask, request, abort, jsonify
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from crawler.fut_contracts import latest as fut_latest, fetch as fut_fetch
from crawler.pc_ratio      import latest as pc_latest
from utils.db import get_col

# ────────────────────────────────────────────────────────────
app      = Flask(__name__)
line_api = LineBotApi(os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
handler  = WebhookHandler(os.environ["LINE_CHANNEL_SECRET"])

LOGGER = logging.getLogger(__name__)
ADMIN_IDS = set(os.getenv("ADMIN_USER_IDS", "").split(","))  # "uid1,uid2"

# ───────────────────────── 小工具 ────────────────────────────
def safe_latest(prod: str) -> str:
    """
    取最新 retail_net，無資料則顯示 0
    """
    doc = fut_latest(prod)        # ←★ 只傳 1 個位置參數
    val = doc.get("retail_net", 0) if doc else 0
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:,}"

def build_report() -> str:
    pc      = pc_latest().get("pc_oi_ratio", "–")
    mtx_ret = safe_latest("mtx")
    imtx_ret= safe_latest("imtx")
    today   = pc_latest().get("date", "").strftime("%Y/%m/%d (%a)")

    return (
        f"日期：{today}\n"
        f"🧮 PC ratio 未平倉比：{pc}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{mtx_ret}\n"
        f"微台：{imtx_ret}"
    )

# ───────────────────────── Webhook ──────────────────────────
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers["X-Line-Signature"]
    body      = request.get_data(as_text=True)
    LOGGER.info("[LINE] %s -> %s", request.remote_addr, body[:120])

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessage)
def on_message(event: MessageEvent):
    uid  = event.source.user_id
    text = event.message.text.strip()

    if text == "/today":
        line_api.reply_message(event.reply_token, TextSendMessage(build_report()))
    elif text == "/reset_fut" and uid in ADMIN_IDS:
        fut_fetch()
        line_api.reply_message(event.reply_token, TextSendMessage("fut_contracts 已重新抓取"))
    elif text.startswith("/debug") and uid in ADMIN_IDS:
        col = text.split("=",1)[-1] if "=" in text else "fut_contracts"
        docs = list(get_col(col).find().sort("date",-1).limit(5))
        line_api.reply_message(event.reply_token, TextSendMessage(str(docs)))
    else:
        line_api.reply_message(event.reply_token, TextSendMessage("指令無效"))

# ───────────────────────── gunicorn entry ──────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
