# -*- coding: utf-8 -*-
"""
LINE Bot server
"""
import logging
import os
from datetime import datetime

from flask import Flask, request, abort, jsonify
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

# ─────────────── 內部模組 ────────────────
from crawler.fut_contracts import latest as fut_latest, fetch as fut_fetch
from crawler.pc_ratio      import latest as pc_latest, fetch as pc_fetch
from utils.db import get_col

# ─────────────── Flask / LINE init ───────
app      = Flask(__name__)
line_api = LineBotApi(os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
handler  = WebhookHandler(os.environ["LINE_CHANNEL_SECRET"])

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

ADMIN_IDS = set(filter(None, os.getenv("ADMIN_USER_IDS", "").split(",")))  # "uid1,uid2"

# ─────────────── 小工具 ───────────────────
def safe_latest(prod: str) -> str:
    """回傳散戶未平倉字串，無資料顯示 0"""
    doc = fut_latest(prod)             # 只傳一個位置參數
    val = doc.get("retail_net", 0) if doc else 0
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:,}"

def build_report() -> str:
    """組合 /today 報告字串 (PC ratio + 散戶口數)"""
    pc_doc = pc_latest()
    if isinstance(pc_doc, list):                # 若 latest() 回傳 list 先取第 0 筆
        pc_doc = pc_doc[0] if pc_doc else {}

    pc_ratio = pc_doc.get("pc_oi_ratio", "–")
    date_obj = pc_doc.get("date")
    today    = date_obj.strftime("%Y/%m/%d (%a)") if isinstance(date_obj, datetime) else "—"

    mtx_ret  = safe_latest("mtx")
    imtx_ret = safe_latest("imtx")

    return (
        f"日期：{today}\n"
        f"🧮 PC ratio 未平倉比：{pc_ratio}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{mtx_ret}\n"
        f"微台：{imtx_ret}"
    )

# ─────────────── Webhook ─────────────────
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

    # ───── 指令處理 ─────
    if text == "/today":
        line_api.reply_message(event.reply_token, TextSendMessage(build_report()))

    elif text == "/reset_fut" and uid in ADMIN_IDS:
        fut_fetch()
        line_api.reply_message(event.reply_token, TextSendMessage("fut_contracts 已重新抓取"))

    elif text == "/reset_pc" and uid in ADMIN_IDS:
        pc_fetch()
        line_api.reply_message(event.reply_token, TextSendMessage("pc_ratio 已重新抓取"))

    elif text.startswith("/debug") and uid in ADMIN_IDS:
        # /debug 或 /debug?col=pc_ratio
        col = "fut_contracts"
        if "=" in text:
            _, val = text.split("=", 1)
            col = val.strip()
        docs = list(get_col(col).find({}, {"_id": 0}).sort("date", -1).limit(5))
        line_api.reply_message(event.reply_token, TextSendMessage(str(docs)))

    else:
        line_api.reply_message(event.reply_token, TextSendMessage("指令無效或權限不足"))

# ─────────────── gunicorn entry ───────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
