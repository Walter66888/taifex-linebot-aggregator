# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# bot/server.py  v1.5  2025‑04‑19 09:20  (作者：GPT‑PM)
# 變更：
#   • add  GET /debug?col=fut&token=<UID>  供瀏覽器直接查 DB
# ------------------------------------------------------------
import logging, os
from datetime import datetime
from flask import Flask, request, abort, jsonify
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from crawler.fut_contracts import latest as fut_latest, fetch as fut_fetch
from crawler.pc_ratio      import latest as pc_latest, fetch as pc_fetch
from utils.db import get_col
# ─────────────────────────────────────────────────────────────
app      = Flask(__name__)
line_api = LineBotApi(os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
handler  = WebhookHandler(os.environ["LINE_CHANNEL_SECRET"])

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
LOGGER     = logging.getLogger(__name__)
ADMIN_IDS  = set(filter(None, os.getenv("ADMIN_USER_IDS", "").split(",")))
# ───────────────────────── utils ─────────────────────────────
def safe_latest(prod: str) -> str:
    doc = fut_latest(prod)
    val = doc.get("retail_net", 0) if doc else 0
    return f"{'+' if val>=0 else ''}{val:,}"

def build_report() -> str:
    pc_raw = pc_latest()
    pc_doc = pc_raw[0] if isinstance(pc_raw, list) else pc_raw or {}

    pc_ratio = pc_doc.get("pc_oi_ratio", "–")
    date_obj = pc_doc.get("date")
    today    = date_obj.strftime("%Y/%m/%d (%a)") if isinstance(date_obj, datetime) else "—"

    return (
        f"日期：{today}\n"
        f"🧮 PC ratio 未平倉比：{pc_ratio}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{safe_latest('mtx')}\n"
        f"微台：{safe_latest('imtx')}"
    )
# ───────────────────────── HTTP GET /debug ──────────────────
@app.route("/debug")
def http_debug():
    """GET /debug?col=fut&token=<UID>  僅限管理員"""
    uid  = request.args.get("token", "")
    col  = request.args.get("col", "fut_contracts")
    if uid not in ADMIN_IDS:
        return jsonify({"error": "unauthorized"}), 401

    docs = list(get_col(col).find({}, {"_id": 0}).sort("date", -1).limit(10))
    return jsonify(docs)
# ───────────────────────── webhook ───────────────────────────
@app.route("/callback", methods=["POST"])
def callback():
    sig  = request.headers["X-Line-Signature"]
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessage)
def on_message(event: MessageEvent):
    uid, txt = event.source.user_id, event.message.text.strip()

    if txt == "/today":
        line_api.reply_message(event.reply_token, TextSendMessage(build_report()))

    elif txt == "/reset_fut" and uid in ADMIN_IDS:
        fut_fetch()
        line_api.reply_message(event.reply_token, TextSendMessage("fut_contracts 已重新抓取"))

    elif txt == "/reset_pc" and uid in ADMIN_IDS:
        pc_fetch()
        line_api.reply_message(event.reply_token, TextSendMessage("pc_ratio 已重新抓取"))

    elif txt.startswith("/debug") and uid in ADMIN_IDS:
        col = txt.split("=",1)[-1] if "=" in txt else "fut_contracts"
        docs = list(get_col(col).find({},{"_id":0}).sort("date",-1).limit(5))
        line_api.reply_message(event.reply_token, TextSendMessage(str(docs)))

    else:
        line_api.reply_message(event.reply_token, TextSendMessage("指令無效或權限不足"))
# ───────────────────────── run ───────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
