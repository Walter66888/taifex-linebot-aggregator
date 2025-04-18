"""
bot/server.py
-------------
Flask + line-bot-sdk webhook server
Start with: gunicorn bot.server:app
"""

import os
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from crawler.pc_ratio import latest as pc_latest
from crawler.fut_contracts import latest as mtx_latest

CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")
CHANNEL_TOKEN  = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")

line_api = LineBotApi(CHANNEL_TOKEN)
handler  = WebhookHandler(CHANNEL_SECRET)

app = Flask(__name__)

# ── Webhook Entry ─────────────────────────────────────────
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400, "Invalid signature")
    return "OK"

# ── Bot Logic ─────────────────────────────────────────────
def build_report():
    pc  = pc_latest(1)[0]
    mtx = mtx_latest(1)[0]
    d   = pc["date"].astimezone().strftime("%Y/%m/%d (%a)")
    return (f"日期：{d}\n"
            f"🧮 PC ratio 未平倉比：{pc['pc_oi_ratio']:.2f}\n"
            f"散戶小台未平倉：{mtx['retail_net']:+,} 口")

@handler.add(MessageEvent, message=TextMessage)
def on_msg(event):
    if event.message.text.strip().lower() == "/today":
        line_api.reply_message(event.reply_token, TextSendMessage(build_report()))
    else:
        line_api.reply_message(event.reply_token, TextSendMessage("指令：/today"))

# ── Local debug ───────────────────────────────────────────
if __name__ == "__main__":
    app.run(port=8000, debug=True)
