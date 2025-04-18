"""
bot/server.py   v2.0
--------------------
Flask webhook server for LINE Bot

◆ 指令
   /today  → 今日 PC ratio、散戶小台／微台未平倉

依賴：Flask, line-bot-sdk, pymongo
"""

import os
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from crawler.pc_ratio import latest as pc_latest
from crawler.fut_contracts import latest as fut_latest

# ── LINE SDK 初始化 ───────────────────────────────────────────
CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")
CHANNEL_TOKEN  = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")

line_api = LineBotApi(CHANNEL_TOKEN)
handler  = WebhookHandler(CHANNEL_SECRET)

# ── Flask App ───────────────────────────────────────────────
app = Flask(__name__)

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400, "Invalid signature")
    return "OK"

# ── 報表組裝 ─────────────────────────────────────────────────
def build_report() -> str:
    pc   = pc_latest(1)[0]
    mtx  = fut_latest("mtx", 1)[0]
    imtx = fut_latest("imtx", 1)[0]

    date_str = pc["date"].astimezone().strftime("%Y/%m/%d (%a)")

    return (
        f"日期：{date_str}\n"
        f"🧮 PC ratio 未平倉比：{pc['pc_oi_ratio']:.2f}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{mtx['retail_net']:+,}\n"
        f"微台：{imtx['retail_net']:+,}"
    )

# ── LINE Event Handler ──────────────────────────────────────
@handler.add(MessageEvent, message=TextMessage)
def on_message(event):
    text = event.message.text.strip().lower()
    if text == "/today":
        line_api.reply_message(event.reply_token, TextSendMessage(build_report()))
    else:
        line_api.reply_message(event.reply_token, TextSendMessage("指令：/today"))

# ── Local debug 用 ───────────────────────────────────────────
if __name__ == "__main__":
    app.run(port=8000, debug=True)
