"""
bot/server.py  v2.1
-------------------
Flask webhook server for LINE Bot

指令
  /today  → 今日 PC ratio 、散戶小台／微台未平倉
"""

import os
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from crawler.pc_ratio import latest as pc_latest
from crawler.fut_contracts import latest as fut_latest

SECRET = os.getenv("LINE_CHANNEL_SECRET")
TOKEN  = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
line_api = LineBotApi(TOKEN)
handler  = WebhookHandler(SECRET)
app      = Flask(__name__)

@app.route("/callback", methods=["POST"])
def callback():
    body = request.get_data(as_text=True)
    signature = request.headers.get("X-Line-Signature", "")
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400, "Bad signature")
    return "OK"

def safe_latest(prod):
    doc = fut_latest(prod, 1)
    return f"{doc[0]['retail_net']:+,}" if doc else "N/A"

def build_report():
    pc   = pc_latest(1)[0]
    date = pc["date"].astimezone().strftime("%Y/%m/%d (%a)")
    return (
        f"日期：{date}\n"
        f"🧮 PC ratio 未平倉比：{pc['pc_oi_ratio']:.2f}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{safe_latest('mtx')}\n"
        f"微台：{safe_latest('imtx')}"
    )

@handler.add(MessageEvent, message=TextMessage)
def on_message(event):
    if event.message.text.strip().lower() == "/today":
        line_api.reply_message(event.reply_token, TextSendMessage(build_report()))
    else:
        line_api.reply_message(event.reply_token, TextSendMessage("指令：/today"))

if __name__ == "__main__":
    app.run(port=8000, debug=True)
