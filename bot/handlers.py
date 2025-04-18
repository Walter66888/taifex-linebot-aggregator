"""
極簡範例：收到「/today」時送出最新報表
Render WSGI 佈署：gunicorn bot.handlers:app
"""
import os
from datetime import datetime, timedelta, timezone
from linebot import LineBotApi, WebhookHandler
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from crawler.pc_ratio import latest as pc_latest
from crawler.fut_contracts import latest as mtx_latest

line_api = LineBotApi(os.getenv("LINE_CHANNEL_ACCESS_TOKEN"))
handler  = WebhookHandler(os.getenv("LINE_CHANNEL_SECRET"))

# --- Reporter --------------------------------------------------

def _build_report():
    pc  = pc_latest(1)[0]
    mtx = mtx_latest(1)[0]
    d   = pc["date"].astimezone(timezone(timedelta(hours=8))).strftime("%Y/%m/%d (%a)")
    return (
        f"日期：{d}\n"
        f"🧮 PC ratio 未平倉比：{pc['pc_oi_ratio']:.2f}\n"
        f"散戶小台未平倉：{mtx['retail_net']:+,} 口"
    )

# --- Line Webhook ---------------------------------------------

@handler.add(MessageEvent, message=TextMessage)
def on_message(event):
    if event.message.text.strip().lower() == "/today":
        line_api.reply_message(event.reply_token, TextSendMessage(_build_report()))
    else:
        line_api.reply_message(event.reply_token, TextSendMessage("指令：/today"))

app = handler.app  # for gunicorn
