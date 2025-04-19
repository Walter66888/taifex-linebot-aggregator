import os
import json
import logging
from datetime import datetime, timezone

from flask import Flask, request, abort, jsonify

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

# ──────────────────────────────────────
# 本專案自己的模組
# ──────────────────────────────────────
from crawler.fut_contracts import latest as fut_latest, fetch as fut_fetch
from crawler.pc_ratio      import latest as pc_latest
from utils.db              import get_col

# ──────────────────────────────────────
# 基本設定
# ──────────────────────────────────────
ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")
ADMIN_USER_IDS = set(os.getenv("ADMIN_USER_IDS", "").split(","))  # 多個 ID 用逗號隔開

if not ACCESS_TOKEN or not CHANNEL_SECRET:
    raise RuntimeError("請在環境變數中設定 LINE_CHANNEL_ACCESS_TOKEN / LINE_CHANNEL_SECRET")

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
line_api = LineBotApi(ACCESS_TOKEN)
handler  = WebhookHandler(CHANNEL_SECRET)

# ──────────────────────────────────────
#  小工具
# ──────────────────────────────────────
def reply(token: str, text: str) -> None:
    """包一層，避免每次都要 new TextSendMessage"""
    line_api.reply_message(token, TextSendMessage(text=text))

def safe_latest(prod: str) -> str:
    """把 None / 空 dict 轉成 '–'，並加上千分位"""
    doc = fut_latest(prod)
    if not doc:
        return "–"
    val = doc.get("retail_net")
    return f"{val:+,}" if val is not None else "–"

def build_report() -> str:
    today = datetime.now(timezone.utc).strftime("%Y/%m/%d (%a)")
    pc = (pc_latest() or {}).get("pc_oi_ratio", "–")

    return (
        f"日期：{today}\n"
        f"🧮 PC ratio 未平倉比：{pc}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{safe_latest('mtx')}\n"
        f"微台：{safe_latest('imtx')}"
    )

# ──────────────────────────────────────
#  Flask routes
# ──────────────────────────────────────
@app.route("/callback", methods=["POST"])
def callback():
    sig  = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)

    return "OK"

@app.route("/debug")
def debug():
    """快速看 Mongo 裡目前抓到什麼資料（只顯示最新 5 筆）"""
    col = request.args.get("col")
    token = request.args.get("token", "")
    if token not in ADMIN_USER_IDS:
        abort(403)

    if not col:
        return "need ?col=", 400

    docs = list(get_col(col).find().sort("date", -1).limit(5))
    for d in docs:
        d["_id"] = str(d["_id"])
    return jsonify(docs)

# ──────────────────────────────────────
#  LINE 事件處理
# ──────────────────────────────────────
@handler.add(MessageEvent, message=TextMessage)
def on_message(event: MessageEvent):
    uid  = event.source.user_id
    text = event.message.text.strip()

    if text == "/today":
        reply(event.reply_token, build_report())
        return

    if text == "/reset_fut":
        if uid not in ADMIN_USER_IDS:
            reply(event.reply_token, "權限不足")
            return

        # 假日或平日都強制重新抓
        try:
            fut_fetch(force=True)
            reply(event.reply_token, "期貨資料已重新抓取完成！")
        except Exception as e:
            logging.exception("reset_fut failed")
            reply(event.reply_token, f"抓取失敗：{e}")
        return

    # 其它訊息直接 echo（方便測試）
    reply(event.reply_token, f"你說的是：「{text}」")

# ──────────────────────────────────────
#  Local run（Render 用 gunicorn，不會執行這段）
# ──────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
