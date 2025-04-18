"""
bot/server.py  v2.3
-------------------
LINE Bot 服務

指令
────
/today         今日 PC‑ratio + 散戶小台/微台未平倉
/reset_fut     **管理員**：刪除 fut_contracts → 立即重抓
/show_indexes  **管理員**：列出 fut_contracts 現有索引

環境變數
────────
LINE_CHANNEL_SECRET
LINE_CHANNEL_ACCESS_TOKEN
ADMIN_USER_IDS          # 逗號分隔的 user_id 白名單
"""

import os, json, logging, traceback
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from crawler.pc_ratio       import latest as pc_latest
from crawler.fut_contracts  import latest as fut_latest, fetch as fut_fetch
from utils.db               import get_col

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

line_api = LineBotApi(os.getenv("LINE_CHANNEL_ACCESS_TOKEN"))
handler  = WebhookHandler(os.getenv("LINE_CHANNEL_SECRET"))
ADMIN_IDS = set(filter(None, os.getenv("ADMIN_USER_IDS", "").split(",")))

# ── Helper ────────────────────────────────────────────
def is_admin(uid: str) -> bool:
    return uid in ADMIN_IDS

def safe_latest(prod: str):
    doc = fut_latest(prod, 1)
    return f"{doc[0]['retail_net']:+,}" if doc else "N/A"

def build_report() -> str:
    pc   = pc_latest(1)[0]
    date = pc["date"].astimezone().strftime("%Y/%m/%d (%a)")
    return (
        f"日期：{date}\n"
        f"🧮 PC ratio 未平倉比：{pc['pc_oi_ratio']:.2f}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{safe_latest('mtx')}\n"
        f"微台：{safe_latest('imtx')}"
    )

# ── Flask route ───────────────────────────────────────
@app.route("/callback", methods=["POST"])
def callback():
    body      = request.get_data(as_text=True)
    signature = request.headers.get("X-Line-Signature", "")
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400, "Bad signature")
    return "OK"

# ── LINE Event Handler ────────────────────────────────
@handler.add(MessageEvent, message=TextMessage)
def on_message(event: MessageEvent):
    uid  = event.source.user_id
    text = event.message.text.strip().lower()
    logging.info(f"[LINE] {uid} -> {text}")

    # ── 公開指令 ──
    if text == "/today":
        line_api.reply_message(event.reply_token, TextSendMessage(build_report()))
        return

    # ── 非管理員 ──
    if not is_admin(uid):
        line_api.reply_message(event.reply_token, TextSendMessage("可用指令：/today"))
        return

    # ── 管理員指令 ──
    if text == "/reset_fut":
        try:
            get_col("fut_contracts").drop()
            fut_fetch()
            msg = "fut_contracts 已重建 ✔"
        except Exception as e:
            logging.error("reset_fut failed\n" + traceback.format_exc())
            msg = f"重建失敗：{type(e).__name__}"
        line_api.reply_message(event.reply_token, TextSendMessage(msg))
        return

    if text == "/show_indexes":
        idx = json.dumps(get_col("fut_contracts").index_information(),
                         ensure_ascii=False, indent=2)
        line_api.reply_message(event.reply_token, TextSendMessage(idx))
        return

    line_api.reply_message(
        event.reply_token,
        TextSendMessage("管理指令：/reset_fut /show_indexes /today")
    )

# ── Local run ─────────────────────────────────────────
if __name__ == "__main__":
    app.run(port=8000, debug=True)
