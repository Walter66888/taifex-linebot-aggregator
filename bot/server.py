# bot/server.py   v2.3.0  (2025‑04‑19)
# ==========================================================
import os, logging, datetime as dt
from flask import Flask, request, abort

from linebot.v3.webhook     import WebhookHandler
from linebot.v3.messaging   import MessagingApi, Configuration
from linebot.v3.messaging   import ReplyMessageRequest, TextMessage
from linebot.v3.exceptions  import InvalidSignatureError

from crawler.fut_contracts  import latest as fut_latest, fetch as fut_fetch
from crawler.pc_ratio       import latest as pc_latest
from utils.db               import get_col

# ---------- LINE & APP 初始 ----------
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_TOKEN  = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
ADMIN_USER_IDS      = set(os.environ.get("ADMIN_USER_IDS", "").split(","))  # 多個用半形逗號

app      = Flask(__name__)
handler  = WebhookHandler(LINE_CHANNEL_SECRET)
cfg      = Configuration(access_token=LINE_CHANNEL_TOKEN)
line_api = MessagingApi(cfg)

# ---------- Mongo ----------
COL_FUT  = get_col("fut_contracts")

# ---------- 工具 ----------
def fmt_num(n: int) -> str:
    return f"{n:+,}"

def safe_latest(prod: str) -> str:
    doc = fut_latest(prod)       # 不帶參數→回單筆或 None
    if not doc:
        return "–"
    return fmt_num(doc["retail_net"])

def build_report() -> str:
    today  = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y/%m/%d (%a)")
    pc_doc = pc_latest() or {}
    pc     = pc_doc.get("pc_oi_ratio", "–")

    return (
        f"日期：{today}\n"
        f"🧮 PC ratio 未平倉比：{pc}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{safe_latest('mtx')}\n"
        f"微台：{safe_latest('imtx')}"
    )

# ---------- LINE Webhook ----------
@app.route("/callback", methods=["POST"])
def callback():
    sig  = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)
    return "OK"

# ---------- 指令處理 ----------
@handler.add(event_type="message")
def on_message(event):
    if event.message.type != "text":
        return

    text = event.message.text.strip().lower()
    uid  = event.source.user_id

    # /today -------------------------------------------------
    if text == "/today":
        try:
            line_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=build_report())]
                )
            )
        except Exception as e:
            logging.exception(e)

    # /update_fut －－當天重新抓（週末自動略過）---------------
    elif text == "/update_fut":
        try:
            fut_fetch()                       # 內部會判斷週末
            msg = "fut_contracts 已更新 ✅"
        except RuntimeError as e:
            msg = str(e)
        except Exception as e:
            logging.exception(e)
            msg = f"更新失敗：{e}"
        line_api.reply_message(
            ReplyMessageRequest(reply_token=event.reply_token,
                                messages=[TextMessage(text=msg)])
        )

    # /reset_fut －－清空後強制重抓 --------------------------
    elif text == "/reset_fut":
        if uid not in ADMIN_USER_IDS:
            line_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text="❌ 你沒有權限執行 /reset_fut")]
                )
            )
            return
        # 1) 清空
        COL_FUT.drop()
        # 2) 強制抓
        try:
            fut_fetch(force=True)
            cnt = COL_FUT.count_documents({})
            msg = f"✨ fut_contracts 已重建，現有 {cnt} 筆。"
        except Exception as e:
            logging.exception(e)
            msg = f"重抓失敗：{e}"
        line_api.reply_message(
            ReplyMessageRequest(reply_token=event.reply_token,
                                messages=[TextMessage(text=msg)])
        )

# ---------- 本地測試 ----------
if __name__ == "__main__":
    app.run("0.0.0.0", 5000, debug=True)
