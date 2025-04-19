import os
import json
import logging
from datetime import datetime, timezone, timedelta

from flask import Flask, request, abort, jsonify

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

# ──────────────────────────────────────
# 本專案自己的模組
# ──────────────────────────────────────
from crawler.fut_contracts import latest as fut_latest
from crawler.fut_contracts import fetch as fut_fetch
from crawler.fut_contracts import get_raw_html
from crawler.pc_ratio import latest as pc_latest
from utils.db import get_col

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
handler = WebhookHandler(CHANNEL_SECRET)

# ──────────────────────────────────────
#  小工具
# ──────────────────────────────────────
def reply(token: str, text: str) -> None:
    """包一層，避免每次都要 new TextSendMessage"""
    line_api.reply_message(token, TextSendMessage(text=text))

def safe_latest(prod: str) -> str:
    """把 None / 空 dict 轉成 '–'，並加上千分位"""
    doc = fut_latest(prod)  # 使用 fut_contracts.py 中定义的 latest 函数
    logging.info(f"產品 {prod} 的資料: {doc}")
    
    if not doc:
        logging.warning(f"無法找到產品 {prod} 的資料")
        return "–"
        
    val = doc.get("retail_net")
    logging.info(f"產品 {prod} 的 retail_net: {val}")
    return f"{val:+,}" if val is not None else "–"

def build_report() -> str:
    """建立報告，包含 PC ratio 和散戶小台未平倉"""
    # 使用台灣時區
    tw_tz = timezone(timedelta(hours=8))
    
    # 獲取 PC ratio
    pc_data = pc_latest()
    pc_ratio = "–"
    if pc_data and isinstance(pc_data, dict):
        pc_ratio = f"{pc_data.get('pc_oi_ratio', '–'):.2f}"
    
    # 獲取最新期貨資料
    mtx_data = safe_latest('mtx')
    imtx_data = safe_latest('imtx')
    
    # 當前日期格式化
    today = datetime.now(tw_tz).strftime("%Y/%m/%d (%a)")
    
    # 構建報告
    return (
        f"日期：{today}\n"
        f"🧮 PC ratio 未平倉比：{pc_ratio}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{mtx_data}\n"
        f"微台：{imtx_data}"
    )

# ──────────────────────────────────────
#  Flask routes
# ──────────────────────────────────────
@app.route("/callback", methods=["POST"])
def callback():
    sig = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)

    return "OK"

@app.route("/debug", methods=["GET"])
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

@app.route("/debug_html", methods=["GET"])
def debug_html():
    """查看最新的原始 HTML 內容（需要管理员权限）"""
    token = request.args.get("token", "")
    if token not in ADMIN_USER_IDS:
        abort(403)
    
    html = get_raw_html()
    if not html:
        return "No HTML data found", 404
    
    return html

@app.route("/debug_data", methods=["GET"])
def debug_data():
    """查看解析後的期貨資料（需要管理员权限）"""
    token = request.args.get("token", "")
    if token not in ADMIN_USER_IDS:
        abort(403)
    
    mtx = fut_latest("mtx")
    imtx = fut_latest("imtx")
    
    return jsonify({
        "mtx": mtx,
        "imtx": imtx
    })

# ──────────────────────────────────────
#  LINE 事件處理
# ──────────────────────────────────────
@handler.add(MessageEvent, message=TextMessage)
def on_message(event: MessageEvent):
    uid = event.source.user_id
    text = event.message.text.strip()

    if text == "/today":
        # 添加日誌以追蹤
        logging.info("處理 /today 命令")
        report = build_report()
        logging.info(f"生成報告: {report}")
        reply(event.reply_token, report)
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
#  本地運行（用於測試）
# ──────────────────────────────────────
@app.route("/test")
def test_endpoint():
    """測試報告生成"""
    return build_report()

# ──────────────────────────────────────
#  Local run（Render 用 gunicorn，不會執行這段）
# ──────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
