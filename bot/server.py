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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
line_api = LineBotApi(ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# ──────────────────────────────────────
#  小工具
# ──────────────────────────────────────
def reply(token: str, text: str) -> None:
    """包一層，避免每次都要 new TextSendMessage"""
    line_api.reply_message(token, TextSendMessage(text=text))

def format_number(val: int | float | None) -> str:
    """把 None / 0 轉成 '–'，並加上千分位及正負符號"""
    if val is None:
        return "–"
    return f"{val:+,}" if val != 0 else "0"

def build_report() -> str:
    """建立報告，包含 PC ratio 和散戶小台/微台未平倉"""
    # 使用台灣時區
    tw_tz = timezone(timedelta(hours=8))
    
    # 獲取 PC ratio
    pc_data = pc_latest()
    pc_ratio = "–"
    try:
        if pc_data and isinstance(pc_data, dict) and 'pc_oi_ratio' in pc_data:
            pc_ratio = f"{pc_data['pc_oi_ratio']:.2f}"
            pc_date = pc_data['date'].replace(tzinfo=timezone.utc)
            pc_date_str = pc_date.astimezone(tw_tz).strftime("%Y/%m/%d (%a)")
            logger.info(f"PC ratio 資料日期: {pc_date_str}, 比值: {pc_ratio}")
    except Exception as e:
        logger.error(f"處理 PC ratio 資料時發生錯誤: {e}")
    
    # 獲取最新期貨資料
    mtx_data = fut_latest('mtx')
    imtx_data = fut_latest('imtx')
    
    mtx_net = "–"
    imtx_net = "–"
    date_str = "–"
    
    try:
        # 取得資料日期
        if mtx_data and 'date' in mtx_data:
            date_obj = mtx_data['date'].replace(tzinfo=timezone.utc)
            date_str = date_obj.astimezone(tw_tz).strftime("%Y/%m/%d (%a)")
            logger.info(f"期貨資料日期: {date_str}")
        
        # 取得散戶淨額
        if mtx_data and 'retail_net' in mtx_data:
            mtx_net = format_number(mtx_data['retail_net'])
            logger.info(f"小台散戶淨額: {mtx_net}")
        
        if imtx_data and 'retail_net' in imtx_data:
            imtx_net = format_number(imtx_data['retail_net'])
            logger.info(f"微台散戶淨額: {imtx_net}")
    except Exception as e:
        logger.error(f"處理期貨資料時發生錯誤: {e}")
    
    # 當前日期格式化（用於顯示當前時間）
    now = datetime.now(tw_tz).strftime("%H:%M:%S")
    
    # 構建報告
    report = (
        f"📊 期貨籌碼報告 ({now})\n"
        f"日期：{date_str}\n"
        f"🧮 PC ratio 未平倉比：{pc_ratio}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{mtx_net}\n"
        f"微台：{imtx_net}"
    )
    
    # 添加詳細分解（僅當資料存在時）
    if mtx_data and all(k in mtx_data for k in ['prop_net', 'itf_net', 'foreign_net']):
        report += "\n\n📝 小台成分分解\n"
        report += f"自營商：{format_number(mtx_data['prop_net'])}\n"
        report += f"投信：{format_number(mtx_data['itf_net'])}\n"
        report += f"外資：{format_number(mtx_data['foreign_net'])}\n"
        sum_inst = mtx_data['prop_net'] + mtx_data['itf_net'] + mtx_data['foreign_net']
        report += f"三大法人：{format_number(sum_inst)}"
    
    return report

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
        logger.error("Invalid signature")
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
    pc = pc_latest()
    
    return jsonify({
        "mtx": mtx,
        "imtx": imtx,
        "pc_ratio": pc
    })

# ──────────────────────────────────────
#  LINE 事件處理
# ──────────────────────────────────────
@handler.add(MessageEvent, message=TextMessage)
def on_message(event: MessageEvent):
    uid = event.source.user_id
    text = event.message.text.strip()

    if text.lower() in ["/today", "/report", "/籌碼"]:
        # 添加日誌以追蹤
        logger.info(f"使用者 {uid} 使用命令: {text}")
        try:
            report = build_report()
            logger.info(f"生成籌碼報告，長度: {len(report)}")
            reply(event.reply_token, report)
        except Exception as e:
            logger.exception(f"生成報告時發生錯誤: {e}")
            reply(event.reply_token, f"生成報告時發生錯誤，請稍後再試")
        return

    if text == "/reset_fut":
        if uid not in ADMIN_USER_IDS:
            reply(event.reply_token, "權限不足")
            return

        # 假日或平日都強制重新抓
        try:
            logger.info(f"管理員 {uid} 執行重新抓取期貨資料")
            fut_fetch(force=True)
            reply(event.reply_token, "期貨資料已重新抓取完成！")
        except Exception as e:
            logger.exception(f"重新抓取期貨資料失敗: {e}")
            reply(event.reply_token, f"抓取失敗：{e}")
        return

    if text == "/help":
        help_text = (
            "📊 期貨籌碼機器人 📊\n\n"
            "可用指令：\n"
            "/today 或 /report - 顯示當日期貨籌碼報告\n"
            "/籌碼 - 同上\n"
            "/help - 顯示此幫助信息"
        )
        if uid in ADMIN_USER_IDS:
            help_text += "\n\n管理員指令：\n/reset_fut - 重新抓取期貨資料"
            
        reply(event.reply_token, help_text)
        return

    # 其它訊息提示使用者使用正確指令
    reply(event.reply_token, "請使用 /today 或 /report 查看期貨籌碼報告\n或使用 /help 查看所有指令")

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
