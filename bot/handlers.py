"""
極簡範例：收到「/today」時送出最新報表
Render WSGI 佈署：gunicorn bot.handlers:app
"""
import os
import logging
from datetime import datetime, timedelta, timezone
from linebot import LineBotApi, WebhookHandler
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from crawler.pc_ratio import latest as pc_latest
from crawler.fut_contracts import latest as fut_latest

line_api = LineBotApi(os.getenv("LINE_CHANNEL_ACCESS_TOKEN"))
handler = WebhookHandler(os.getenv("LINE_CHANNEL_SECRET"))

# 設定日誌
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Reporter --------------------------------------------------

def format_number(val: int | float | None) -> str:
    """把 None / 0 轉成 '–'，並加上千分位及正負符號"""
    if val is None:
        return "–"
    return f"{val:+,}" if val != 0 else "0"

def _build_report():
    """建立期貨籌碼報告，包含 PC ratio 和散戶小台/微台未平倉"""
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

# --- Line Webhook ---------------------------------------------

@handler.add(MessageEvent, message=TextMessage)
def on_message(event):
    text = event.message.text.strip().lower()
    if text in ["/today", "/report", "/籌碼"]:
        try:
            logger.info("處理籌碼報告請求")
            report = _build_report()
            logger.info(f"生成籌碼報告，長度: {len(report)}")
            line_api.reply_message(event.reply_token, TextSendMessage(report))
        except Exception as e:
            logger.exception(f"生成報告時發生錯誤: {e}")
            line_api.reply_message(event.reply_token, TextSendMessage("生成報告時發生錯誤，請稍後再試"))
    elif text == "/help":
        help_text = (
            "📊 期貨籌碼機器人 📊\n\n"
            "可用指令：\n"
            "/today 或 /report - 顯示當日期貨籌碼報告\n"
            "/籌碼 - 同上\n"
            "/help - 顯示此幫助信息"
        )
        line_api.reply_message(event.reply_token, TextSendMessage(help_text))
    else:
        line_api.reply_message(event.reply_token, TextSendMessage("請使用 /today 查看期貨籌碼報告"))

app = handler.app  # for gunicorn
