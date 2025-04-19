# -*- coding: utf-8 -*-
# crawler/fut_contracts.py  v7.0  2025‑04‑19
"""
抓取『三大法人‑區分各期貨契約』並存儲整個 HTML：
  ‑ 小型臺指期貨 (product = mtx)
  ‑ 微型臺指期貨 (product = imtx)

使用方式：
  python -m crawler.fut_contracts run             # 平日自動跳過假日
  python -m crawler.fut_contracts run --force     # 強制抓

資料表：
  1. taifex.fut_raw_html      - 原始 HTML 內容 {date, html_content}
  2. taifex.fut_contracts     - 解析後資料 {product,date,prop_net,itf_net,foreign_net,retail_net}
"""

from __future__ import annotations
import re, requests, argparse, logging, pprint, sys
from datetime import datetime, timezone
from collections import defaultdict

from bs4 import BeautifulSoup
from pymongo import ASCENDING, UpdateOne
from utils.db import get_col

LOG = logging.getLogger(__name__)
URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

# 創建兩個集合：一個存原始 HTML，一個存解析後資料
RAW_COL = get_col("fut_raw_html")
RAW_COL.create_index([("date", ASCENDING)], unique=True)

COL = get_col("fut_contracts")
COL.create_index([("product", 1), ("date", 1)], unique=True)

TARGETS = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}

# ───────────────────────── 內部輔助函數 ──────────────────────────
def _clean_int(txt: str) -> int:
    """清理字串為整數，移除所有非數字和負號的字符"""
    if not txt:
        return 0
    txt = txt.strip() if txt else "0"
    # 僅保留數字和負號
    cleaned = re.sub(r"[^\d\-]", "", txt) or "0"
    return int(cleaned)


# ────────────────────────── 核心解析函數 ─────────────────────────────
def parse_html(html: str) -> tuple[datetime, list[dict]]:
    """
    解析 HTML 內容，返回 (日期物件, 解析後文檔列表)
    
    特別處理 HTML 表格中的三大法人資料：
    1. 找到小型臺指期貨和微型臺指期貨所在的位置
    2. 針對每個產品，獲取自營商、投信和外資的未平倉淨額
    3. 計算散戶淨額 = -(自營商淨額 + 投信淨額 + 外資淨額)
    """
    soup = BeautifulSoup(html, "lxml")

    # 解析日期
    m = re.search(r"日期(\d{4}/\d{2}/\d{2})", html)
    if not m:
        raise RuntimeError("找不到日期")
    date_obj = datetime.strptime(m.group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)
    LOG.info(f"解析到日期: {date_obj.strftime('%Y/%m/%d')}")

    # 找出所有包含商品名稱的表格單元格
    product_cells = soup.find_all("td", class_="left_tit", attrs={"rowspan": "3"})
    if not product_cells:
        LOG.warning("找不到產品單元格")
        raise RuntimeError("HTML結構異常：找不到產品單元格")
    
    LOG.info(f"找到 {len(product_cells)} 個產品單元格")
    
    # 存儲結果的字典
    result = {}
    
    # 遍歷所有產品單元格，找出我們要的產品
    for product_cell in product_cells:
        product_name_div = product_cell.find("div", align="center")
        if not product_name_div:
            continue
            
        product_name = product_name_div.text.strip()
        
        # 只處理目標產品
        if product_name not in TARGETS:
            continue
            
        LOG.info(f"發現目標產品: {product_name}")
        
        # 找到產品所在的行
        product_row = product_cell.parent
        if not product_row:
            LOG.warning(f"找不到產品 {product_name} 的表格行")
            continue
        
        # 初始化該產品的資料
        result[product_name] = {
            "prop_net": 0,
            "itf_net": 0, 
            "foreign_net": 0,
            "retail_net": 0
        }
        
        # 處理三個連續的行（自營商、投信、外資）
        current_row = product_row
        
        # 處理自營商行
        identity_cell = current_row.find("td", class_="left_tit", attrs={"scope": "row"})
        if identity_cell and "自營商" in identity_cell.text:
            # 找未平倉淨額 (倒數第 3 列)
            all_cells = current_row.find_all("td", attrs={"align": "right", "nowrap": True})
            if len(all_cells) >= 13:
                net_cell = all_cells[12]  # 第13個儲存格 (索引12) 是未平倉淨額-口數
                font_tag = net_cell.find("font")
                if font_tag:
                    result[product_name]["prop_net"] = _clean_int(font_tag.text)
                    LOG.info(f"{product_name} 自營商淨額: {result[product_name]['prop_net']}")
        
        # 處理投信行
        current_row = current_row.find_next("tr", class_="12bk")
        if current_row:
            identity_cell = current_row.find("td", class_="left_tit", attrs={"scope": "row"})
            if identity_cell and "投信" in identity_cell.text:
                all_cells = current_row.find_all("td", attrs={"align": "right", "nowrap": True})
                if len(all_cells) >= 13:
                    net_cell = all_cells[12]
                    font_tag = net_cell.find("font")
                    if font_tag:
                        result[product_name]["itf_net"] = _clean_int(font_tag.text)
                        LOG.info(f"{product_name} 投信淨額: {result[product_name]['itf_net']}")
        
        # 處理外資行
        current_row = current_row.find_next("tr", class_="12bk")
        if current_row:
            identity_cell = current_row.find("td", class_="left_tit", attrs={"scope": "row"})
            if identity_cell and "外資" in identity_cell.text:
                all_cells = current_row.find_all("td", attrs={"align": "right", "nowrap": True})
                if len(all_cells) >= 13:
                    net_cell = all_cells[12]
                    font_tag = net_cell.find("font")
                    if font_tag:
                        result[product_name]["foreign_net"] = _clean_int(font_tag.text)
                        LOG.info(f"{product_name} 外資淨額: {result[product_name]['foreign_net']}")
        
        # 計算散戶淨額
        prop_net = result[product_name]["prop_net"]
        itf_net = result[product_name]["itf_net"]
        foreign_net = result[product_name]["foreign_net"]
        retail_net = -(prop_net + itf_net + foreign_net)
        result[product_name]["retail_net"] = retail_net
        
        LOG.info(f"{product_name} 計算散戶淨額: -({prop_net} + {itf_net} + {foreign_net}) = {retail_net}")
    
    # 確認是否找到所有目標產品
    missing_products = set(TARGETS.keys()) - set(result.keys())
    if missing_products:
        LOG.warning(f"找不到以下產品資料: {', '.join(missing_products)}")
    
    # 生成最終文檔列表
    docs = []
    for pname, vals in result.items():
        docs.append({
            "date": date_obj,
            "product": TARGETS[pname],
            **vals,
        })
    
    return date_obj, docs


# ─────────────────────────── 抓取與儲存函數 ───────────────────────────
def _is_weekend() -> bool:
    """檢查今天是否為週末"""
    return datetime.now().weekday() >= 5  # 5,6 -> Sat, Sun


def fetch(force: bool = False) -> list[dict]:
    """
    抓取期交所數據並保存
    1. 原始 HTML 保存到 fut_raw_html 集合
    2. 解析後資料保存到 fut_contracts 集合
    """
    if _is_weekend() and not force:
        LOG.info("今天是週末，不抓取資料 (加 --force 可強制)")
        raise RuntimeError("週末不抓 (加 --force 可強制)")

    # 1. 下載 HTML
    LOG.info("正在從期交所下載數據...")
    try:
        res = requests.get(URL, headers=HEADERS, timeout=20)
        res.raise_for_status()
        html_content = res.text
    except requests.RequestException as e:
        LOG.error(f"下載數據時發生錯誤: {e}")
        raise RuntimeError(f"下載失敗: {e}")
    
    # 2. 解析 HTML 獲取日期和數據
    LOG.info("解析 HTML 內容...")
    try:
        date_obj, docs = parse_html(html_content)
    except Exception as e:
        LOG.error(f"解析 HTML 時發生錯誤: {e}")
        raise RuntimeError(f"解析失敗: {e}")
    
    if not docs:
        LOG.warning("未取得任何商品資料")
        raise RuntimeError("未取得任何商品資料")

    # 3. 保存原始 HTML 到 fut_raw_html 集合
    try:
        RAW_COL.update_one(
            {"date": date_obj},
            {"$set": {"html_content": html_content, "fetched_at": datetime.now(timezone.utc)}},
            upsert=True
        )
        LOG.info(f"HTML 保存成功，長度: {len(html_content)} 字符")
    except Exception as e:
        LOG.error(f"保存 HTML 時發生錯誤: {e}")
        raise RuntimeError(f"保存 HTML 失敗: {e}")
    
    # 4. 保存解析後資料到 fut_contracts 集合
    try:
        ops = [
            UpdateOne(
                {"product": d["product"], "date": d["date"]},
                {"$set": d},
                upsert=True
            ) for d in docs
        ]
        result = COL.bulk_write(ops, ordered=False)
        LOG.info(f"解析數據保存成功: {len(docs)} 筆記錄 (新增:{result.upserted_count}, 修改:{result.modified_count})")
    except Exception as e:
        LOG.error(f"保存解析數據時發生錯誤: {e}")
        raise RuntimeError(f"保存解析數據失敗: {e}")
    
    return docs


def latest(product: str | None = None, count: int = 1) -> dict | list[dict] | None:
    """
    獲取最新的期貨數據
    
    參數:
        product: 產品代碼，如 'mtx' 或 'imtx'，如果為 None 則不限制產品
        count: 要獲取的記錄數量，預設為 1
    
    返回:
        如果 count=1，返回單個文檔 (dict 或 None)
        如果 count>1，返回文檔列表 (list of dict)
    """
    query = {"product": product} if product else {}
    
    if count == 1:
        return COL.find_one(query, {"_id": 0}, sort=[("date", -1)])
    else:
        docs = list(COL.find(query, {"_id": 0}, sort=[("date", -1)]).limit(count))
        return docs


def get_raw_html(date: datetime = None) -> str | None:
    """獲取指定日期的原始 HTML，如未指定日期則獲取最新的"""
    query = {"date": date} if date else {}
    doc = RAW_COL.find_one(query, sort=[("date", -1)])
    return doc["html_content"] if doc else None


# ────────────────────────── 命令行介面 ─────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "show"])
    ap.add_argument("--force", action="store_true", help="ignore weekend guard")
    ap.add_argument("--debug", action="store_true", help="啟用調試日誌")
    ap.add_argument("--product", help="指定要顯示的產品 (mtx/imtx)")
    ap.add_argument("--count", type=int, default=1, help="要顯示的資料筆數")
    args = ap.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        LOG.setLevel(logging.DEBUG)
        LOG.debug("調試模式已啟用")

    if args.cmd == "run":
        try:
            result = fetch(args.force)
            pprint.pp(result)
        except Exception as e:
            LOG.error(f"爬蟲錯誤: {e}")
            sys.exit(1)
    elif args.cmd == "show":
        if args.product:
            docs = latest(args.product, args.count)
        else:
            docs = []
            for product in TARGETS.values():
                doc = latest(product)
                if doc:
                    docs.append(doc)
        
        pprint.pp(docs if isinstance(docs, list) else [docs])
