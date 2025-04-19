# -*- coding: utf-8 -*-
# crawler/fut_contracts.py  v5.1  2025‑04‑19
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

LOG      = logging.getLogger(__name__)
URL      = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEADERS  = {"User-Agent": "Mozilla/5.0"}

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
    txt = txt.strip() if txt else "0"
    # 僅保留數字和負號
    cleaned = re.sub(r"[^\d\-]", "", txt) or "0"
    return int(cleaned)


def _extract_net_value(row, col_index: int) -> int:
    """
    從表格行中提取淨值
    :param row: BeautifulSoup 的 tr 元素
    :param col_index: 含有多空淨額的列索引
    :return: 解析後的整數值
    """
    cells = row.find_all("td")
    if len(cells) <= col_index:
        LOG.debug(f"行中的儲存格數量不足，預期至少 {col_index+1} 個，實際有 {len(cells)} 個")
        return 0
    
    # 從儲存格中提取包含數字的 font 標籤
    font_tag = cells[col_index].find("font")
    if not font_tag:
        LOG.debug(f"在儲存格 {col_index} 中未找到 font 標籤")
        return 0
    
    text = font_tag.get_text(strip=True)
    LOG.debug(f"從 font 標籤提取的文本：{text}")
    return _clean_int(text)


# ────────────────────────── 核心解析函數 ─────────────────────────────
def parse_html(html: str) -> tuple[datetime, list[dict]]:
    """解析 HTML 內容，返回 (日期物件, 解析後文檔列表)"""
    soup = BeautifulSoup(html, "lxml")

    # 解析日期
    m = re.search(r"日期(\d{4}/\d{2}/\d{2})", html)
    if not m:
        raise RuntimeError("找不到日期")
    date_obj = datetime.strptime(m.group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    # 打印原始 HTML 的部分內容，用於調試
    LOG.debug(f"HTML 前 200 字符: {html[:200]}")
    
    # 找出所有表格行
    rows = soup.find_all("tr", class_="12bk")
    LOG.debug(f"找到 {len(rows)} 行資料")
    
    if not rows:
        raise RuntimeError("tbody 無 tr.12bk 資料列")

    # 存儲解析結果的字典
    result = {}

    # 標記當前正在處理的產品
    current_product = None
    
    # 遍歷所有表格行
    for i, tr in enumerate(rows):
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        # 檢查是否是新產品的開始
        prod_cell = cells[1].get_text(strip=True)
        if prod_cell:
            current_product = prod_cell
            LOG.debug(f"發現產品名稱: {current_product}")

        # 如果不是目標產品，跳過
        if current_product not in TARGETS:
            continue

        # 獲取身份別（自營商/投信/外資）
        identity = cells[2].get_text(strip=True)
        LOG.debug(f"處理 {current_product} - {identity}")
        
        # 找出正確的未平倉淨額列索引
        # 一般是第 13 列 (索引 12)，這裡多個條件確保能正確找到
        net_value = 0
        
        # 嘗試從第 13 列提取
        try:
            # 未平倉淨額通常在第 13 列 (索引 12)
            net_value = _extract_net_value(tr, 12)
            LOG.debug(f"從第 13 列提取的淨值: {net_value}")
        except Exception as e:
            LOG.debug(f"從第 13 列提取失敗: {e}")
            
            # 如果失敗，嘗試從第 14 列提取
            try:
                net_value = _extract_net_value(tr, 13)
                LOG.debug(f"從第 14 列提取的淨值: {net_value}")
            except Exception as e:
                LOG.debug(f"從第 14 列提取也失敗: {e}")
        
        # 初始化該產品的記錄
        if current_product not in result:
            result[current_product] = {
                "prop_net": 0,
                "itf_net": 0,
                "foreign_net": 0
            }
        
        # 根據身份別存儲淨值
        if identity == "自營商":
            result[current_product]["prop_net"] = net_value
        elif identity == "投信":
            result[current_product]["itf_net"] = net_value
        elif identity == "外資":
            result[current_product]["foreign_net"] = net_value

    # 生成最終文檔列表
    docs = []
    for pname, vals in result.items():
        # 計算散戶淨額 = -(自營商淨額 + 投信淨額 + 外資淨額)
        retail = -(vals["prop_net"] + vals["itf_net"] + vals["foreign_net"])
        
        docs.append({
            "date": date_obj,
            "product": TARGETS[pname],
            **vals,
            "retail_net": retail,
        })
        
        # 記錄解析結果
        LOG.info(f"{pname} 解析結果: 自營商={vals['prop_net']}, 投信={vals['itf_net']}, 外資={vals['foreign_net']}, 散戶={retail}")
    
    return date_obj, docs


# ─────────────────────────── 抓取與儲存函數 ───────────────────────────
def _is_weekend() -> bool:
    """檢查今天是否為週末"""
    return datetime.now().weekday() >= 5       # 5,6 -> Sat, Sun


def fetch(force: bool = False) -> list[dict]:
    """
    抓取期交所數據並保存
    1. 原始 HTML 保存到 fut_raw_html 集合
    2. 解析後資料保存到 fut_contracts 集合
    """
    if _is_weekend() and not force:
        raise RuntimeError("週末不抓 (加 --force 可強制)")

    # 1. 下載 HTML
    LOG.info("正在從期交所下載數據...")
    res = requests.get(URL, headers=HEADERS, timeout=20)
    res.raise_for_status()
    html_content = res.text
    
    # 2. 解析 HTML 獲取日期和數據
    LOG.info("解析 HTML 內容...")
    date_obj, docs = parse_html(html_content)
    
    if not docs:
        raise RuntimeError("未取得任何商品資料")

    # 3. 保存原始 HTML 到 fut_raw_html 集合
    RAW_COL.update_one(
        {"date": date_obj},
        {"$set": {"html_content": html_content, "fetched_at": datetime.now()}},
        upsert=True
    )
    LOG.info(f"HTML 保存成功，長度: {len(html_content)} 字符")
    
    # 4. 保存解析後資料到 fut_contracts 集合
    ops = [
        UpdateOne({"product": d["product"], "date": d["date"]},
                  {"$set": d}, upsert=True)
        for d in docs
    ]
    COL.bulk_write(ops, ordered=False)
    LOG.info(f"解析數據保存成功: {len(docs)} 筆記錄")
    
    return docs


def latest(product: str | None = None) -> dict | None:
    """
    獲取最新的期貨數據
    1. 如果指定了 product，則返回該產品的最新數據
    2. 否則返回最新日期的所有產品數據
    """
    query = {"product": product} if product else {}
    return COL.find_one(query, {"_id": 0}, sort=[("date", -1)])


def get_raw_html(date: datetime = None) -> str | None:
    """獲取指定日期的原始 HTML，如未指定日期則獲取最新的"""
    query = {"date": date} if date else {}
    doc = RAW_COL.find_one(query, sort=[("date", -1)])
    return doc["html_content"] if doc else None


# ────────────────────────── 命令行介面 ─────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--force", action="store_true", help="ignore weekend guard")
    ap.add_argument("--debug", action="store_true", help="啟用調試日誌")
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
