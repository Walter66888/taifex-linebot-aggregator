# -*- coding: utf-8 -*-
# crawler/fut_contracts.py  v5.0  2025‑04‑19
"""
抓取『三大法人‑区分各期貨契約』并存储整个 HTML：
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

# ───────────────────────── internal helpers ──────────────────────────
def _clean_int(txt: str) -> int:
    """清理字串為整數"""
    return int(re.sub(r"[^\d\-]", "", txt or "0") or 0)


def _row_net(cells) -> int:
    """取『未平倉多空淨額‑口數』：行长可能 15,14,13 → index 13 / 12 / 11"""
    length = len(cells)
    if length >= 14:
        idx = 13       # 15 或 14 欄
    elif length == 13:
        idx = 11
    else:
        raise ValueError(f"不支援的欄位數 {length}")
    return _clean_int(cells[idx].get_text())


# ────────────────────────── core parser ─────────────────────────────
def parse_html(html: str) -> tuple[datetime, list[dict]]:
    """解析 HTML 內容，返回 (日期物件, 解析後文檔列表)"""
    soup = BeautifulSoup(html, "lxml")

    # 解析日期
    m = re.search(r"日期(\d{4}/\d{2}/\d{2})", html)
    if not m:
        raise RuntimeError("找不到日期")
    date_obj = datetime.strptime(m.group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    rows = soup.find_all("tr", class_="12bk")
    if not rows:
        raise RuntimeError("tbody 無 tr.12bk 資料列")

    result: dict[str, dict] = {}     # prod_name → dict

    current_product: str | None = None
    for tr in rows:
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        # 若第 2 欄有文字代表新的商品開始
        prod_cell_txt = cells[1].get_text(strip=True)
        if prod_cell_txt:
            current_product = prod_cell_txt

        if current_product not in TARGETS:
            continue                          # 只要 mtx / imtx

        idf = cells[2].get_text(strip=True)   # 自營商 / 投信 / 外資
        try:
            net = _row_net(cells)
        except ValueError as e:
            LOG.debug("skip row: %s", e)
            continue

        entry = result.setdefault(
            current_product,
            {"prop_net": 0, "itf_net": 0, "foreign_net": 0}
        )
        if idf == "自營商":
            entry["prop_net"] = net
        elif idf == "投信":
            entry["itf_net"] = net
        elif idf == "外資":
            entry["foreign_net"] = net

    docs: list[dict] = []
    for pname, vals in result.items():
        retail = -(vals["prop_net"] + vals["itf_net"] + vals["foreign_net"])
        docs.append({
            "date": date_obj,
            "product": TARGETS[pname],
            **vals,
            "retail_net": retail,
        })
    return date_obj, docs


# ─────────────────────────── fetch & util ───────────────────────────
def _is_weekend() -> bool:
    return datetime.now().weekday() >= 5       # 5,6 -> Sat, Sun


def fetch(force: bool = False) -> list[dict]:
    """
    抓取期交所數據並保存
    1. 原始 HTML 保存到 fut_raw_html 集合
    2. 解析後資料保存到 fut_contracts 集合
    """
    if _is_weekend() and not force:
        raise RuntimeError("週末不抓 (加 --force 可強制)")

    # 1. 下载 HTML
    res = requests.get(URL, headers=HEADERS, timeout=20)
    res.raise_for_status()
    html_content = res.text
    
    # 2. 解析 HTML 获取日期和数据
    date_obj, docs = parse_html(html_content)
    
    if not docs:
        raise RuntimeError("未取得任何商品資料")

    # 3. 保存原始 HTML 到 fut_raw_html 集合
    RAW_COL.update_one(
        {"date": date_obj},
        {"$set": {"html_content": html_content, "fetched_at": datetime.now()}},
        upsert=True
    )
    LOG.info("HTML 保存成功，長度: %d 字符", len(html_content))
    
    # 4. 保存解析後資料到 fut_contracts 集合
    ops = [
        UpdateOne({"product": d["product"], "date": d["date"]},
                  {"$set": d}, upsert=True)
        for d in docs
    ]
    COL.bulk_write(ops, ordered=False)
    LOG.info("解析数据保存成功: %d 条记录", len(docs))
    
    return docs


def latest(product: str | None = None) -> dict | None:
    """
    获取最新的期货数据
    1. 如果指定了 product，则返回该产品的最新数据
    2. 否则返回最新日期的所有产品数据
    """
    query = {"product": product} if product else {}
    return COL.find_one(query, {"_id": 0}, sort=[("date", -1)])


def get_raw_html(date: datetime = None) -> str | None:
    """获取指定日期的原始 HTML，如未指定日期则获取最新的"""
    query = {"date": date} if date else {}
    doc = RAW_COL.find_one(query, sort=[("date", -1)])
    return doc["html_content"] if doc else None


# ────────────────────────── CLI ─────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--force", action="store_true", help="ignore weekend guard")
    args = ap.parse_args()

    if args.cmd == "run":
        try:
            pprint.pp(fetch(args.force))
        except Exception as e:
            LOG.error("crawler error: %s", e)
            sys.exit(1)
