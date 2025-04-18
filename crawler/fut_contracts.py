"""
crawler/fut_contracts.py  v3.4  (stable)
----------------------------------------
抓取 https://www.taifex.com.tw/cht/3/futContractsDateExcel
寫入：小台 (mtx)‧微台 (imtx)   retail_net = -(prop+itf+foreign)

改動重點
• ensure_index()：只在「檢測到唯一且僅含 date 的舊索引」時刪除
• 若不存在複合唯一索引 -> 自動建立
"""

from __future__ import annotations
import re, sys
from datetime import datetime, timezone, timedelta

import bs4 as bs
import requests
from pymongo import ASCENDING, UpdateOne
from utils.db import get_col

# ── 常量 ──────────────────────────────────────────────
URL  = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD = {"User-Agent": "taifex-fut-crawler/3.4"}

TARGETS = {
    "小型臺指期貨": "mtx", "小型台指期貨": "mtx",
    "微型臺指期貨": "imtx","微型台指期貨": "imtx",
}
ROLE_MAP = {
    "自營商": "prop_net", "自營商(避險)": "prop_net",
    "投信":   "itf_net",
    "外資":   "foreign_net", "外資及陸資": "foreign_net",
}

DATE_RE = re.compile(r"日期\s*(\d{4}/\d{1,2}/\d{1,2})")
NUM_RE  = re.compile(r"^-?\d[\d,]*$")

# ── Mongo ────────────────────────────────────────────
COL = get_col("fut_contracts")

def ensure_index(col):
    """移除 legacy `date` 唯一索引（若存在）→ 建立 (date,product) 複合唯一索引"""
    for name, spec in col.index_information().items():
        if name == "_id_":  # ignore default id index
            continue
        keys = spec["key"]               # list[tuple]
        if keys == [("date", 1)] and spec.get("unique"):
            col.drop_index(name)
    if "date_1_product_1" not in col.index_information():
        col.create_index(
            [("date", ASCENDING), ("product", ASCENDING)],
            unique=True,
            name="date_1_product_1",
        )
ensure_index(COL)

# ── 工具函式 ────────────────────────────────────────────
def today_tw(): 
    return datetime.now(timezone(timedelta(hours=8))).date()

def _extract_net(nums):
    arr = [n.replace(",", "") for n in nums if NUM_RE.match(n)]
    return int(arr[-2]) if len(arr) >= 2 else None   # 倒數第2欄=口數

# ── 解析 HTML ──────────────────────────────────────────
def parse(html: str):
    soup = bs.BeautifulSoup(html, "lxml")
    span = soup.find(string
