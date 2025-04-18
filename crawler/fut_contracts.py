# -*- coding: utf-8 -*-
"""
fut_contracts.py  v3.6  2025‑04‑18
----------------------------------
抓「三大法人－區分各期貨契約」網頁，
只萃取『小型臺指期貨‧微型臺指期貨』的
　┌─ 自營商(prop)  投信(itf)  外資(foreign)
　└─ 未平倉多空淨額(口數)  ➜ retail = ‑(prop+itf+foreign)

廉價但可靠的抓法：
1. 每一個法人 row 固定含 15 個 <td>；第 3 欄起有 12 個純數字欄位
   index 10 (=第 11 欄) 永遠是「未平倉多空淨額－口數」。
   只要取它即可，不再去動態比對表頭文字，避免抓錯欄。
2. 以『商品名稱』判斷產品、以『身份別』判斷法人，累加完一次就停。
3. 建立 (date, product) 複合唯一索引 → 不會再撞 DuplicateKey。

──────────────────────────────────────────
"""
from __future__ import annotations

import datetime as _dt
import re
import logging
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
import pymongo                                    # type: ignore

from utils.db import get_col

_URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"

# ── Mongo ──────────────────────────────────────────────────────────
COL = get_col("fut_contracts")
# 只建立一次即可；若已存在 PyMongo 會吞掉 DuplicateKeyError
COL.create_index([("date", 1), ("product", 1)], unique=True)

# ── 表格->程式 轉換表 ────────────────────────────────────────────────
IDENT_MAP = {"自營商": "prop", "投信": "itf", "外資": "foreign"}
PRODUCT_CODE = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}

_NUM_RE = re.compile(r"[-\d,]+")


def _int(txt: str) -> int:
    """把 '8,551' → 8551 , '‑3,048' → -3048 , 其它 → 0"""
    m = _NUM_RE.search(txt.strip().replace("—", "0"))
    return int(m.group(0).replace(",", "")) if m else 0


# ── 解析 html ──────────────────────────────────────────────────────
def _date_from_html(html: str) -> _dt.datetime:
    m = re.search(r"日期\s*(\d{4}/\d{2}/\d{2})", html)
    if not m:
        raise ValueError("抓不到日期")
    return _dt.datetime.strptime(m.group(1), "%Y/%m/%d")


def parse(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("tbody tr.12bk")

    prod_stats: Dict[str, Dict[str, int]] = {}   # {code: {prop, itf, foreign}}

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 15:                # 保險：不是數字列就跳過
            continue

        prod_name = tds[1].get_text(strip=True)
        if prod_name not in PRODUCT_CODE:
            continue

        identity = tds[2].get_text(strip=True)
        role = IDENT_MAP.get(identity)
        if role is None:
            continue

        nums = [_int(td.get_text()) for td in tds[3:]]   # 12 個純數字欄
        if len(nums) < 11:
            continue
        net_lots = nums[10]               # ← 位置固定

        code = PRODUCT_CODE[prod_name]
        prod_stats.setdefault(code, {"prop": 0, "itf": 0, "foreign": 0})[role] = net_lots

        # 若三法人都到齊了就可以提早結束迴圈，節省解析時間
        if all(k in prod_stats[code] and isinstance(prod_stats[code][k], int)
               for k in ("prop", "itf", "foreign")) and len(prod_stats) == len(PRODUCT_CODE):
            pass  # 不 break，保險起見還是走完整個 tbody

    if not prod_stats:
        raise RuntimeError("找不到任何目標商品資料")

    date = _date_from_html(html)
    docs = []
    for code, st in prod_stats.items():
        retail = -(st["prop"] + st["itf"] + st["foreign"])
        docs.append(
            {
                "date": date,
                "product": code,
                "prop_net": st["prop"],
                "itf_net": st["itf"],
                "foreign_net": st["foreign"],
                "retail_net": retail,
            }
        )
    return docs


# ── 抓網頁 + 寫入 Mongo ─────────────────────────────────────────────
def fetch() -> List[dict]:
    logging.info("[fut_contracts] fetching taifex html…")
    res = requests.get(_URL, timeout=15)
    res.encoding = "utf-8"
    docs = parse(res.text)

    ops = [
        pymongo.UpdateOne(
            {"date": d["date"], "product": d["product"]},
            {"$set": d},
            upsert=True,
        )
        for d in docs
    ]
    if ops:
        COL.bulk_write(ops, ordered=False)
    logging.info("[fut_contracts] upsert %s docs", len(docs))
    return docs


# ── 查詢最近資料 (給 bot 用) ──────────────────────────────────────────
def latest(product: str, limit: int = 1) -> List[dict]:
    return list(COL.find({"product": product}).sort("date", -1).limit(limit))


if __name__ == "__main__":
    from pprint import pprint

    pprint(fetch())
