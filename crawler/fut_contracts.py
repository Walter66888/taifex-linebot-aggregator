# -*- coding: utf-8 -*-
"""
crawler.fut_contracts  v4.5  — 抓「三大法人‑區分各期貨契約」
----------------------------------------------------------------
• 只關心『小型臺指期貨 (mtx)』與『微型臺指期貨 (imtx)』
• 未平倉 多空淨額〈口數〉在每列數字的 index = 10（自營商列從 tds[3:] 數）
• long OI – short OI → 淨口數
• 散戶 = 反手 = -(自營 + 投信 + 外資)
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from pymongo import UpdateOne

from utils.db import get_col

# ───── 基本設定 ──────────────────────────────────────────
URL = "https://www.taifex.com.tw/cht/3/futContractsDate"
COL = get_col("fut_contracts")
COL.create_index([("date", 1), ("product", 1)], unique=True)  # (date, product) 唯一索引

PRODUCT_MAP = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}
ROLE_MAP = {                 # 欄位對照
    "自營商": "prop_net",
    "自營商(避險)": "prop_net",
    "投信": "itf_net",
    "外資": "foreign_net",
    "外資及陸資": "foreign_net",
}

_INT_RE = re.compile(r"-?\d+")


# ───── 工具函式 ──────────────────────────────────────────
def _i(s: str) -> int:
    """去千分位逗號、全形空白後轉 int；非數字回 0"""
    s = s.replace(",", "").replace(" ", "").replace("−", "-").replace("‑", "-")
    return int(s) if _INT_RE.fullmatch(s) else 0


def _trade_date(html: str) -> datetime:
    m = re.search(r"日期\s*(\d{4}/\d{2}/\d{2})", html)
    return (
        datetime.strptime(m.group(1), "%Y/%m/%d")
        .replace(tzinfo=timezone.utc)
    )


# ───── 解析主流程 ────────────────────────────────────────
def parse(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    tdate = _trade_date(html)

    docs: Dict[str, Dict] = {}
    cur_prod = ""

    for tr in soup.find_all("tr", class_="12bk"):
        tds = tr.find_all("td")
        if len(tds) < 15:        # 最少要有完整欄數
            continue

        # 商品切換列：tds[1] 有 rowspan="3"
        if len(tds) > 1 and tds[1].has_attr("rowspan"):
            prod_name = tds[1].get_text(strip=True)
            cur_prod = PRODUCT_MAP.get(prod_name, "")
            if not cur_prod:
                continue
            docs[cur_prod] = {
                "date": tdate,
                "product": cur_prod,
                "prop_net": 0,
                "itf_net": 0,
                "foreign_net": 0,
            }

        if cur_prod not in docs:
            continue  # 非目標商品

        # 判斷角色欄位置
        role_cell, num_start = (tds[2], 3) if len(tds) > 2 else (tds[0], 1)
        role_key = ROLE_MAP.get(role_cell.get_text(strip=True))
        if role_key is None:
            continue

        numbers = [_i(td.get_text()) for td in tds[num_start:]]
        if len(numbers) < 11:
            continue  # 不足以取到 long/short OI

        net_oi = numbers[10]  # index 10 = 未平倉多空淨額 口數
        docs[cur_prod][role_key] = net_oi

    # 散戶 = 反手
    result: List[Dict] = []
    for d in docs.values():
        d["retail_net"] = -(d["prop_net"] + d["itf_net"] + d["foreign_net"])
        result.append(d)

    return result


# ───── 對外函式 ──────────────────────────────────────────
def fetch() -> List[Dict]:
    res = requests.get(URL, timeout=20)
    res.encoding = "utf-8"
    docs = parse(res.text)

    if docs:
        COL.bulk_write(
            [
                UpdateOne(
                    {"date": d["date"], "product": d["product"]},
                    {"$set": d},
                    upsert=True,
                )
                for d in docs
            ],
            ordered=False,
        )
    return docs


def latest(prod: str = "mtx", days: int = 1) -> List[Dict]:
    """保留舊介面給 bot.server.safe_latest() 使用"""
    return list(
        COL.find({"product": prod}, {"_id": 0})
        .sort("date", -1)
        .limit(days)
    )


# ───── CLI 測試 ─────────────────────────────────────────
if __name__ == "__main__":
    from pprint import pprint
    pprint(fetch())
