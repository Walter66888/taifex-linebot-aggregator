# -*- coding: utf-8 -*-
"""
fut_contracts.py  v4.3  – final stable
--------------------------------------
• long OI (idx 6) - short OI (idx 8) → 淨口數
• 自動偵測欄位，不受缺格影響
• 含 latest() 供 bot.server 匯入
"""

from __future__ import annotations
import datetime as dt
import logging
import re
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from pymongo import UpdateOne

from utils.db import get_col

URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
COL = get_col("fut_contracts")
COL.create_index([("date", 1), ("product", 1)], unique=True)

PROD_MAP = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}
ROLE_MAP = {
    "自營商": "prop_net",
    "自營商(避險)": "prop_net",
    "投信": "itf_net",
    "外資": "foreign_net",
    "外資及陸資": "foreign_net",
}


def _clean_int(s: str) -> int:
    s = s.replace(",", "").replace(" ", "").replace("−", "-").replace("‑", "-")
    return int(s) if s.lstrip("-").isdigit() else 0


def _date_from_html(html: str) -> dt.date:
    m = re.search(r"日期\s*(\d{4}/\d{2}/\d{2})", html)
    return dt.datetime.strptime(m.group(1), "%Y/%m/%d").date()


def parse(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    trade_date = _date_from_html(html)
    docs: Dict[str, Dict] = {}

    cur_prod = ""
    for tr in soup.find_all("tr", class_="12bk"):
        tds = tr.find_all("td")
        if not tds:
            continue

        # 商品名稱列：第二格帶 rowspan=3
        if len(tds) > 1 and tds[1].has_attr("rowspan"):
            prod_name = tds[1].get_text(strip=True)
            cur_prod = PROD_MAP.get(prod_name, "")
            if not cur_prod:
                continue
            docs[cur_prod] = {
                "date": dt.datetime.combine(trade_date, dt.time()),
                "product": cur_prod,
                "prop_net": 0,
                "itf_net": 0,
                "foreign_net": 0,
            }

        if cur_prod not in docs:
            continue  # 我們不關心的商品

        # 判斷此列角色
        role_cell = tds[2] if len(tds) > 2 else tds[0]
        role_key = ROLE_MAP.get(role_cell.get_text(strip=True))
        if role_key is None:
            continue

        numbers = [_clean_int(td.get_text()) for td in tds[3:] if td.get_text()]
        if len(numbers) < 9:
            continue  # 至少要有 long/short OI

        net_oi = numbers[6] - numbers[8]  # long OI - short OI
        docs[cur_prod][role_key] = net_oi

    # 補散戶
    result = []
    for d in docs.values():
        d["retail_net"] = -(d["prop_net"] + d["itf_net"] + d["foreign_net"])
        result.append(d)
    return result


def fetch() -> List[Dict]:
    html = requests.get(URL, timeout=10).text
    docs = parse(html)
    if not docs:
        logging.error("No docs parsed!"); return []

    ops = [
        UpdateOne(
            {"date": d["date"], "product": d["product"]},
            {"$set": d},
            upsert=True,
        )
        for d in docs
    ]
    if ops:
        COL.bulk_write(ops, ordered=False)
        logging.info("upsert %s docs into fut_contracts", len(ops))
    return docs


def latest(prod: str = "mtx", days: int = 1) -> List[Dict]:
    """讓 bot.server 調用：取最新 n 筆"""
    return list(
        COL.find({"product": prod}, {"_id": 0})
        .sort("date", -1)
        .limit(days)
    )


if __name__ == "__main__":
    from pprint import pprint
    pprint(fetch())
