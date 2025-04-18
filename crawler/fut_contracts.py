# -*- coding: utf-8 -*-
"""
crawler.fut_contracts  v4.6 – 最終穩定版
---------------------------------------
• 修正投信/外資列因空白 <td> 被跳過 → 全欄補 0
• long OI - short OI (index 10) 取未平倉多空淨額口數
"""

from __future__ import annotations
import re
from datetime import datetime, timezone
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from pymongo import UpdateOne

from utils.db import get_col

URL = "https://www.taifex.com.tw/cht/3/futContractsDate"
COL = get_col("fut_contracts")
COL.create_index([("date", 1), ("product", 1)], unique=True)

PRODUCT_MAP = {"小型臺指期貨": "mtx", "微型臺指期貨": "imtx"}
ROLE_MAP = {
    "自營商": "prop_net",
    "自營商(避險)": "prop_net",
    "投信": "itf_net",
    "外資": "foreign_net",
    "外資及陸資": "foreign_net",
}

_INT = re.compile(r"-?\d+")


def _int_or_zero(text: str) -> int:
    text = text.replace(",", "").replace(" ", "").replace("−", "-").replace("‑", "-")
    return int(text) if _INT.fullmatch(text) else 0


def _trade_date(html: str) -> datetime:
    m = re.search(r"日期\s*(\d{4}/\d{2}/\d{2})", html)
    return datetime.strptime(m.group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)


def parse(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    tdate = _trade_date(html)
    docs: Dict[str, Dict] = {}
    cur_prod = ""

    for tr in soup.find_all("tr", class_="12bk"):
        tds = tr.find_all("td")
        if len(tds) < 15:
            continue

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
            continue

        role_cell, num_start = (tds[2], 3) if len(tds) > 2 else (tds[0], 1)
        role_key = ROLE_MAP.get(role_cell.get_text(strip=True))
        if role_key is None:
            continue

        # 補 0 以固定長度 12
        numbers = [
            _int_or_zero(td.get_text(strip=True)) for td in tds[num_start : num_start + 12]
        ]
        if len(numbers) < 11:
            continue

        net_oi = numbers[10]  # 未平倉多空淨額 口數
        docs[cur_prod][role_key] = net_oi

    result: List[Dict] = []
    for d in docs.values():
        d["retail_net"] = -(d["prop_net"] + d["itf_net"] + d["foreign_net"])
        result.append(d)
    return result


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
    return list(
        COL.find({"product": prod}, {"_id": 0})
        .sort("date", -1)
        .limit(days)
    )


if __name__ == "__main__":
    from pprint import pprint

    pprint(fetch())
