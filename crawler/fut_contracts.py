# -*- coding: utf-8 -*-
"""
crawler.fut_contracts
抓「三大法人－區分各期貨契約」的小台(小型臺指期貨, mtx)與微台(微型臺指期貨, imtx)
只用到『未平倉餘額 → 多空淨額 → 口數』欄位。
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from pymongo import UpdateOne

from utils.db import get_col

# ────────────────────────────────────────────────────────────────────────
URL = "https://www.taifex.com.tw/cht/3/futContractsDate"
COL = get_col("fut_contracts")
# 以 (date, product) 做唯一索引，第一次建立即可；已存在時會自動忽略
COL.create_index([("date", 1), ("product", 1)], unique=True)

PRODUCT_MAP = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}

_INT_RE = re.compile(r"-?\d+")


def _to_int(s: str) -> int:
    """條件允許的話把字串轉成 int；否則丟 ValueError"""
    return int(s.replace(",", ""))


# ────────────────────────────────────────────────────────────────────────
def parse(html: str) -> List[Dict]:
    """
    解析頁面文字，回傳 docs list
    每個 doc:
        {
          date, product, prop_net, itf_net, foreign_net, retail_net
        }
    """
    soup = BeautifulSoup(html, "html.parser")

    # 取日期（頁面右側會有『日期YYYY/MM/DD』）
    date_span = soup.find("span", class_="right")
    if not date_span or "/" not in date_span.text:
        raise RuntimeError("無法判讀日期欄位")
    dt = (
        datetime.strptime(date_span.text.strip().replace("日期", ""), "%Y/%m/%d")
        .replace(tzinfo=timezone.utc)
    )

    docs: List[Dict] = []

    for tr in soup.find_all("tr", class_="12bk"):
        tds = tr.find_all("td")
        if len(tds) < 15:  # 保險檢查
            continue

        product_name = tds[1].get_text(strip=True)
        if product_name not in PRODUCT_MAP:
            continue  # 我們只要小台 / 微台

        role = tds[2].get_text(strip=True)  # 自營商 / 投信 / 外資

        # 把該列全部數字抓出來
        nums: List[int] = []
        for td in tds[3:]:
            txt = td.get_text(strip=True).replace(",", "")
            if _INT_RE.fullmatch(txt):
                nums.append(int(txt))
        if len(nums) < 11:
            # 沒抓到「未平倉淨額口數」→ 跳過
            continue
        net_oi = nums[10]  # index 10 = 未平倉餘額 → 多空淨額 → 口數

        code = PRODUCT_MAP[product_name]
        doc = next((d for d in docs if d["product"] == code), None)
        if doc is None:
            doc = {"product": code}
            docs.append(doc)

        if role == "自營商":
            doc["prop_net"] = net_oi
        elif role == "投信":
            doc["itf_net"] = net_oi
        elif role == "外資":
            doc["foreign_net"] = net_oi

    # 填入日期、算散戶
    for d in docs:
        d["date"] = dt
        d.setdefault("prop_net", 0)
        d.setdefault("itf_net", 0)
        d.setdefault("foreign_net", 0)
        d["retail_net"] = -(d["prop_net"] + d["itf_net"] + d["foreign_net"])

    return docs


# ────────────────────────────────────────────────────────────────────────
def fetch() -> List[Dict]:
    """抓取並寫入 DB，回傳本次抓取的 docs"""
    res = requests.get(URL, timeout=20)
    res.encoding = "utf-8"
    docs = parse(res.text)

    if docs:
        ops = [
            UpdateOne(
                {"date": d["date"], "product": d["product"]},
                {"$set": d},
                upsert=True,
            )
            for d in docs
        ]
        COL.bulk_write(ops, ordered=False)

    return docs


def latest(limit: int = 10) -> List[Dict]:
    """取最新 n 筆（預設 10）"""
    return list(COL.find().sort([("date", -1), ("product", 1)]).limit(limit))


# ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from pprint import pprint

    pprint(fetch())
