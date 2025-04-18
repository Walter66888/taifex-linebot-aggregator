# -*- coding: utf-8 -*-
"""
三大法人 – 各期貨契約   (小台 MTX、微台 IMTX)
抓取「未平倉多空淨額 ‧ 口數」並存進 MongoDB
"""
from __future__ import annotations

import re
import logging
from datetime import datetime, timezone

import pytz
import requests
from bs4 import BeautifulSoup
from pymongo import ASCENDING, UpdateOne

from utils.db import get_col           # 你自己的封裝
from utils.time import taipei_tz       # → 若沒有就改用 pytz.timezone("Asia/Taipei")

LOGGER = logging.getLogger(__name__)

URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"

#: 把中文商品名稱對應到資料庫 product code
PROD_MAP = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}

COL = get_col("fut_contracts")
# 日期 + 商品 雙欄唯一鍵，避免重複
COL.create_index([("date", ASCENDING), ("product", ASCENDING)], unique=True)


# ---------- 解析 HTML ----------------------------------------------------- #
def _clean_int(text: str) -> int:
    """把「-3,048 」→ -3048"""
    text = text.replace(",", "").strip()
    return int(text or 0)


def _parse_rows(rows: list[list[str]]) -> dict[str, int]:
    """rows = 3x 行（自營商 / 投信 / 外資）"""
    # 「未平倉多空淨額 ‧ 口數」在 <td> 的第 14 欄 (= index 13)
    prop_net = _clean_int(rows[0][13])
    itf_net = _clean_int(rows[1][13])
    foreign_net = _clean_int(rows[2][13])
    retail_net = -(prop_net + itf_net + foreign_net)
    return {
        "prop_net": prop_net,
        "itf_net": itf_net,
        "foreign_net": foreign_net,
        "retail_net": retail_net,
    }


def parse(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")

    # 解析「日期 yyyy/mm/dd」
    m = re.search(r"日期\s*(\d{4})/(\d{2})/(\d{2})", html)
    if not m:
        raise RuntimeError("找不到日期")
    date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                    tzinfo=timezone.utc)

    docs: list[dict] = []

    trs = soup.select("tbody tr.12bk")
    i = 0
    while i < len(trs):
        td_texts = [td.get_text(" ", strip=True) for td in trs[i].find_all("td")]
        if len(td_texts) < 4:        # 空行、表頭等
            i += 1
            continue

        # 只在「自營商」(identity) 這一行讀出商品名稱
        identity = td_texts[2]
        if identity != "自營商":
            i += 1
            continue

        prod_name = td_texts[1]
        prod_code = PROD_MAP.get(prod_name)
        if not prod_code:            # 只抓小台 / 微台
            i += 3                   # 跳過整個 3-row group
            continue

        rows_3 = []
        for j in range(3):
            rows_3.append([td.get_text(" ", strip=True)
                           for td in trs[i + j].find_all("td")])
        nets = _parse_rows(rows_3)

        docs.append({
            "date": date,
            "product": prod_code,
            **nets,
        })
        i += 3                        # 下一商品
    return docs


# ---------- 對外介面 (fetch / latest) ------------------------------------ #
def fetch() -> list[dict]:
    """抓官網 → 寫 MongoDB → 回傳 list[dict]"""
    resp = requests.get(URL, timeout=20)
    resp.encoding = "utf-8"          # 官網是 UTF‑8
    docs = parse(resp.text)

    # bulk upsert
    ops = []
    for d in docs:
        ops.append(UpdateOne(
            {"date": d["date"], "product": d["product"]},
            {"$set": d},
            upsert=True
        ))
    if ops:
        COL.bulk_write(ops, ordered=False)
    LOGGER.info("[fut_contracts] upsert %s documents", len(ops))
    return docs


def latest(product: str | None = None, limit: int = 1) -> list[dict] | dict | None:
    """取最新資料；product=None → 全部商品最新各 1 筆"""
    if product:
        cur = COL.find({"product": product}).sort("date", -1).limit(limit)
        docs = list(cur)
        return docs[0] if limit == 1 else docs

    # 取每個商品最新 1 筆
    pipeline = [
        {"$sort": {"date": -1}},
        {"$group": {
            "_id": "$product",
            "doc": {"$first": "$$ROOT"}
        }},
        {"$replaceRoot": {"newRoot": "$doc"}}
    ]
    return list(COL.aggregate(pipeline))


# ---------- module CLI --------------------------------------------------- #
if __name__ == "__main__":           # 手動測試
    from pprint import pprint
    pprint(fetch())
