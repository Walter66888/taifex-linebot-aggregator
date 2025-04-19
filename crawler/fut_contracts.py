# -*- coding: utf-8 -*-
# crawler/fut_contracts.py  v4.3  2025‑04‑19
"""
抓『三大法人‑區分各期貨契約』中
  ‑ 小型臺指期貨 (mtx)
  ‑ 微型臺指期貨 (imtx)

一共會得到 2(商品) × 3(身份別) ＝ 6 筆／天

document 結構
{
    date        : UTC datetime
    product     : "mtx" | "imtx"
    type        : "自營商" | "投信" | "外資"
    net_value   : int           # 未平倉多空淨額口數
    raw_data    : {...}         # 原始 15 欄字串，方便日後 debug
}

使用：
    python -m crawler.fut_contracts run            # 平日自動跳過假日
    python -m crawler.fut_contracts run --force    # 假日強制抓
"""
from __future__ import annotations

import argparse, logging, re, sys
from datetime import datetime, timezone
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from pymongo import UpdateOne, ASCENDING

from utils.db import get_col

LOG      = logging.getLogger(__name__)
URL      = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEADERS  = {"User-Agent": "Mozilla/5.0"}

TARGETS = {           # 中文 → 代號
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}

COL = get_col("fut_contracts")
# date + product + type 必須唯一
COL.create_index([("date", 1), ("product", 1), ("type", 1)], unique=True)

# ───────────────────────── utility ──────────────────────────
def _clean_int(txt: str) -> int:
    """去掉逗號、空白，把空字串視為 0"""
    return int(re.sub(r"[^\d\-]", "", txt) or "0")


def _row_net(tds) -> int:
    """
    取「未平倉多空淨額‑口數」欄位。
    可能有 15 / 14 / 13 欄，對應 index 13 / 12 / 11
    """
    n = len(tds)
    if n >= 14:
        idx = 13
    elif n == 13:
        idx = 11
    else:
        raise ValueError(f"unsupported column length {n}")
    return _clean_int(tds[idx].get_text())


# ───────────────────────── parser ───────────────────────────
def parse(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")

    # 解析日期
    m = re.search(r"日期(\d{4}/\d{2}/\d{2})", html)
    if not m:
        raise RuntimeError("找不到日期")
    date_obj = datetime.strptime(m.group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    trs = soup.find_all("tr", class_="12bk")
    if not trs:
        raise RuntimeError("找不到 tr.12bk")

    docs: List[Dict] = []
    current_product_name = None

    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        # column[1] 有值代表新商品開始
        prod_cn = tds[1].get_text(strip=True)
        if prod_cn:
            current_product_name = prod_cn

        if current_product_name not in TARGETS:
            continue           # 只要 mtx / imtx

        idf = tds[2].get_text(strip=True)       # 自營商 / 投信 / 外資
        try:
            net = _row_net(tds)
        except ValueError as e:
            LOG.debug("skip %s", e)
            continue

        docs.append({
            "date": date_obj,
            "product": TARGETS[current_product_name],
            "type": idf,
            "net_value": net,
            "raw_data": {"column_data": [td.get_text(strip=True) for td in tds]},
        })

    return docs


# ────────────────────── fetch & helpers ─────────────────────
def _is_weekend() -> bool:
    return datetime.now().weekday() >= 5      # 5,6 = Sat, Sun


def fetch(force: bool = False) -> List[Dict]:
    if _is_weekend() and not force:
        raise RuntimeError("週末不抓 (加 --force 可強制)")

    res = requests.get(URL, headers=HEADERS, timeout=20)
    res.raise_for_status()

    docs = parse(res.text)
    if not docs:
        raise RuntimeError("empty docs")

    ops = [
        UpdateOne(
            {"date": d["date"], "product": d["product"], "type": d["type"]},
            {"$set": d},
            upsert=True,
        )
        for d in docs
    ]
    COL.bulk_write(ops, ordered=False)
    LOG.info("upsert %d docs OK", len(docs))
    return docs


def latest(product: str | None = None, idf: str | None = None) -> Dict | None:
    """
    取最後一筆。可加 product（mtx/imtx）或 idf（自營商/投信/外資）過濾。
    """
    q = {}
    if product:
        q["product"] = product
    if idf:
        q["type"] = idf
    return COL.find_one(q, {"_id": 0}, sort=[("date", -1)])


# ────────────────────────── CLI ─────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(message)s",
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--force", action="store_true", help="ignore weekend guard")
    args = ap.parse_args()

    if args.cmd == "run":
        try:
            from pprint import pprint
            pprint(fetch(args.force))
        except Exception as e:
            LOG.error("crawler error: %s", e)
            sys.exit(1)
