"""
crawler/fut_contracts.py  v3.4  (stable)
----------------------------------------
抓取 https://www.taifex.com.tw/cht/3/futContractsDateExcel
寫入：小台 (mtx)‧微台 (imtx)   retail_net = -(prop+itf+foreign)
"""

from __future__ import annotations
import re, sys
from datetime import datetime, timezone, timedelta

import bs4 as bs
import requests
from pymongo import ASCENDING, UpdateOne
from utils.db import get_col

# ── 常量 ────────────────────────────────────────────────
URL  = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD = {"User-Agent": "taifex-fut-crawler/3.4"}

TARGETS = {
    "小型臺指期貨": "mtx",
    "小型台指期貨": "mtx",
    "微型臺指期貨": "imtx",
    "微型台指期貨": "imtx",
}
ROLE_MAP = {
    "自營商":         "prop_net",
    "自營商(避險)":    "prop_net",
    "投信":           "itf_net",
    "外資":           "foreign_net",
    "外資及陸資":     "foreign_net",
}

DATE_RE = re.compile(r"日期\s*(\d{4}/\d{1,2}/\d{1,2})")
NUM_RE  = re.compile(r"^-?\d[\d,]*$")

# ── Mongo ────────────────────────────────────────────
COL = get_col("fut_contracts")

def ensure_index(col):
    """移除 legacy `date` 唯一索引 → 建 (date,product) 複合唯一索引"""
    for name, spec in col.index_information().items():
        if name == "_id_":
            continue
        if spec.get("unique") and spec["key"] == [("date", 1)]:
            col.drop_index(name)
    if "date_1_product_1" not in col.index_information():
        col.create_index(
            [("date", ASCENDING), ("product", ASCENDING)],
            unique=True,
            name="date_1_product_1",
        )
ensure_index(COL)

# ── 工具 ────────────────────────────────────────────────
def today_tw():
    return datetime.now(timezone(timedelta(hours=8))).date()

def _extract_net(nums: list[str]) -> int | None:
    arr = [n.replace(",", "") for n in nums if NUM_RE.match(n)]
    return int(arr[-2]) if len(arr) >= 2 else None   # 倒數第2欄=口數

# ── 解析 HTML ──────────────────────────────────────────
def parse(html: str):
    soup = bs.BeautifulSoup(html, "lxml")
    span = soup.find(string=DATE_RE)
    if not span:
        raise ValueError("找不到日期字串")
    date_dt = datetime.strptime(DATE_RE.search(span).group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    results = {v: {"date": date_dt, "product": v} for v in TARGETS.values()}
    current_prod = None

    for tr in soup.select("tbody tr"):
        cells = [td.get_text(strip=True).replace(",", "").replace("口", "") for td in tr.find_all("td")]
        if not cells:
            continue

        for zh, code in TARGETS.items():
            if zh in cells:
                current_prod = code
                break
        if current_prod is None:
            continue

        if len(cells) >= 3 and cells[1] in TARGETS and cells[2] in ROLE_MAP:
            role, nums = cells[2], cells[3:]
        elif cells[0] in ROLE_MAP:
            role, nums = cells[0], cells[1:]
        else:
            continue

        net = _extract_net(nums)
        if net is not None:
            results[current_prod][ROLE_MAP[role]] = net

    docs = []
    for d in results.values():
        if all(k in d for k in ("prop_net", "itf_net", "foreign_net")):
            d["retail_net"] = -(d["prop_net"] + d["itf_net"] + d["foreign_net"])
            docs.append(d)
    return docs

# ── 抓取 ───────────────────────────────────────────────
def fetch(upsert=True):
    res = requests.get(URL, headers=HEAD, timeout=30)
    res.encoding = res.apparent_encoding or "utf-8"
    docs = parse(res.text)

    if not docs or docs[0]["date"].date() < today_tw():
        print("[WARN] fut_contracts not updated"); sys.exit(75)

    if upsert:
        ops = [
            UpdateOne(
                {"date": d["date"].replace(tzinfo=None), "product": d["product"]},
                {"$set": {**d, "date": d["date"].replace(tzinfo=None)}},
                upsert=True,
            ) for d in docs
        ]
        COL.bulk_write(ops, ordered=False)
    print(f"更新 {len(docs)} 商品 fut_contracts → MongoDB")
    return docs

# ── 查詢 ───────────────────────────────────────────────
def latest(product="mtx", days=1):
    return list(COL.find({"product": product}, {"_id": 0}).sort("date", -1).limit(days))

# ── CLI ───────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "show":
        print(latest(sys.argv[2] if len(sys.argv) > 2 else "mtx", 3))
    else:
        fetch()
