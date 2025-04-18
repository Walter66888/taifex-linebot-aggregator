"""
crawler/fut_contracts.py  v3.3.6
--------------------------------
抓取 https://www.taifex.com.tw/cht/3/futContractsDateExcel
同步寫入 2 商品：小型臺指期貨 (mtx)、微型臺指期貨 (imtx)

最後修正：
• ensure_index() → *鐵血版*：凡是 **唯一鍵集合只含 'date'** 的索引全部刪除  
  ‑ 無論 PyMongo 版本、keys 型別(list[tuple]/list[list]/list[dict])、名稱、複本  
• 其餘邏輯保持 v3.3.5
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
HEAD = {"User-Agent": "Mozilla/5.0 (fut-contracts-crawler/3.3.6)"}

TARGETS = {
    "小型臺指期貨": "mtx", "小型台指期貨": "mtx",
    "微型臺指期貨": "imtx","微型台指期貨": "imtx",
}
ROLE_MAP = {
    "自營商": "prop_net", "自營商(避險)": "prop_net",
    "投信": "itf_net",
    "外資": "foreign_net", "外資及陸資": "foreign_net",
}

DATE_RE = re.compile(r"日期\s*(\d{4}/\d{1,2}/\d{1,2})")
NUM_RE  = re.compile(r"^-?\d[\d,]*$")

# ── Mongo ──────────────────────────────────────────────
COL = get_col("fut_contracts")

def _field_from_keyitem(item):
    """item 可為 tuple/list/dict，統一取欄位名"""
    if isinstance(item, (list, tuple)):
        return item[0]
    if isinstance(item, dict):
        # PyMongo 4.x: {'date': 1}
        return next(iter(item))
    return str(item)

def ensure_index(col):
    """刪除任何 *僅含 date* 的索引 -> 建複合唯一 (date,product)"""
    for name, spec in list(col.index_information().items()):
        if name == "_id_":
            continue
        fields = {_field_from_keyitem(k) for k in spec["key"]}
        if fields == {"date"}:
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

def _extract_net(nums):
    numeric = [n.replace(",", "") for n in nums if NUM_RE.match(n)]
    return int(numeric[-2]) if len(numeric) >= 2 else None

# ── 解析 ────────────────────────────────────────────────
def parse(html: str):
    soup = bs.BeautifulSoup(html, "lxml")
    span = soup.find(string=DATE_RE)
    if not span:
        raise ValueError("找不到日期")
    date_dt = datetime.strptime(DATE_RE.search(span).group(1), "%Y/%m/%d").replace(
        tzinfo=timezone.utc
    )

    results = {v: {"date": date_dt, "product": v} for v in TARGETS.values()}
    current_prod = None

    for tr in soup.select("tbody tr"):
        raw   = [td.get_text(strip=True) for td in tr.find_all("td")]
        cells = [c.replace(",", "").replace("口", "") for c in raw]
        if not cells:
            continue

        for zh, code in TARGETS.items():
            if zh in cells:
                current_prod = code; break
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

# ── 抓取 ────────────────────────────────────────────────
def fetch(upsert=True):
    res = requests.get(URL, headers=HEAD, timeout=30)
    res.encoding = res.apparent_encoding or "utf-8"
    docs = parse(res.text)

    if not docs:
        print("[WARN] HTML 缺 MTX/IMTX 完整資料，Neutral Exit"); sys.exit(75)
    if docs[0]["date"].date() < today_tw():
        print("尚未更新，Neutral Exit"); sys.exit(75)

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

# ── 查詢 ────────────────────────────────────────────────
def latest(product="mtx", days=1):
    return list(
        COL.find({"product": product}, {"_id": 0})
           .sort("date", -1)
           .limit(days)
    )

# ── CLI ────────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        prod = sys.argv[2] if len(sys.argv) > 2 else "mtx"
        print(latest(prod, 3))
