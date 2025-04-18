"""
crawler/fut_contracts.py  v3.3
------------------------------
抓取 https://www.taifex.com.tw/cht/3/futContractsDateExcel
輸出 2 商品：小型臺指期貨(mtx)、微型臺指期貨(imtx)

✦ 核心改進
1. 角色、商品名稱容錯（外資及陸資、全/半形）
2. **穩定鎖定「倒數第 2 個數字欄」** → 一定是「未平倉多空淨額 ‑ 口數」，不受金額大小影響
3. 抓不到完整三法人 ⇒ neutral exit，不 raise
4. MongoDB 複合索引 (date, product) 保證唯一
"""

from __future__ import annotations
import re, sys, requests, bs4 as bs
from datetime import datetime, timezone, timedelta
from pymongo import UpdateOne, ASCENDING
from utils.db import get_col

URL   = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD  = {"User-Agent": "Mozilla/5.0 (fut-contracts-crawler/3.3)"}

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
NUM_RE  = re.compile(r"^-?\d[\d,]*$")      # 任意千分位整數

COL = get_col("fut_contracts")
if "date_1_product_1" not in COL.index_information():
    COL.create_index([("date", ASCENDING), ("product", ASCENDING)], unique=True)

def today_tw(): 
    return datetime.now(timezone(timedelta(hours=8))).date()

# ── 工具：從數字欄陣列取「倒數第 2 格」──────────────────────
def _extract_net(nums: list[str]) -> int | None:
    numeric = [n.replace(",", "") for n in nums if NUM_RE.match(n)]
    if len(numeric) < 2:
        return None
    return int(numeric[-2])   # 倒數第 2 個即口數欄

# ── 主要解析 ────────────────────────────────────────────────
def parse(html: str) -> list[dict]:
    soup = bs.BeautifulSoup(html, "lxml")

    # 1️⃣ 日期
    span = soup.find(string=DATE_RE)
    if not span:
        raise ValueError("找不到日期")
    date_dt = datetime.strptime(DATE_RE.search(span).group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    results = {v: {"date": date_dt, "product": v} for v in TARGETS.values()}
    current_prod: str | None = None

    # 2️⃣ 逐 <tr> 掃描
    for tr in soup.select("tbody tr"):
        raw = [td.get_text(strip=True) for td in tr.find_all("td")]
        cells = [c.replace(",", "").replace("口", "") for c in raw]
        if not cells:
            continue

        # 更新商品名稱
        for zh, code in TARGETS.items():
            if zh in cells:
                current_prod = code
                break
        if current_prod is None:
            continue

        # 身份別定位
        role = None
        if len(cells) >= 3 and cells[1] in TARGETS and cells[2] in ROLE_MAP:
            role = cells[2]; nums = cells[3:]
        elif cells[0] in ROLE_MAP:
            role = cells[0]; nums = cells[1:]
        if role not in ROLE_MAP:
            continue

        net_val = _extract_net(nums)
        if net_val is None:
            continue

        results[current_prod][ROLE_MAP[role]] = net_val

    # 3️⃣ 彙整
    docs = []
    for d in results.values():
        if all(k in d for k in ("prop_net", "itf_net", "foreign_net")):
            d["retail_net"] = -(d["prop_net"] + d["itf_net"] + d["foreign_net"])
            docs.append(d)
    return docs

# ── 抓取 & 寫入 ─────────────────────────────────────────────
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
            )
            for d in docs
        ]
        COL.bulk_write(ops, ordered=False)
    print(f"更新 {len(docs)} 商品 fut_contracts → MongoDB")
    return docs

def latest(product="mtx", days=1):
    return list(COL.find({"product": product}, {"_id": 0}).sort("date", -1).limit(days))

# ── CLI 測試 ───────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        prod = sys.argv[2] if len(sys.argv) > 2 else "mtx"
        print(latest(prod, 3))
