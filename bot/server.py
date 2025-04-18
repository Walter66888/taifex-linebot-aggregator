"""
crawler/fut_contracts.py  v3.3.4
--------------------------------
抓取 https://www.taifex.com.tw/cht/3/futContractsDateExcel
同步寫入 2 商品：小型臺指期貨 (mtx)、微型臺指期貨 (imtx)

★ 重點
1. 逐 <tr> 掃描，鎖定「倒數第 2 個數字欄」= 未平倉多空淨額 (口數)
2. ROLE_MAP / TARGETS 全半形與變體容錯
3. 自動遷移索引：刪除任何「僅含 date 欄位」索引 → 建 (date, product) 複合唯一索引
4. 抓不到三法人齊全 ⇒ neutral exit，不 raise

依賴：beautifulsoup4、lxml、pymongo
"""

from __future__ import annotations
import re
import sys
from datetime import datetime, timezone, timedelta

import bs4 as bs
import requests
from pymongo import ASCENDING, UpdateOne
from utils.db import get_col

# ── 常量設定 ──────────────────────────────────────────────
URL  = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD = {"User-Agent": "Mozilla/5.0 (fut-contracts-crawler/3.3.4)"}

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
NUM_RE  = re.compile(r"^-?\d[\d,]*$")   # 任意千分位整數

# ── Mongo 連線 & 索引保證 ────────────────────────────────
COL = get_col("fut_contracts")

def ensure_index(col):
    """
    • 刪除任何『僅含 date 欄位』索引 (名稱 / unique 與否皆刪)
    • 建立 (date, product) 複合唯一索引
    """
    for name, spec in col.index_information().items():
        if name == "_id_":
            continue
        keys = spec["key"]                # list[tuple] 或 list[list]
        if len(keys) == 1:
            field = keys[0][0] if isinstance(keys[0], (list, tuple)) else list(keys[0].values())[0]
            if field == "date":
                col.drop_index(name)

    if "date_1_product_1" not in col.index_information():
        col.create_index(
            [("date", ASCENDING), ("product", ASCENDING)],
            unique=True,
            name="date_1_product_1",
        )

ensure_index(COL)

# ── 工具函式 ──────────────────────────────────────────────
def today_tw(): 
    return datetime.now(timezone(timedelta(hours=8))).date()

def _extract_net(nums: list[str]) -> int | None:
    """倒數第 2 個數字欄 (口數)；最後 1 個為契約金額。"""
    numeric = [n.replace(",", "") for n in nums if NUM_RE.match(n)]
    if len(numeric) < 2:
        return None
    return int(numeric[-2])

# ── 解析 HTML ────────────────────────────────────────────
def parse(html: str) -> list[dict]:
    soup = bs.BeautifulSoup(html, "lxml")

    # ① 解析日期
    span = soup.find(string=DATE_RE)
    if not span:
        raise ValueError("找不到日期")
    date_dt = datetime.strptime(DATE_RE.search(span).group(1), "%Y/%m/%d").replace(
        tzinfo=timezone.utc
    )

    # ② 初始化容器
    results = {v: {"date": date_dt, "product": v} for v in TARGETS.values()}
    current_prod: str | None = None

    # ③ 逐列掃描
    for tr in soup.select("tbody tr"):
        raw   = [td.get_text(strip=True) for td in tr.find_all("td")]
        cells = [c.replace(",", "").replace("口", "") for c in raw]
        if not cells:
            continue

        # → 商品名稱
        for zh, code in TARGETS.items():
            if zh in cells:
                current_prod = code
                break
        if current_prod is None:
            continue

        # → 身份別
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

    # ④ 彙整
    docs = []
    for doc in results.values():
        if all(k in doc for k in ("prop_net", "itf_net", "foreign_net")):
            doc["retail_net"] = -(doc["prop_net"] + doc["itf_net"] + doc["foreign_net"])
            docs.append(doc)
    return docs

# ── 抓取 & 寫入 ───────────────────────────────────────────
def fetch(upsert: bool = True):
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
                {"date": doc["date"].replace(tzinfo=None), "product": doc["product"]},
                {"$set": {**doc, "date": doc["date"].replace(tzinfo=None)}},
                upsert=True,
            )
            for doc in docs
        ]
        COL.bulk_write(ops, ordered=False)
    print(f"更新 {len(docs)} 商品 fut_contracts → MongoDB")
    return docs

# ── 快速查詢 ─────────────────────────────────────────────
def latest(product="mtx", days: int = 1):
    return list(
        COL.find({"product": product}, {"_id": 0})
           .sort("date", -1)
           .limit(days)
    )

# ── CLI 用途 ─────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        prod = sys.argv[2] if len(sys.argv) > 2 else "mtx"
        print(latest(prod, 3))
