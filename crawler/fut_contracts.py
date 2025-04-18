"""
crawler/fut_contracts.py  v3.0
------------------------------
解析 https://www.taifex.com.tw/cht/3/futContractsDateExcel

◎ 支援兩檔商品：
   ‑ 小型臺指期貨  →  product = "mtx"
   ‑ 微型臺指期貨  →  product = "imtx"

◎ 取值邏輯
   1. 逐 <tr> 掃描；透過 rowspan 記憶目前商品名稱
   2. 只抓「未平倉多空淨額 ‑ 口數」欄（倒數第 2 個 <td>，金額欄在最後一格）
   3. 三法人 (自營商/投信/外資) 口數齊全才寫入
   4. 散戶未平倉 = −(prop + itf + foreign)

◎ 寫入集合：fut_contracts
   doc = {
     date        : datetime (UTC, naive),
     product     : "mtx" | "imtx",
     prop_net    : int,
     itf_net     : int,
     foreign_net : int,
     retail_net  : int
   }
   唯一索引 (date, product)

依賴：bs4==4.12.*, lxml, pymongo
"""

from __future__ import annotations
import re, sys, requests, bs4 as bs
from datetime import datetime, timezone, timedelta
from pymongo import UpdateOne, ASCENDING
from utils.db import get_col

URL   = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD  = {"User-Agent": "Mozilla/5.0 (fut-contracts-crawler/3.0)"}

TARGETS = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}
ROLE_MAP = {"自營商": "prop_net", "投信": "itf_net", "外資": "foreign_net"}

DATE_RE  = re.compile(r"日期\s*(\d{4}/\d{1,2}/\d{1,2})")
NUM_RE   = re.compile(r"^-?\d{1,3}(?:,\d{3})*$")          # 1~6 位千分位，排除金額欄

COL = get_col("fut_contracts")
# 建立複合索引
if "date_1_product_1" not in COL.index_information():
    COL.create_index([("date", ASCENDING), ("product", ASCENDING)], unique=True)

def today_tw(): 
    return datetime.now(timezone(timedelta(hours=8))).date()

# ── HTML 解析 ───────────────────────────────────────────────────
def parse(html: str) -> list[dict]:
    soup = bs.BeautifulSoup(html, "lxml")

    # 1️⃣ 取日期
    span = soup.find(string=DATE_RE)
    if not span:
        raise ValueError("找不到日期字串")
    date_dt = datetime.strptime(DATE_RE.search(span).group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    # 2️⃣ 逐列掃描
    results: dict[str, dict] = {v: {"date": date_dt, "product": v} for v in TARGETS.values()}
    current_prod_key: str | None = None

    for tr in soup.select("tbody tr"):
        cells = [td.get_text(strip=True).replace(",", "") for td in tr.find_all("td")]
        if not cells:
            continue

        # 若該列含任何 target 名稱 -> 更新 current_prod_key
        for zh_name, code in TARGETS.items():
            if zh_name in cells:
                current_prod_key = code
                break

        if current_prod_key is None:
            continue  # 還未遇到目標商品

        # 判定身份別與數字欄
        role = None
        if len(cells) >= 3 and cells[1] in TARGETS and cells[2] in ROLE_MAP:
            role = cells[2]
            nums = cells[3:]
        elif cells[0] in ROLE_MAP:
            role = cells[0]
            nums = cells[1:]
        if role not in ROLE_MAP:
            continue

        # 從後往前找第一個符合 NUM_RE 的口數欄
        net_val = None
        for c in reversed(nums):
            if NUM_RE.match(c):
                net_val = int(c.replace(",", ""))
                break
        if net_val is None:
            continue

        results[current_prod_key][ROLE_MAP[role]] = net_val

    # 3️⃣ 完成三法人欄位者才回傳
    docs = []
    for code, doc in results.items():
        if all(k in doc for k in ("prop_net", "itf_net", "foreign_net")):
            doc["retail_net"] = -(doc["prop_net"] + doc["itf_net"] + doc["foreign_net"])
            docs.append(doc)

    if not docs:
        raise ValueError("未取得任何完整商品資料")
    return docs

# ── 主流程 ────────────────────────────────────────────────────
def fetch(upsert: bool = True):
    res = requests.get(URL, headers=HEAD, timeout=30)
    res.encoding = res.apparent_encoding or "utf-8"
    docs = parse(res.text)

    if docs[0]["date"].date() < today_tw():
        print("尚未更新，Neutral Exit"); sys.exit(75)

    if upsert:
        ops = []
        for d in docs:
            d["date"] = d["date"].replace(tzinfo=None)
            ops.append(UpdateOne({"date": d["date"], "product": d["product"]}, {"$set": d}, upsert=True))
        if ops:
            COL.bulk_write(ops, ordered=False)
    print(f"更新 {len(docs)} 商品 fut_contracts → MongoDB")
    return docs

def latest(product: str = "mtx", days: int = 1):
    return list(
        COL.find({"product": product}, {"_id": 0})
           .sort("date", -1)
           .limit(days)
    )

# ── CLI 測試 ──────────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        prod = sys.argv[2] if len(sys.argv) > 2 else "mtx"
        print(latest(prod, 3))
