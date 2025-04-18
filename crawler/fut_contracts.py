"""
crawler/fut_contracts.py  v2.1
------------------------------
小型臺指期貨 (MTX) 三大法人未平倉淨額解析 — 最強容錯
  • BeautifulSoup 逐 <tr> 掃描，可識別 rowspan 結構
  • 記憶當前商品名稱；若後續列缺名稱，沿用上一列
  • 從每列最後一個「純數字 / 帶負號」欄位取未平倉多空淨額 (口數)
"""

from __future__ import annotations
import re, sys, requests, bs4 as bs
from datetime import datetime, timezone, timedelta
from pymongo import UpdateOne
from utils.db import get_col

URL   = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD  = {"User-Agent": "Mozilla/5.0 (fut-contracts-crawler/2.1)"}
COL   = get_col("fut_contracts")

TARGET   = "小型臺指期貨"
ROLE_MAP = {"自營商": "prop_net", "投信": "itf_net", "外資": "foreign_net"}
DATE_RE  = re.compile(r"日期\s*(\d{4}/\d{1,2}/\d{1,2})")
NUM_RE   = re.compile(r"^-?\d[\d,]*$")

def today_tw(): return datetime.now(timezone(timedelta(hours=8))).date()

def parse(html:str) -> dict:
    soup = bs.BeautifulSoup(html, "lxml")

    # 1️⃣ 日期
    span = soup.find(string=DATE_RE)
    if not span:
        raise ValueError("找不到日期")
    date_dt = datetime.strptime(DATE_RE.search(span).group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    # 2️⃣ 掃 <tr>
    data, current_prod = {}, None
    for tr in soup.select("tbody tr"):
        tds = [td.get_text(strip=True).replace(",", "") for td in tr.find_all("td")]
        if not tds:
            continue

        # 判斷是否帶有商品名稱
        if TARGET in tds:
            current_prod = TARGET
        if current_prod != TARGET:
            continue  # 只處理小台行

        # 抓身份別位置 (帶商品名→ role 在 tds[2]; 無商品名→ role 在 tds[0])
        role = None
        if len(tds) >= 3 and tds[1] == TARGET and tds[2] in ROLE_MAP:
            role = tds[2]
            nums = tds[3:]
        elif tds[0] in ROLE_MAP:
            role = tds[0]
            nums = tds[1:]
        if role is None:
            continue

        # 從尾端找第一個數字欄位
        for cell in reversed(nums):
            if NUM_RE.match(cell):
                net = int(cell.replace(",", ""))
                break
        else:
            continue

        data[ROLE_MAP[role]] = net
        if len(data) == 3:
            break

    if len(data) < 3:
        raise ValueError("三法人資料不齊全")

    data["date"] = date_dt
    data["retail_net"] = -(data["prop_net"] + data["itf_net"] + data["foreign_net"])
    return data

# ── 主流程 ────────────────────────────────────────────────
def fetch(upsert=True):
    res = requests.get(URL, headers=HEAD, timeout=30)
    res.encoding = res.apparent_encoding or "utf-8"

    try:
        doc = parse(res.text)
    except ValueError as e:
        print("[WARN]", e, "Neutral Exit"); sys.exit(75)

    if doc["date"].date() < today_tw():
        print("尚未更新，Neutral Exit"); sys.exit(75)

    doc["date"] = doc["date"].replace(tzinfo=None)
    COL.update_one({"date": doc["date"]}, {"$set": doc}, upsert=True)
    print("更新 fut_contracts → MongoDB")

def latest(n=1):
    return list(COL.find({}, {"_id": 0}).sort("date", -1).limit(n))

# ── CLI ──────────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        print(latest(int(sys.argv[2]) if len(sys.argv) > 2 else 1))
