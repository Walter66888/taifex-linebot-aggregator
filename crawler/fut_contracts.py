"""
crawler/fut_contracts.py  v2.0
------------------------------
穩定解析 https://www.taifex.com.tw/cht/3/futContractsDateExcel
  • 完全用 BeautifulSoup + regex 直接掃 <tr>
  • 自營商 / 投信 / 外資 三行，取「未平倉多空淨額 (口數)」= 最後一個 <td>
  • 計算散戶小台未平倉 retail_net
"""

from __future__ import annotations
import re, sys, requests, bs4 as bs
from datetime import datetime, timezone, timedelta
from pymongo import UpdateOne
from utils.db import get_col

URL   = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD  = {"User-Agent": "Mozilla/5.0 (fut-contracts-crawler/2.0)"}
COL   = get_col("fut_contracts")

TARGET = "小型臺指期貨"
ROLE_MAP = {"自營商": "prop_net", "投信": "itf_net", "外資": "foreign_net"}
DATE_RE = re.compile(r"日期\s*(\d{4}/\d{1,2}/\d{1,2})")

def today_tw(): return datetime.now(timezone(timedelta(hours=8))).date()

def parse(html: str) -> dict:
    soup = bs.BeautifulSoup(html, "lxml")

    # ① 日期：在 <span class="right">日期YYYY/MM/DD</span>
    span_date = soup.find(string=DATE_RE)
    if not span_date:
        raise ValueError("找不到日期")
    date_str = DATE_RE.search(span_date).group(1)
    date_dt  = datetime.strptime(date_str, "%Y/%m/%d").replace(tzinfo=timezone.utc)

    # ② 掃所有 <tr>，找商品名稱=小型臺指期貨
    data = {}
    for tr in soup.select("tr"):
        tds = [td.get_text(strip=True).replace(",", "") for td in tr.find_all("td")]
        if len(tds) < 3 or tds[1] != TARGET:   # [序號] [商品名稱] [身份別] ...
            continue
        role = tds[2]
        if role not in ROLE_MAP:
            continue
        # 官方最後一欄就是未平倉多空淨額 (口數)
        try:
            net = int(tds[-2 if tds[-1] == '' else -1].replace(",", ""))
        except ValueError:
            continue
        data[ROLE_MAP[role]] = net

    if len(data) != 3:
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

    # Upsert
    doc["date"] = doc["date"].replace(tzinfo=None)  # naive UTC
    COL.update_one({"date": doc["date"]}, {"$set": doc}, upsert=True)
    print("更新 fut_contracts → MongoDB")

def latest(n=1):
    return list(COL.find({}, {"_id":0}).sort("date",-1).limit(n))

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv)>1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        print(latest(int(sys.argv[2]) if len(sys.argv)>2 else 1))
