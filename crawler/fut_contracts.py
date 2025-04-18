"""
crawler/fut_contracts.py  v1.4
------------------------------
抓取 https://www.taifex.com.tw/cht/3/futContractsDateExcel
輸出：date, prop_net, itf_net, foreign_net, retail_net
"""

from __future__ import annotations
import io, sys, re, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from pymongo import UpdateOne
from utils.db import get_col

URL    = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD   = {"User-Agent": "Mozilla/5.0 (fut-contracts-crawler/1.4)"}
TARGET = "小型臺指期貨"
COL    = get_col("fut_contracts")

DATE_RE = re.compile(r"20\\d{2}/\\d{1,2}/\\d{1,2}")

ROLE_MAP = {
    "自營商": "prop_net",
    "投信":   "itf_net",
    "外資":   "foreign_net",  # 官方欄為「外資及陸資」
}

def today_tw(): return datetime.now(timezone(timedelta(hours=8))).date()

# ── 抽取單一 HTML 表格 ─────────────────────────────────────────
def _extract_tbl(tbl: pd.DataFrame) -> dict | None:
    # 先確認有日期欄
    date_cell = [c for c in tbl.columns if DATE_RE.match(str(c))]
    if date_cell:
        tbl.columns = range(tbl.shape[1])  # 重編簡單索引
    else:
        # 嘗試 header=2 解析
        tbl = pd.read_html(io.StringIO(tbl.to_html()), header=2, thousands=",")[0]

    # 期貨商品名稱欄通常在 index 1
    if not (tbl == TARGET).any().any():
        return None

    # 找出所有含「口數」欄；最後一個就是「未平倉多空淨額 口數」
    k_cols = [c for c in tbl.columns if "口數" in str(c)]
    if not k_cols:
        return None
    net_col = k_cols[-1]

    result = {}
    for _, row in tbl.iterrows():
        if row.iloc[1] != TARGET:  # 商品名稱
            continue
        role = str(row.iloc[2]).strip()  # 身份別
        if role not in ROLE_MAP:         # 只要三法人
            continue
        result[ROLE_MAP[role]] = int(str(row[net_col]).replace(",", ""))
        if len(result) == 3:
            break
    if len(result) < 3:
        return None
    # 日期在頁面 h2 旁邊「日期YYYY/MM/DD」，簡易抽取
    date_match = DATE_RE.search(" ".join(tbl.columns.astype(str)))
    if not date_match:
        return None
    date = pd.to_datetime(date_match.group(), format="%Y/%m/%d", utc=True)
    result["date"] = date
    result["retail_net"] = -(result["prop_net"] +
                             result["itf_net"] +
                             result["foreign_net"])
    return result

# ── 主要 parse ──────────────────────────────────────────────
def parse(raw:str) -> dict:
    html = raw if raw.lstrip().startswith("<") else None
    if html:
        tables = pd.read_html(io.StringIO(html), thousands=",", header=0)
        for t in tables:
            data = _extract_tbl(t)
            if data: return data
    # 還是找不到就拋例外
    raise ValueError("無法解析 futContractsDateExcel")

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

    # PyMongo upsert
    doc["date"] = doc["date"].to_pydatetime().replace(tzinfo=None)
    COL.update_one({"date": doc["date"]}, {"$set": doc}, upsert=True)
    print("更新 fut_contracts → MongoDB")

def latest(n:int=1):
    return list(COL.find({}, {"_id":0}).sort("date",-1).limit(n))

# ── CLI ──────────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv)>1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        print(latest(int(sys.argv[2]) if len(sys.argv)>2 else 1))
