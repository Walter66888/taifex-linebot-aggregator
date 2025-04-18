"""
crawler/fut_contracts.py  v1.2
------------------------------
抓取 https://www.taifex.com.tw/cht/3/futContractsDateExcel
目標：小型臺指期貨 (MTX) 三大法人淨額 + 散戶未平倉
輸出欄位：date, prop_net, itf_net, foreign_net, retail_net
依賴：pandas、requests、pymongo、lxml、beautifulsoup4
"""

from __future__ import annotations
import io, re, sys, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from pymongo import UpdateOne
from utils.db import get_col

URL  = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD = {"User-Agent": "Mozilla/5.0 (fut-contracts-crawler/1.2)"}
TARGET = "小型臺指期貨"
COL  = get_col("fut_contracts")

MAP = {
    "日期": "date",
    "多空淨額(自營商)": "prop_net",
    "多空淨額(投信)": "itf_net",
    "多空淨額(外資及陸資)": "foreign_net",
}
DATE_RE = re.compile(r"^20\d{2}/\d{1,2}/\d{1,2}$")

# ── HTML 解析 ──────────────────────────────────────────────
def _parse_html(html:str) -> pd.DataFrame:
    tbls = pd.read_html(io.StringIO(html), thousands=",")
    for t in tbls:
        if "商品名稱" not in t.columns: continue
        df = t[t["商品名稱"] == TARGET].copy()
        if df.empty: continue
        df.rename(columns=MAP, inplace=True)
        df["date"] = pd.to_datetime(df["date"], format="%Y/%m/%d", utc=True)
        df["retail_net"] = -(df["prop_net"] + df["itf_net"] + df["foreign_net"])
        return df[["date","prop_net","itf_net","foreign_net","retail_net"]]
    raise ValueError("HTML 無目標表格")

# ── CSV 解析 (舊格式備用) ──────────────────────────────────
def _parse_csv(text:str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(text), thousands=",")
    df = df[df["商品名稱"] == TARGET].copy()
    df.rename(columns=MAP, inplace=True)
    df["date"] = pd.to_datetime(df["date"], format="%Y/%m/%d", utc=True)
    df["retail_net"] = -(df["prop_net"] + df["itf_net"] + df["foreign_net"])
    return df[["date","prop_net","itf_net","foreign_net","retail_net"]]

def parse(raw:str) -> pd.DataFrame:
    return _parse_html(raw) if raw.lstrip().startswith("<") else _parse_csv(raw)

def today_tw(): return datetime.now(timezone(timedelta(hours=8))).date()

# ── 主流程 ────────────────────────────────────────────────
def fetch(upsert:bool=True) -> pd.DataFrame:
    res = requests.get(URL, headers=HEAD, timeout=30)
    res.encoding = res.apparent_encoding or "utf-8"
    df = parse(res.text)
    if df["date"].dt.date.max() < today_tw():
        print("尚未更新，Neutral Exit"); sys.exit(75)

    if upsert:
        ops = []
        for _, row in df.iterrows():
            doc = row.to_dict()
            doc["date"] = row["date"].to_pydatetime().replace(tzinfo=None)
            ops.append(UpdateOne({"date": doc["date"]}, {"$set": doc}, upsert=True))
        if ops: COL.bulk_write(ops, ordered=False)
    return df

def latest(days:int=1):
    return list(COL.find({}, {"_id":0}).sort("date",-1).limit(days))

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv)>1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        print(latest(int(sys.argv[2]) if len(sys.argv)>2 else 5))
