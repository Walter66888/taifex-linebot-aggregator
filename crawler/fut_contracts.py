"""
crawler/fut_contracts.py
------------------------
抓取 https://www.taifex.com.tw/cht/3/futContractsDateExcel
目標：小型臺指期貨 (MTX) 三大法人淨額 + 計算散戶未平倉
欄位：date, prop_net, itf_net, foreign_net, retail_net
"""
import io, sys, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from utils.db import get_col

URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEADERS = {"User-Agent": "Mozilla/5.0 (fut-contracts-crawler/1.0)"}
TARGET = "小型臺指期貨"
COL = get_col("fut_contracts")

COL_NAMES = {
    "日期": "date",
    "多空淨額(自營商)": "prop_net",
    "多空淨額(投信)": "itf_net",
    "多空淨額(外資及陸資)": "foreign_net",
}

def _today_tw():
    return datetime.now(timezone(timedelta(hours=8))).date()

def _parse(text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(text), thousands=",")
    df = df[df["商品名稱"] == TARGET].copy()
    df.rename(columns=COL_NAMES, inplace=True)
    df["date"] = pd.to_datetime(df["date"], format="%Y/%m/%d", utc=True)
    df["retail_net"] = -(df["prop_net"] + df["itf_net"] + df["foreign_net"])
    return df[["date", "prop_net", "itf_net", "foreign_net", "retail_net"]]

def fetch(upsert: bool = True):
    res = requests.get(URL, headers=HEADERS, timeout=30)
    res.encoding = res.apparent_encoding or "utf-8"
    df = _parse(res.text)

    latest_date = df["date"].dt.date.max()
    if latest_date < _today_tw():
        print("資料尚未更新，稍後重試")
        sys.exit(75)

    if upsert:
        ops = [
            {
                "updateOne": {
                    "filter": {"date": row["date"]},
                    "update": {"$set": row.to_dict()},
                    "upsert": True,
                }
            }
            for _, row in df.iterrows()
        ]
        if ops:
            COL.bulk_write(ops, ordered=False)
    return df

def latest(days: int = 1):
    return list(COL.find({}, {"_id": 0}).sort("date", -1).limit(days))

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        print(latest(int(sys.argv[2]) if len(sys.argv) > 2 else 5))
