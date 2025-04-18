"""
crawler/pc_ratio.py
-------------------
抓取 https://www.taifex.com.tw/cht/3/pcRatioExcel
欄位：date, put_volume, call_volume, pc_volume_ratio,
      put_oi, call_oi, pc_oi_ratio
"""
import io, sys, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from utils.db import get_col

URL = "https://www.taifex.com.tw/cht/3/pcRatioExcel"
HEADERS = {"User-Agent": "Mozilla/5.0 (pc-ratio-crawler/1.0)"}
COL = get_col("pc_ratio")

EXPECTED_COLS = [
    "date",
    "put_volume",
    "call_volume",
    "pc_volume_ratio",
    "put_oi",
    "call_oi",
    "pc_oi_ratio",
]

def _parse(text: str) -> pd.DataFrame:
    """同時支援空白或逗號分隔格式。"""
    import re, io, pandas as pd
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    # 定位第一行資料（以 YYYY/ 開頭）
    start = next(i for i, l in enumerate(lines) if re.match(r"^20\d{2}/", l))
        data = "
".join(lines[start:])lines[start:])

    # 判斷分隔符號
    sample = lines[start]
    if "," in sample:
        sep = ","
    else:
        sep = r"\s+"

    df = pd.read_csv(
        io.StringIO(data),
        sep=sep,
        engine="python",
        header=None,
        thousands=",",
    )
    if df.shape[1] != 7:
        raise ValueError(f"Unexpected columns: {df.shape[1]}")
    df.columns = EXPECTED_COLS
    df["date"] = pd.to_datetime(df["date"], format="%Y/%m/%d", utc=True)
    df[df.columns[1:]] = df[df.columns[1:]].apply(pd.to_numeric, downcast="integer")
    return df

def _today_tw():
    return datetime.now(timezone(timedelta(hours=8))).date()

def fetch(upsert: bool = True):
    res = requests.get(URL, headers=HEADERS, timeout=30)
    res.encoding = res.apparent_encoding or "utf-8"
    df = _parse(res.text)

    latest_date = df["date"].dt.date.max()
    if latest_date < _today_tw():
        print("資料尚未更新，稍後重試")
        sys.exit(75)  # GitHub Actions neutral exit

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
