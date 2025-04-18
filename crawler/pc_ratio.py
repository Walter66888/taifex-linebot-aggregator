"""
crawler/pc_ratio.py  v1.7
-------------------------
抓取 https://www.taifex.com.tw/cht/3/pcRatioExcel
HTML / CSV 自動解析 → MongoDB《pc_ratio》
依賴：pandas、requests、pymongo、lxml、beautifulsoup4
"""

from __future__ import annotations
import io, re, sys, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from pymongo import UpdateOne
from utils.db import get_col

URL  = "https://www.taifex.com.tw/cht/3/pcRatioExcel"
HEAD = {"User-Agent": "Mozilla/5.0 (pc-ratio-crawler/1.7)"}
COL  = get_col("pc_ratio")

EXPECTED = [
    "date", "put_volume", "call_volume", "pc_volume_ratio",
    "put_oi", "call_oi", "pc_oi_ratio",
]
DATE_RE = re.compile(r"^20\d{2}/\d{1,2}/\d{1,2}$")

# ── CSV 解析 ───────────────────────────────────────────────
def _parse_csv(text: str) -> pd.DataFrame:
    text  = text.lstrip("\ufeff")
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    try:
        idx = next(i for i,l in enumerate(lines)
                   if DATE_RE.match(l.split(",")[0].split()[0]))
    except StopIteration:
        raise ValueError("CSV 無日期列")

    sep = "," if "," in lines[idx] else r"\s+"
    df  = pd.read_csv(
        io.StringIO("\n".join(lines[idx:])),
        sep=sep, engine="python", header=None, thousands=","
    )
    if df.shape[1] != 7:
        raise ValueError(f"CSV 欄位數異常: {df.shape[1]}")
    df.columns = EXPECTED
    return df

# ── HTML 解析 ──────────────────────────────────────────────
def _parse_html(html: str) -> pd.DataFrame:
    tbls = pd.read_html(io.StringIO(html), thousands=",")
    for t in tbls:
        first = str(t.iloc[0,0]).strip()
        if DATE_RE.match(first):
            df = t.copy()
        elif first == "日期" and DATE_RE.match(str(t.iloc[1,0]).strip()):
            df = t.iloc[1:].reset_index(drop=True)
        else:
            continue
        if df.shape[1] != 7: continue
        df.columns = EXPECTED
        return df
    raise ValueError("HTML 無符合格式的表格")

# ── 通用 parse ─────────────────────────────────────────────
def parse(raw: str) -> pd.DataFrame:
    probe = raw.lstrip()[:4096].lower()
    df = _parse_html(raw) if probe.startswith("<") or "<table" in probe else _parse_csv(raw)
    df["date"] = pd.to_datetime(df["date"], format="%Y/%m/%d", utc=True)
    df[df.columns[1:]] = df[df.columns[1:]].apply(pd.to_numeric, downcast="integer")
    return df

def today_tw() -> datetime.date:
    return datetime.now(timezone(timedelta(hours=8))).date()

# ── 主流程 ────────────────────────────────────────────────
def fetch(upsert: bool=True) -> pd.DataFrame:
    res = requests.get(URL, headers=HEAD, timeout=30)
    res.encoding = res.apparent_encoding or "utf-8"
    df = parse(res.text)

    if df["date"].dt.date.max() < today_tw():
        print("尚未更新，Neutral Exit")
        sys.exit(75)

    if upsert:
        ops = []
        for _, row in df.iterrows():
            d = row.to_dict()
            d["date"] = row["date"].to_pydatetime().replace(tzinfo=None)
            ops.append(UpdateOne({"date": d["date"]}, {"$set": d}, upsert=True))
        if ops:
            COL.bulk_write(ops, ordered=False)
    return df

def latest(days:int=1):
    return list(COL.find({}, {"_id":0}).sort("date",-1).limit(days))

# ── CLI ──────────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        fetch()
    elif cmd == "show":
        print(latest(int(sys.argv[2]) if len(sys.argv) > 2 else 5))
