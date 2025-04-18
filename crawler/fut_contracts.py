"""
crawler/fut_contracts.py  v3.8  ― header‑safe final
---------------------------------------------------
• 透過 thead 找到「未平倉餘額 → 多空淨額 → 口數」欄位 index
• 只在 〈td index 1〉(商品名稱欄) 比對 TARGETS → 絕不混行
"""

from __future__ import annotations
import re, sys
from datetime import datetime, timezone, timedelta
import bs4 as bs, requests
from pymongo import ASCENDING, UpdateOne
from utils.db import get_col

URL  = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD = {"User-Agent": "taifex-fut-crawler/3.8"}

TARGETS = {
    "小型臺指期貨": "mtx", "小型台指期貨": "mtx",
    "微型臺指期貨": "imtx","微型台指期貨": "imtx",
}
ROLE_MAP = {
    "自營商": "prop_net", "自營商(避險)": "prop_net",
    "投信":   "itf_net",
    "外資":   "foreign_net", "外資及陸資": "foreign_net",
}

DATE_RE = re.compile(r"日期\s*(\d{4}/\d{1,2}/\d{1,2})")
NUM     = lambda s:int(s.replace(",","")) if s and s.replace(",","").lstrip("-").isdigit() else 0

COL = get_col("fut_contracts")   # utils/db 建複合唯一索引

def today_tw(): return datetime.now(timezone(timedelta(hours=8))).date()

# ── 找「未平倉多空淨額 口數」欄位 index ───────────────────
def _oi_net_idx(soup: bs.BeautifulSoup) -> int:
    for th in soup.select("thead th"):
        if th.text.strip() == "未平倉餘額":
            ths = list(th.parent.find_all("th"))
            for i, t in enumerate(ths):
                if t.get_text(strip=True) == "多空淨額":
                    return (i * 2) + 2   # colspan=2：long‧amt / short‧amt / 淨額‧口數
    raise RuntimeError("找不到 未平倉多空淨額 口數 欄位")

# ── 解析 HTML ──────────────────────────────────────────
def parse(html:str):
    soup=bs.BeautifulSoup(html,"lxml")
    date_str=DATE_RE.search(soup.find(string=DATE_RE)).group(1)
    date_dt = datetime.strptime(date_str,"%Y/%m/%d").replace(tzinfo=timezone.utc)
    idx_net = _oi_net_idx(soup)

    res={v:{"date":date_dt,"product":v,"prop_net":0,"itf_net":0,"foreign_net":0} for v in TARGETS.values()}
    cur=None
    for tr in soup.select("tbody tr"):
        tds=[td.get_text(strip=True) for td in tr.find_all("td")]
        if not tds: continue

        # ── 只在商品名稱欄 (index 1) 比對 ──
        if len(tds) > 1 and tds[1] in TARGETS:
            cur = TARGETS[tds[1]]
        if cur is None:
            continue

        # ── 身份別判定 ──
        role = tds[2] if len(tds) >= 3 and tds[2] in ROLE_MAP else tds[0] if tds[0] in ROLE_MAP else None
        if role not in ROLE_MAP:
            continue

        if idx_net >= len(tds):
            continue
        net = NUM(tds[idx_net])
        res[cur][ROLE_MAP[role]] = net

    return [
        {**d,"retail_net":-(d["prop_net"]+d["itf_net"]+d["foreign_net"])}
        for d in res.values()
    ]

# ── 抓取 & 寫入 ─────────────────────────────────────────
def fetch(upsert=True):
    html=requests.get(URL,headers=HEAD,timeout=30).text
    docs=parse(html)
    if docs[0]["date"].date() < today_tw():
        print("[WARN] fut_contracts 未更新"); sys.exit(75)

    if upsert:
        ops=[UpdateOne({"date":d["date"].replace(tzinfo=None),"product":d["product"]},
                       {"$set":{**d,"date":d["date"].replace(tzinfo=None)}},
                       upsert=True) for d in docs]
        COL.bulk_write(ops,ordered=False)
    print(f"更新 {len(docs)} 商品 fut_contracts → MongoDB")
    return docs

def latest(prod="mtx",days=1):
    return list(COL.find({"product":prod},{"_id":0}).sort("date",-1).limit(days))

# ── CLI ───────────────────────────────────────────────
if __name__=="__main__":
    if len(sys.argv)>1 and sys.argv[1]=="show":
        print(latest(sys.argv[2] if len(sys.argv)>2 else "mtx",3))
    else:
        fetch()
