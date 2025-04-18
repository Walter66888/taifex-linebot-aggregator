"""
crawler/fut_contracts.py  v3.9   – robust header‑safe version
------------------------------------------------------------
抓取 https://www.taifex.com.tw/cht/3/futContractsDateExcel

核心改進
• 先在 <thead> 找到「未平倉餘額」→ 同列「多空淨額」→ 再往右 2 格 = 淨額‧口數欄 index
  ‑ 若任何版面變動找不到，就 fallback=10（舊版正確位置）
• 只在 <td index 1> (商品名稱欄，rowspan=3) 比對 TARGETS → 絕不混行
• retail_net = -(prop_net + itf_net + foreign_net)
"""

from __future__ import annotations
import re, sys
from datetime import datetime, timezone, timedelta
import bs4 as bs, requests
from pymongo import ASCENDING, UpdateOne
from utils.db import get_col

# ── 常量 ────────────────────────────────────────────────
URL  = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD = {"User-Agent": "taifex-fut-crawler/3.9"}

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
FALLBACK_IDX = 10         # 舊版表格：未平倉淨額‧口數在 tds[10]

COL = get_col("fut_contracts")   # utils/db.py 已建唯一複合索引

# ── 時間工具 ───────────────────────────────────────────
def today_tw(): return datetime.now(timezone(timedelta(hours=8))).date()

# ── 解析 thead 取得口數欄 index ─────────────────────────
def _oi_net_idx(soup: bs.BeautifulSoup) -> int:
    """回傳未平倉‧多空淨額‧口數所在 td index，找不到則給 fallback=10"""
    for th in soup.select("thead th"):
        txt = th.get_text(strip=True).replace("　", "").replace(" ", "")
        if txt.startswith("未平倉餘額"):
            row_ths = list(th.parent.find_all("th"))
            for i, t in enumerate(row_ths):
                if t.get_text(strip=True).replace("　", "").replace(" ", "") == "多空淨額":
                    return (i * 2) + 2    # 因 colspan=2：long/amt, short/amt, 淨額/口數
    return FALLBACK_IDX

# ── 解析 HTML ──────────────────────────────────────────
def parse(html: str):
    soup = bs.BeautifulSoup(html, "lxml")
    date_dt = datetime.strptime(
        DATE_RE.search(soup.find(string=DATE_RE)).group(1), "%Y/%m/%d"
    ).replace(tzinfo=timezone.utc)
    idx_net = _oi_net_idx(soup)

    res = {v: {"date": date_dt, "product": v,
               "prop_net": 0, "itf_net": 0, "foreign_net": 0}
           for v in TARGETS.values()}

    current_prod = None
    for tr in soup.select("tbody tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not tds:
            continue

        # ── 商品名稱只出現在 index 1 (rowspan=3) ──
        if len(tds) > 1 and tds[1] in TARGETS:
            current_prod = TARGETS[tds[1]]
        if current_prod is None:
            continue

        # ── 身份別 ──
        role = tds[2] if len(tds) >= 3 and tds[2] in ROLE_MAP else (
               tds[0] if tds[0] in ROLE_MAP else None)
        if role not in ROLE_MAP:
            continue

        if idx_net >= len(tds):
            continue
        res[current_prod][ROLE_MAP[role]] = NUM(tds[idx_net])

    # ── 產出文件 ──
    return [
        {**d, "retail_net": -(d["prop_net"] + d["itf_net"] + d["foreign_net"])}
        for d in res.values()
    ]

# ── 抓取 & 寫入 DB ──────────────────────────────────────
def fetch(upsert: bool = True):
    html = requests.get(URL, headers=HEAD, timeout=30).text
    docs = parse(html)

    if docs[0]["date"].date() < today_tw():
        print("[WARN] fut_contracts 未更新"); sys.exit(75)

    if upsert:
        ops = [
            UpdateOne(
                {"date": d["date"].replace(tzinfo=None), "product": d["product"]},
                {"$set": {**d, "date": d["date"].replace(tzinfo=None)}},
                upsert=True,
            ) for d in docs
        ]
        COL.bulk_write(ops, ordered=False)
    print(f"更新 {len(docs)} 商品 fut_contracts → MongoDB")
    return docs

# ── 查詢 API ──────────────────────────────────────────
def latest(product: str = "mtx", days: int = 1):
    return list(
        COL.find({"product": product}, {"_id": 0})
           .sort("date", -1)
           .limit(days)
    )

# ── CLI ───────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "show":
        prod = sys.argv[2] if len(sys.argv) > 2 else "mtx"
        print(latest(prod, 3))
    else:
        fetch()
