# -*- coding: utf-8 -*-
# crawler/fut_contracts.py  v4.1  2025‑04‑19
"""抓取期交所『三大法人‐區分各期貨契約』，
   目前僅入庫：小型臺指期貨(mtx)、微型臺指期貨(imtx)。"""

from __future__ import annotations
import re, requests, logging, argparse, pprint, sys
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from utils.db import get_col
from pymongo import ASCENDING

LOG     = logging.getLogger(__name__)
URL     = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ──────────────────────────────────────────
COL = get_col("fut_contracts")
# 保證 (product, date) 唯一
COL.create_index([("product", 1), ("date", 1)], unique=True)

# ──────────────────────────────────────────
def _clean_int(txt: str) -> int:
    """移除逗號、空白，轉 int。"""
    return int(re.sub(r"[^\d\-]", "", txt or "0") or 0)


def _row_net(tds) -> int:
    """回傳『未平倉多空淨額‑口數』欄位值。
       ‑ 15 欄 → index 13
       ‑ 13 欄 → index 11
       若欄位不足則丟例外。"""
    idx = 13 if len(tds) == 15 else 11
    try:
        return _clean_int(tds[idx].get_text())
    except IndexError:
        raise ValueError(f"列長度={len(tds)} 無 index {idx}")


# ──────────────────────────────────────────
def parse(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("tbody > tr[class]")
    if not rows:
        LOG.error("解析失敗：tbody 無資料列")
        return []

    # 解析日期
    m = re.search(r"日期\s*(\d{4}/\d{2}/\d{2})", html)
    if not m:
        raise RuntimeError("找不到日期字串")
    date_obj = datetime.strptime(m.group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    docs: list[dict] = []
    for i in range(0, len(rows), 3):
        grp = rows[i:i + 3]
        if len(grp) < 3:
            continue                 # 不完整三列跳過

        r_prop, r_itf, r_foreign = grp
        prod_name = r_prop.find_all("td")[1].get_text(strip=True)
        if prod_name not in ("小型臺指期貨", "微型臺指期貨"):
            continue

        product = "mtx" if prod_name.startswith("小型") else "imtx"

        try:
            prop_net    = _row_net(r_prop.find_all("td"))
            itf_net     = _row_net(r_itf.find_all("td"))
            foreign_net = _row_net(r_foreign.find_all("td"))
        except ValueError as e:
            LOG.warning("跳過 %s：%s", prod_name, e)
            continue

        retail_net = -(prop_net + itf_net + foreign_net)

        docs.append(dict(
            date=date_obj, product=product,
            prop_net=prop_net, itf_net=itf_net,
            foreign_net=foreign_net, retail_net=retail_net
        ))
    return docs


# ──────────────────────────────────────────
def fetch(force: bool = False) -> list[dict]:
    res = requests.get(URL, headers=HEADERS, timeout=20)
    res.raise_for_status()
    docs = parse(res.text)
    if not docs:
        raise RuntimeError("未取得任何商品資料")

    from pymongo import UpdateOne
    ops = [UpdateOne({"product": d["product"], "date": d["date"]},
                     {"$set": d}, upsert=True) for d in docs]
    COL.bulk_write(ops, ordered=False)
    LOG.info("upsert %d docs", len(docs))
    return docs


def latest(product: str | None = None):
    """取最新一筆；product 可為 mtx / imtx"""
    query = {"product": product} if product else {}
    return COL.find_one(query, {"_id": 0}, sort=[("date", -1)])


# ──────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--force", action="store_true", help="ignore weekday guard")
    args = ap.parse_args()

    if args.cmd == "run":
        try:
            pprint.pprint(fetch(args.force))
        except Exception as e:
            LOG.error("crawler error: %s", e)
            sys.exit(1)
