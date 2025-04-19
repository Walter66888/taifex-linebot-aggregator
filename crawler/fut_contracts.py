# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# crawler/fut_contracts.py  v3.9  2025-04-19 09:50  debug+
# ------------------------------------------------------------
import re, requests, io, os, logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict
from bs4 import BeautifulSoup
from utils.db import get_col
from pymongo import ASCENDING

LOGGER = logging.getLogger(__name__)
URL    = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEADERS = {"User-Agent": "Mozilla/5.0"}
COL   = get_col("fut_contracts")
COL.create_index([("product",1), ("date",1)], unique=True)

# ────────────────────── helper ──────────────────────────────
def _clean_int(s: str) -> int:
    return int(re.sub(r"[^\d\-]", "", s or "0") or 0)

def parse(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    # 每個商品三條 <tr> ，第一列是自營商，以此定位
    rows = soup.select("tbody > tr[class]")
    if not rows:
        LOGGER.error("⚠️  HTML rows 解析失敗，rows=0")
        return []

    docs = []
    for i in range(0, len(rows), 3):
        try:
            r_prop, r_itf, r_foreign = rows[i:i+3]
        except ValueError:
            LOGGER.warning("row grouping 不足三列，跳過 index=%s", i)
            continue

        tds_prop = r_prop.find_all("td")
        prod_name = tds_prop[1].get_text(strip=True)

        # 只抓小台、微台
        if prod_name not in ("小型臺指期貨", "微型臺指期貨"):
            continue
        product = "mtx" if prod_name.startswith("小型") else "imtx"

        prop_net   = _clean_int(tds_prop[14].get_text())
        itf_net    = _clean_int(r_itf.find_all("td")[14].get_text())
        foreign_net= _clean_int(r_foreign.find_all("td")[14].get_text())
        retail_net = -(prop_net + itf_net + foreign_net)

        date_str = soup.select_one(".h2 + table span.right").get_text(strip=True) \
                  .replace("日期","").strip()    # ex: 2025/04/18
        date_obj = datetime.strptime(date_str, "%Y/%m/%d").replace(tzinfo=timezone.utc)

        docs.append(dict(
            date=date_obj,
            product=product,
            prop_net=prop_net,
            itf_net=itf_net,
            foreign_net=foreign_net,
            retail_net=retail_net
        ))
    return docs

def fetch() -> List[Dict]:
    LOGGER.info("fetch %s", URL)
    res = requests.get(URL, headers=HEADERS, timeout=20)
    res.raise_for_status()
    docs = parse(res.text)
    if not docs:
        raise RuntimeError("未解析到任何 fut_contracts 資料")

    ops = [
        dict(update_one=dict(
            filter={"product": d["product"], "date": d["date"]},
            update={"$set": d},
            upsert=True))
        for d in docs
    ]
    COL.bulk_write(ops, ordered=False)
    LOGGER.info("upserted %d docs", len(docs))
    return docs

def latest(product: str = None):
    q = {"product": product} if product else {}
    return COL.find_one(q, {"_id":0}, sort=[("date",-1)])

if __name__ == "__main__":
    import argparse, pprint, sys
    ap = argparse.ArgumentParser()
    ap.add_argument("run", nargs="?", default="run")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    if args.run == "run":
        try:
            pprint.pprint(fetch())
        except Exception as e:
            LOGGER.error("crawler error: %s", e)
            sys.exit(1)
