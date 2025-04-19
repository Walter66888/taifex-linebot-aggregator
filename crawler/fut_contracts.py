# -*- coding: utf-8 -*-
# crawler/fut_contracts.py  v4.0  2025‑04‑19 11:05  robust index
import re, requests, logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from utils.db import get_col
from pymongo import ASCENDING

LOG = logging.getLogger(__name__)
URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEADERS = {"User-Agent": "Mozilla/5.0"}
COL = get_col("fut_contracts")
COL.create_index([("product",1),("date",1)], unique=True)

def _clean_int(txt: str) -> int:
    return int(re.sub(r"[^\d\-]", "", txt or "0") or 0)

def _row_net(tds) -> int:
    # 取該列所有「可能是數字」的欄位，再選最後一個
    nums = [_clean_int(td.get_text()) for td in tds if re.search(r"[\d,]", td.text)]
    if not nums:
        raise ValueError("該列無數字欄位")
    return nums[-1]

def parse(html: str):
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("tbody > tr[class]")
    if not rows:
        LOG.error("tbody rows = 0 ‑ 解析失敗")
        return []

    docs = []
    for i in range(0, len(rows), 3):
        try:
            r_prop, r_itf, r_foreign = rows[i:i+3]
        except ValueError:
            continue
        prod = r_prop.find_all("td")[1].get_text(strip=True)
        if prod not in ("小型臺指期貨", "微型臺指期貨"):
            continue
        product = "mtx" if prod.startswith("小型") else "imtx"

        prop_net    = _row_net(r_prop.find_all("td"))
        itf_net     = _row_net(r_itf.find_all("td"))
        foreign_net = _row_net(r_foreign.find_all("td"))
        retail_net  = -(prop_net + itf_net + foreign_net)

        date_str = soup.select_one(".h2 + table span.right").get_text(strip=True).replace("日期","")
        date_obj = datetime.strptime(date_str, "%Y/%m/%d").replace(tzinfo=timezone.utc)

        docs.append(dict(
            date=date_obj, product=product,
            prop_net=prop_net, itf_net=itf_net,
            foreign_net=foreign_net, retail_net=retail_net
        ))
    return docs

def fetch(force=False):
    res = requests.get(URL, headers=HEADERS, timeout=20)
    res.raise_for_status()
    docs = parse(res.text)
    if not docs:
        raise RuntimeError("解析結果為空")

    ops = [dict(update_one=dict(
        filter={"product": d["product"], "date": d["date"]},
        update={"$set": d}, upsert=True)) for d in docs]
    COL.bulk_write(ops, ordered=False)
    LOG.info("upsert %d docs", len(docs))
    return docs

def latest(product=None):
    q = {"product":product} if product else {}
    return COL.find_one(q,{"_id":0},sort=[("date",-1)])

if __name__ == "__main__":
    import argparse, pprint, sys, logging, os
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    ap=argparse.ArgumentParser(); ap.add_argument("run"); ap.add_argument("--force",action="store_true")
    args=ap.parse_args()
    if args.run=="run":
        try: pprint.pprint(fetch(args.force))
        except Exception as e: LOG.error("crawler error %s",e); sys.exit(1)
