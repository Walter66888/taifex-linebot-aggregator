# -*- coding: utf-8 -*-
"""
fut_contracts.py  v3.9.1
------------------------
• 修正 .12bk 選擇器 → 以 Python 方式過濾 class
    rows = [tr for tr in soup.select("tbody tr") if '12bk' in tr.get('class', [])]
其餘邏輯與 v3.9 相同。
"""

from __future__ import annotations
import re, sys, logging
from datetime import datetime, timezone, timedelta
import requests
from bs4 import BeautifulSoup
import pymongo

from utils.db import get_col

URL  = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD = {"User-Agent": "taifex-fut-crawler/3.9.1"}

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
FALLBACK_IDX = 10

COL = get_col("fut_contracts")
COL.create_index([("date",1),("product",1)], unique=True)

def today_tw(): return datetime.now(timezone(timedelta(hours=8))).date()

def _oi_net_idx(soup: BeautifulSoup) -> int:
    for th in soup.select("thead th"):
        txt = th.get_text(strip=True).replace("　","").replace(" ","")
        if txt.startswith("未平倉餘額"):
            ths=list(th.parent.find_all("th"))
            for i,t in enumerate(ths):
                if t.get_text(strip=True).replace("　","").replace(" ","")=="多空淨額":
                    return (i*2)+2
    return FALLBACK_IDX

def parse(html:str):
    soup=BeautifulSoup(html,"lxml")
    date_dt=datetime.strptime(DATE_RE.search(soup.text).group(1),"%Y/%m/%d").replace(tzinfo=timezone.utc)
    idx=_oi_net_idx(soup)

    res={v:{"date":date_dt,"product":v,"prop_net":0,"itf_net":0,"foreign_net":0} for v in TARGETS.values()}
    cur=None
    rows=[tr for tr in soup.select("tbody tr") if "12bk" in tr.get("class", [])]

    for tr in rows:
        tds=[td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds)<15: continue

        if len(tds)>1 and tds[1] in TARGETS:
            cur = TARGETS[tds[1]]
        if cur is None: continue

        role = tds[2] if len(tds)>=3 and tds[2] in ROLE_MAP else tds[0] if tds[0] in ROLE_MAP else None
        if role not in ROLE_MAP or idx>=len(tds): continue

        res[cur][ROLE_MAP[role]] = NUM(tds[idx])

    return [
        {**d,"retail_net":-(d["prop_net"]+d["itf_net"]+d["foreign_net"])}
        for d in res.values()
    ]

def fetch():
    html=requests.get(URL,headers=HEAD,timeout=30).text
    docs=parse(html)
    if docs[0]["date"].date()<today_tw():
        logging.warning("fut_contracts 未更新"); sys.exit(75)

    ops=[pymongo.UpdateOne({"date":d["date"],"product":d["product"]},{"$set":d},upsert=True) for d in docs]
    COL.bulk_write(ops,ordered=False)
    logging.info("fut_contracts upsert %s docs", len(docs))
    return docs

def latest(prod:str="mtx",days:int=1):
    return list(COL.find({"product":prod},{"_id":0}).sort("date",-1).limit(days))

if __name__=="__main__":
    from pprint import pprint; pprint(fetch())
