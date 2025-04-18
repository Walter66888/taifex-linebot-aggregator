# -*- coding: utf-8 -*-
"""
fut_contracts.py  v4.0  – compute OI net by 6‑8 formula
------------------------------------------------------
• OI_net = nums[6] - nums[8]  (口數 long ‑ short)
• 再無索引推算，擺脫欄位位移問題
"""
from __future__ import annotations
import re, sys, logging
from datetime import datetime, timezone, timedelta
import requests
from bs4 import BeautifulSoup
import pymongo
from utils.db import get_col

URL  = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD = {"User-Agent": "taifex-fut-crawler/4.0"}

TARGETS = {"小型臺指期貨":"mtx","小型台指期貨":"mtx",
           "微型臺指期貨":"imtx","微型台指期貨":"imtx"}
ROLE_MAP= {"自營商":"prop_net","自營商(避險)":"prop_net",
           "投信":"itf_net","外資":"foreign_net","外資及陸資":"foreign_net"}

NUM = lambda s:int(s.replace(",","")) if s.replace(",","").lstrip("-").isdigit() else 0
DATE_RE = re.compile(r"日期\s*(\d{4}/\d{1,2}/\d{1,2})")

COL = get_col("fut_contracts")
COL.create_index([("date",1),("product",1)], unique=True)

def today_tw(): return datetime.now(timezone(timedelta(hours=8))).date()

def _date(html:str)->datetime:
    return datetime.strptime(DATE_RE.search(html).group(1),"%Y/%m/%d")

def parse(html:str):
    soup=BeautifulSoup(html,"lxml")
    date=_date(html).replace(tzinfo=timezone.utc)
    res={v:{"date":date,"product":v,"prop_net":0,"itf_net":0,"foreign_net":0} for v in TARGETS.values()}

    rows=[tr for tr in soup.select("tbody tr") if "12bk" in tr.get("class",[])]
    cur=None
    for tr in rows:
        nums=[NUM(td.get_text(strip=True).replace("口","")) for td in tr.find_all("td")[3:]]
        if len(nums)<9: continue
        tds=[td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds)>1 and tds[1] in TARGETS: cur=TARGETS[tds[1]]
        if cur is None: continue

        role= tds[2] if len(tds)>=3 and tds[2] in ROLE_MAP else tds[0] if tds[0] in ROLE_MAP else None
        if role not in ROLE_MAP: continue

        net = nums[6]-nums[8]          # long OI – short OI
        res[cur][ROLE_MAP[role]]=net

    return [{**d,"retail_net":-(d["prop_net"]+d["itf_net"]+d["foreign_net"])} for d in res.values()]

def fetch():
    html=requests.get(URL,headers=HEAD,timeout=15).text
    docs=parse(html)
    if docs[0]["date"].date()<today_tw():
        logging.warning("fut_contracts 未更新"); sys.exit(75)
    ops=[pymongo.UpdateOne({"date":d["date"],"product":d["product"]},{"$set":d},upsert=True) for d in docs]
    COL.bulk_write(ops,ordered=False)
    logging.info("fut_contracts upsert %s",len(docs))
    return docs

def latest(prod="mtx",days=1):
    return list(COL.find({"product":prod},{"_id":0}).sort("date",-1).limit(days))

if __name__=="__main__":
    from pprint import pprint; pprint(fetch())
