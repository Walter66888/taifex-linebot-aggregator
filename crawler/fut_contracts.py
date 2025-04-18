# -*- coding: utf-8 -*-
"""
fut_contracts.py  v4.2
----------------------
• 每列取 12 個純數字；淨口數永遠是 nums[-2]（倒數第 2 欄）
  → 投信、外資列少了 <td> 仍可正確定位
"""

from __future__ import annotations
import re, sys, logging
from datetime import datetime, timezone, timedelta
import requests
from bs4 import BeautifulSoup
import pymongo
from utils.db import get_col

URL  = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEAD = {"User-Agent": "taifex-fut-crawler/4.2"}

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
    cur=None

    rows=[tr for tr in soup.select("tbody tr") if "12bk" in tr.get("class",[])]
    for tr in rows:
        tds=[td.get_text(strip=True) for td in tr.find_all("td")]
        if not tds: continue

        if len(tds)>1 and tds[1] in TARGETS:
            cur = TARGETS[tds[1]]
            identity_idx=2; num_start=3
        else:
            identity_idx=0; num_start=1
        if cur is None or identity_idx>=len(tds): continue

        role = tds[identity_idx]
        role_key = ROLE_MAP.get(role)
        if role_key is None: continue

        nums=[NUM(x) for x in tds[num_start:] if x]   # 取後續全部數字
        if len(nums)<2: continue
        net = nums[-2]                                # 倒數第 2 = 未平倉淨額 口數
        res[cur][role_key]=net

    return [{**d,"retail_net":-(d["prop_net"]+d["itf_net"]+d["foreign_net"])} for d in res.values()]

def fetch():
    html=requests.get(URL,headers=HEAD,timeout=15).text
    docs=parse(html)
    if docs[0]["date"].date()<today_tw():
        logging.warning("fut_contracts 未更新"); sys.exit(75)
    COL.bulk_write([pymongo.UpdateOne({"date":d["date"],"product":d["product"]},{"$set":d},upsert=True) for d in docs], ordered=False)
    logging.info("fut_contracts upsert %s",len(docs))
    return docs

def latest(prod="mtx",days=1):
    return list(COL.find({"product":prod},{"_id":0}).sort("date",-1).limit(days))

if __name__=="__main__":
    from pprint import pprint; pprint(fetch())
