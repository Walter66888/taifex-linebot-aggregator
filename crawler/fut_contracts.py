# -*- coding: utf-8 -*-
"""
抓取 TAIFEX 三大法人未平倉餘額（分契約）
小型臺指(mtx) 與 微型臺指(imtx) 兩商品

表格版型固定，未平倉區的『多空淨額‑口數』欄位
在每列 <td> 的索引固定為 13  (0 起算)
   ┌────────────────────────────┐
seq│prod│id │ …  trade(6) … │ … OI(6) … │
 0    1    2           ↑               ↑
            3~8   →   6 7   9 10 11 **13**
"""

import datetime as dt
import logging
import re
from typing import List, Dict

import bs4
import requests

from utils.db import get_col

URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
COL = get_col("fut_contracts")          # 已在 utils.db.ensure_index(date,product) 建唯一索引

# ────────────────────────────── 共用小工具 ──────────────────────────────
def _clean_int(txt: str) -> int:
    """移除逗號、空白轉 int"""
    return int(re.sub(r"[,\s]", "", txt))

def _parse_date(soup: bs4.BeautifulSoup) -> dt.date:
    """
    解析<h2> 區段上的「日期YYYY/MM/DD」
    """
    span = soup.find("span", class_="right")
    if not span or "日期" not in span.text:
        raise RuntimeError("找不到日期欄位")
    dstr = span.text.strip().replace("日期", "")
    return dt.datetime.strptime(dstr, "%Y/%m/%d").date()

def _one_row_net(td_list: List[bs4.Tag]) -> int:
    """
    表格中單列 <td> 清單 ⇒ 回傳 未平倉多空淨額(口數)
    根據固定版型：index 13 就是『未平倉多空淨額 – 口數』
    """
    return _clean_int(td_list[13].text)

# ────────────────────────────── 解析核心 ──────────────────────────────
MAP_PROD = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}

def parse(html: str) -> List[Dict]:
    soup = bs4.BeautifulSoup(html, "lxml")
    date = _parse_date(soup)

    rows = soup.select("tbody tr.12bk")            # 每三列為同一商品
    if not rows:
        raise RuntimeError("找不到 <tr.12bk>，版型可能變動")

    docs = []
    for i in range(0, len(rows), 3):
        r_prop, r_itf, r_foreign = rows[i : i + 3]
        prod_name = r_prop.find_all("td")[1].get_text(strip=True)
        prod = MAP_PROD.get(prod_name)
        if not prod:                               # 只抓 mtx / imtx
            continue

        td_prop     = r_prop.find_all("td")
        td_itf      = r_itf.find_all("td")
        td_foreign  = r_foreign.find_all("td")

        prop_net    = _one_row_net(td_prop)
        itf_net     = _one_row_net(td_itf)
        foreign_net = _one_row_net(td_foreign)
        retail_net  = -(prop_net + itf_net + foreign_net)

        docs.append(
            {
                "date":      dt.datetime.combine(date, dt.time()),
                "product":   prod,
                "prop_net":  prop_net,
                "itf_net":   itf_net,
                "foreign_net": foreign_net,
                "retail_net":  retail_net,
            }
        )
    return docs

# ────────────────────────────── 對外 API ──────────────────────────────
def fetch() -> List[Dict]:
    """抓網頁 → 解析 → upsert 到 MongoDB，並回傳文件"""
    res = requests.get(URL, timeout=20)
    res.encoding = "utf-8"

    docs = parse(res.text)

    ops = [
        bs4.dammit.UpdateOne(
            {"date": d["date"], "product": d["product"]},
            {"$set": d},
            upsert=True,
        )
        for d in docs
    ]
    if ops:
        COL.bulk_write(ops, ordered=False)

    return docs

def latest(prod: str = None) -> Dict:
    """取最新(同一天)的單筆或全體"""
    qry = {"product": prod} if prod else {}
    mm = list(COL.find(qry).sort("date", -1).limit(2))
    return mm[0] if mm else {}

# ────────────────────────────── debug ──────────────────────────────
if __name__ == "__main__":
    from pprint import pprint
    pprint(fetch())
