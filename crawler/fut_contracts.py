# -*- coding: utf-8 -*-
"""
TAIFEX ‑ 三大法人各期貨契約
抓小型臺指(mtx)、微型臺指(imtx) — 未平倉多空淨額(口數)
"""

import datetime as dt
import logging
import re
from typing import List, Dict

import bs4
import requests
from pymongo import UpdateOne

from utils.db import get_col

URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
COL = get_col("fut_contracts")
COL.create_index([("date", 1), ("product", 1)], unique=True)

MAP_PROD = {"小型臺指期貨": "mtx", "微型臺指期貨": "imtx"}
LOGGER = logging.getLogger(__name__)


def _clean_int(s: str) -> int:
    return int(re.sub(r"[,\s]", "", s)) if s.strip() else 0


def _trade_date(soup: bs4.BeautifulSoup) -> dt.date:
    m = re.search(r"日期\s*(\d{4})/(\d{2})/(\d{2})", soup.text)
    if not m:
        raise RuntimeError("找不到日期")
    return dt.date(int(m[1]), int(m[2]), int(m[3]))


def _row_net(tds: List[bs4.Tag]) -> int:
    """第 14 格(index 13) = 未平倉多空淨額 ‧ 口數"""
    return _clean_int(tds[13].get_text())


def parse(html: str) -> List[Dict]:
    soup = bs4.BeautifulSoup(html, "lxml")
    date = _trade_date(soup)

    rows = soup.find_all("tr", class_="12bk")
    docs: List[Dict] = []

    i = 0
    while i + 2 < len(rows):
        r_prop, r_itf, r_for = rows[i : i + 3]

        tds_prop    = r_prop.find_all("td")
        tds_itf     = r_itf.find_all("td")
        tds_foreign = r_for.find_all("td")

        # 三列都必須 >=14 格才是完整資料
        if not (len(tds_prop) >= 14 and len(tds_itf) >= 14 and len(tds_foreign) >= 14):
            i += 1
            continue

        prod_name = tds_prop[1].get_text(strip=True)
        prod      = MAP_PROD.get(prod_name)
        if prod:
            prop_net    = _row_net(tds_prop)
            itf_net     = _row_net(tds_itf)
            foreign_net = _row_net(tds_foreign)
            retail_net  = -(prop_net + itf_net + foreign_net)

            docs.append(
                {
                    "date": dt.datetime.combine(date, dt.time()),
                    "product": prod,
                    "prop_net": prop_net,
                    "itf_net": itf_net,
                    "foreign_net": foreign_net,
                    "retail_net": retail_net,
                }
            )
        i += 3            # 移到下一組
    return docs


def fetch() -> List[Dict]:
    res = requests.get(URL, timeout=20)
    res.encoding = "utf-8"
    docs = parse(res.text)

    if docs:
        COL.bulk_write(
            [
                UpdateOne(
                    {"date": d["date"], "product": d["product"]},
                    {"$set": d},
                    upsert=True,
                )
                for d in docs
            ],
            ordered=False,
        )
    LOGGER.info("upsert %d docs", len(docs))
    return docs


def latest(prod: str | None = None) -> Dict | List[Dict]:
    if prod:
        return COL.find_one({"product": prod}, {"_id": 0}, sort=[("date", -1)])
    return list(COL.find({}, {"_id": 0}).sort("date", -1))


if __name__ == "__main__":
    from pprint import pprint
    pprint(fetch())
