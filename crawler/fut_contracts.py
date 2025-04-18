# -*- coding: utf-8 -*-
"""
fut_contracts.py v4.0  — 精準抓取『三大法人 ‑ 區分各期貨契約』未平倉多空淨額

author : chatgpt‑o3  date : 2025‑04‑18
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Dict, List

import requests
from bs4 import BeautifulSoup

from utils.db import get_col

URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
COL = get_col("fut_contracts")        # Mongo Collection
COL.create_index([("date", 1), ("product", 1)], unique=True)

# 映射『中文品名 → 內部代碼』
PROD_MAP = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}

# ---------- 工具 ----------


def _clean_int(text: str) -> int:
    """把帶逗號、空白的數字轉 int。"""
    return int(text.replace(",", "").replace(" ", "").strip() or "0")


def _guess_oi_net_idx(soup: BeautifulSoup) -> int:
    """
    從 <thead> 自動找『未平倉餘額 > 多空淨額 > 口數』所在欄位。
    若找不到就 fallback 回預設 13。
    """
    head_rows = soup.find_all("tr", class_="12bk")
    for tr in head_rows[:3]:          # 標題列一定在前幾列
        ths = [th.get_text(strip=True) for th in tr.find_all("th")]
        for idx, txt in enumerate(ths):
            if txt == "口數":
                # 往回看前兩層 <th> 的 colspan 標籤文字
                # 只要確定前面兄弟有『多空淨額』且外層有『未平倉餘額』即可
                if "多空淨額" in tr.get_text() and "未平倉" in soup.find("thead").get_text():
                    return idx
    return 13  # 官方表格 2023‑10 起皆為固定列


def parse(html: str) -> List[Dict]:
    """把整張表格轉為 [{date, product, prop_net, itf_net, foreign_net, retail_net}, …]"""
    soup = BeautifulSoup(html, "lxml")

    # 1. 解析日期
    date_tag = soup.find("span", string=re.compile(r"日期\d{4}/\d{2}/\d{2}"))
    date_str = re.search(r"(\d{4})/(\d{2})/(\d{2})", date_tag.text).group(0)
    tdate = dt.datetime.strptime(date_str, "%Y/%m/%d").date()

    # 2. 鎖定未平倉多空淨額『口數』欄位索引
    net_idx = _guess_oi_net_idx(soup)

    # 3. 遍歷資料列
    docs: Dict[str, Dict] = {}
    current_prod = ""  # 換組時更新
    rows = soup.find_all("tr", class_="12bk")

    for tr in rows:
        tds = tr.find_all("td")
        if not tds:
            continue

        # 換商品的第一列會有 rowspan=3，因此第二格（商品名）亦帶 rowspan=3
        if tds[1].has_attr("rowspan"):
            prod_name = tds[1].get_text(strip=True)
            current_prod = PROD_MAP.get(prod_name, "")
            if not current_prod:
                continue  # 不是我們要的商品
            docs[current_prod] = {
                "date": dt.datetime.combine(tdate, dt.time()),
                "product": current_prod,
                "prop_net": 0,
                "itf_net": 0,
                "foreign_net": 0,
            }

        # 如果 current_prod 不是我們關心的品種就跳過
        if current_prod not in PROD_MAP.values():
            continue

        role = tds[2].get_text(strip=True)
        net_oi = _clean_int(tds[net_idx].get_text())

        if role == "自營商":
            docs[current_prod]["prop_net"] = net_oi
        elif role == "投信":
            docs[current_prod]["itf_net"] = net_oi
        elif role == "外資":
            docs[current_prod]["foreign_net"] = net_oi

    # 4. 補上散戶口數
    final_docs: List[Dict] = []
    for d in docs.values():
        d["retail_net"] = -(d["prop_net"] + d["itf_net"] + d["foreign_net"])
        final_docs.append(d)

    return final_docs


def fetch() -> List[Dict]:
    """抓遠端 HTML、解析、寫入 Mongo，並回傳本次文件。"""
    res = requests.get(URL, timeout=10)
    res.raise_for_status()

    docs = parse(res.text)

    # upsert
    ops = [
        {
            "update_one": {
                "filter": {"date": doc["date"], "product": doc["product"]},
                "update": {"$set": doc},
                "upsert": True,
            }
        }
        for doc in docs
    ]
    if ops:
        COL.bulk_write(ops, ordered=False)
    return docs


# 允許 CLI 測試：
if __name__ == "__main__":
    from pprint import pprint

    pprint(fetch())
