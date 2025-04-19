# -*- coding: utf-8 -*-
# crawler/fut_contracts.py  v4.4  2025‑04‑19
"""
抓『三大法人‑區分各期貨契約』：小台(mtx)、微台(imtx)
"""

from __future__ import annotations
import re, requests, argparse, logging, pprint, sys
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from pymongo import UpdateOne
from utils.db import get_col

LOG      = logging.getLogger(__name__)
URL      = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEADERS  = {"User-Agent": "Mozilla/5.0"}

COL = get_col("fut_contracts")
COL.create_index([("product", 1), ("date", 1)], unique=True)

TARGETS = {
    "小型臺指期貨": "mtx",
    "微型臺指期貨": "imtx",
}

IDF_SET = {"自營商", "投信", "外資"}

# ───────────────────────── helpers ──────────────────────────
def _clean_int(txt: str) -> int:
    return int(re.sub(r"[^\d\-]", "", txt or "0") or 0)


def _row_net(cells) -> int:
    """未平倉『多空淨額‑口數』= 倒數第 2 格（14 或 15 欄皆通用）"""
    if len(cells) < 12:
        raise ValueError("too few columns")
    return _clean_int(cells[-2].get_text())


def _row_idf(cells) -> str | None:
    """哪一格是 自營商 / 投信 / 外資──逐格找最保險"""
    for c in cells:
        t = c.get_text(strip=True)
        if t in IDF_SET:
            return t
    return None

# ───────────────────────── parser ──────────────────────────
def parse(html: str) -> list[dict]:
    m = re.search(r"日期(\d{4}/\d{2}/\d{2})", html)
    if not m:
        raise RuntimeError("找不到日期")
    date_obj = datetime.strptime(m.group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    soup  = BeautifulSoup(html, "lxml")
    rows  = soup.find_all("tr", class_="12bk")
    if not rows:
        raise RuntimeError("tbody 無 tr.12bk 列")

    result: dict[str, dict] = {}
    current_product = None

    for tr in rows:
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        prod_txt = cells[1].get_text(strip=True)
        if prod_txt:
            current_product = prod_txt

        if current_product not in TARGETS:
            continue

        idf = _row_idf(cells)
        if idf is None:
            continue

        try:
            net = _row_net(cells)
        except ValueError:
            continue

        entry = result.setdefault(
            current_product,
            {"prop_net": 0, "itf_net": 0, "foreign_net": 0}
        )
        if idf == "自營商":
            entry["prop_net"] = net
        elif idf == "投信":
            entry["itf_net"]  = net
        else:                        # 外資
            entry["foreign_net"] = net

    docs: list[dict] = []
    for pname, v in result.items():
        retail = -(v["prop_net"] + v["itf_net"] + v["foreign_net"])
        docs.append({
            "date": date_obj,
            "product": TARGETS[pname],
            **v,
            "retail_net": retail,
        })
    return docs

# ───────────────────────── fetch ──────────────────────────
def _is_weekend() -> bool:
    from datetime import datetime
    return datetime.now().weekday() >= 5    # 5,6 = Sat,Sun

def fetch(force: bool=False) -> list[dict]:
    if _is_weekend() and not force:
        raise RuntimeError("週末不抓 (加 --force 可強制)")

    res = requests.get(URL, headers=HEADERS, timeout=20)
    res.raise_for_status()
    docs = parse(res.text)
    if not docs:
        raise RuntimeError("未取得任何商品資料")

    COL.bulk_write(
        [UpdateOne({"product": d["product"], "date": d["date"]},
                   {"$set": d}, upsert=True) for d in docs],
        ordered=False
    )
    LOG.info("upsert %d docs OK", len(docs))
    return docs

def latest(product: str | None = None):
    q = {"product": product} if product else {}
    return COL.find_one(q, {"_id":0}, sort=[("date",-1)])

# ───────────────────────── CLI ──────────────────────────
if __name__ == "__main__":
    import logging, pprint, argparse, sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    if args.cmd == "run":
        try:
            pprint.pp(fetch(args.force))
        except Exception as e:
            LOG.error("crawler error: %s", e)
            sys.exit(1)
