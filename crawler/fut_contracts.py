# -*- coding: utf-8 -*-
# crawler/fut_contracts.py  v4.2  2025‑04‑19
"""
抓取『三大法人‑區分各期貨契約』：
  ‑ 小型臺指期貨 (product = mtx)
  ‑ 微型臺指期貨 (product = imtx)

使用方式：
  python -m crawler.fut_contracts run             # 平日自動跳過假日
  python -m crawler.fut_contracts run --force     # 強制抓

資料表：taifex.fut_contracts
  {product,date,prop_net,itf_net,foreign_net,retail_net}
"""

from __future__ import annotations
import re, requests, argparse, logging, pprint, sys
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from pymongo import ASCENDING, UpdateOne
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

# ───────────────────────── internal helpers ──────────────────────────
def _clean_int(txt: str) -> int:
    return int(re.sub(r"[^\d\-]", "", txt or "0") or 0)


def _row_net(cells) -> int:
    """取『未平倉多空淨額‑口數』：行長可能 15,14,13 → index 13 / 12 / 11"""
    length = len(cells)
    if length >= 14:
        idx = 13       # 15 或 14 欄
    elif length == 13:
        idx = 11
    else:
        raise ValueError(f"不支援的欄位數 {length}")
    return _clean_int(cells[idx].get_text())


# ────────────────────────── core parser ─────────────────────────────
def parse(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")

    # 解析日期
    m = re.search(r"日期(\d{4}/\d{2}/\d{2})", html)
    if not m:
        raise RuntimeError("找不到日期")
    date_obj = datetime.strptime(m.group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    rows = soup.find_all("tr", class_="12bk")
    if not rows:
        raise RuntimeError("tbody 無 tr.12bk 資料列")

    result: dict[str, dict] = {}     # prod_name → dict

    current_product: str | None = None
    for tr in rows:
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        # 若第 2 欄有文字代表新的商品開始
        prod_cell_txt = cells[1].get_text(strip=True)
        if prod_cell_txt:
            current_product = prod_cell_txt

        if current_product not in TARGETS:
            continue                          # 只要 mtx / imtx

        idf = cells[2].get_text(strip=True)   # 自營商 / 投信 / 外資
        try:
            net = _row_net(cells)
        except ValueError as e:
            LOG.debug("skip row: %s", e)
            continue

        entry = result.setdefault(
            current_product,
            {"prop_net": 0, "itf_net": 0, "foreign_net": 0}
        )
        if idf == "自營商":
            entry["prop_net"] = net
        elif idf == "投信":
            entry["itf_net"] = net
        elif idf == "外資":
            entry["foreign_net"] = net

    docs: list[dict] = []
    for pname, vals in result.items():
        retail = -(vals["prop_net"] + vals["itf_net"] + vals["foreign_net"])
        docs.append({
            "date": date_obj,
            "product": TARGETS[pname],
            **vals,
            "retail_net": retail,
        })
    return docs


# ─────────────────────────── fetch & util ───────────────────────────
def _is_weekend() -> bool:
    return datetime.now().weekday() >= 5       # 5,6 -> Sat, Sun


def fetch(force: bool = False) -> list[dict]:
    if _is_weekend() and not force:
        raise RuntimeError("週末不抓 (加 --force 可強制)")

    res = requests.get(URL, headers=HEADERS, timeout=20)
    res.raise_for_status()
    docs = parse(res.text)
    if not docs:
        raise RuntimeError("未取得任何商品資料")

    ops = [
        UpdateOne({"product": d["product"], "date": d["date"]},
                  {"$set": d}, upsert=True)
        for d in docs
    ]
    COL.bulk_write(ops, ordered=False)
    LOG.info("upsert %d docs OK", len(docs))
    return docs


def latest(product: str | None = None) -> dict | None:
    query = {"product": product} if product else {}
    return COL.find_one(query, {"_id": 0}, sort=[("date", -1)])


# ────────────────────────── CLI ─────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--force", action="store_true", help="ignore weekend guard")
    args = ap.parse_args()

    if args.cmd == "run":
        try:
            pprint.pp(fetch(args.force))
        except Exception as e:
            LOG.error("crawler error: %s", e)
            sys.exit(1)
