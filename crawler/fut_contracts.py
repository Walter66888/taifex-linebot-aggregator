# crawler/fut_contracts.py  v4.4  2025‑04‑19
"""
抓取『三大法人‑區分各期貨契約』：不篩選任何商品
  ‑ 所有期貨契約會被抓取並存入資料庫。

使用方式：
  python -m crawler.fut_contracts run             # 平日自動跳過假日
  python -m crawler.fut_contracts run --force     # 強制抓

資料表：taifex.fut_contracts
  {product,date,prop_net,itf_net,foreign_net,retail_net,raw_data}
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

        # 只要抓取所有商品，不做篩選
        idf = cells[2].get_text(strip=True)   # 自營商 / 投信 / 外資
        try:
            net = _row_net(cells)
        except ValueError as e:
            LOG.debug("skip row: %s", e)
            continue

        entry = result.setdefault(
            current_product,
            {"prop_net": 0, "itf_net": 0, "foreign_net": 0, "raw_data": {"column_data": []}}
        )
        if idf == "自營商":
            entry["prop_net"] = net
        elif idf == "投信":
            entry["itf_net"] = net
        elif idf == "外資":
            entry["foreign_net"] = net

        # 保存原始資料以便日後檢視
        entry["raw_data"]["column_data"] = [cell.get_text(strip=True) for cell in cells]

    docs: list[dict] = []
    for pname, vals in result.items():
        retail = -(vals["prop_net"] + vals["itf_net"] + vals["foreign_net"])
        docs.append({
            "date": date_obj,
            "product": pname,  # 儲存所有商品名稱（不過濾）
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
